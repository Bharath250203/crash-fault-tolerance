package paxos

import (
	"fmt"
	"net/rpc"
	"time"
)

type NodeRPC struct {
	NodeState *Node
}

type Status struct {
	ID       int
	IsLeader bool
	Ballot   Ballot
	Crashed  bool
}

func (nr *NodeRPC) GetStatus(_ *struct{}, out *Status) error {
	nr.NodeState.mu.Lock()
	defer nr.NodeState.mu.Unlock()
	*out = Status{
		ID:       nr.NodeState.ID,
		IsLeader: nr.NodeState.IsLeader,
		Ballot:   nr.NodeState.Ballot,
		Crashed:  nr.NodeState.Crashed,
	}
	return nil
}

func (nr *NodeRPC) Kill(_ *struct{}, _ *struct{}) error {
	nr.NodeState.mu.Lock()
	if nr.NodeState.Crashed {
		nr.NodeState.mu.Unlock()
		return nil
	}
	nr.NodeState.IsLeader = false
	nr.NodeState.Crashed = true
	nr.NodeState.TimerPaused = true  // Prevent ballot inflation while crashed
	if nr.NodeState.LiveNodesMu != nil && nr.NodeState.LiveNodes != nil {
		nr.NodeState.LiveNodesMu.Lock()
		nr.NodeState.LiveNodes[nr.NodeState.ID] = false
		nr.NodeState.LiveNodesMu.Unlock()
	}
	close(nr.NodeState.QuitCh)
	nr.NodeState.mu.Unlock()
	return nil
}

func (nr *NodeRPC) Recover(_ *struct{}, _ *struct{}) error {
	if nr.NodeState == nil {
		return fmt.Errorf("node state is nil")
	}

	nr.NodeState.mu.Lock()
	if !nr.NodeState.Crashed {
		// already up
		nr.NodeState.mu.Unlock()
		return nil
	}

	nr.NodeState.Crashed = false
	nr.NodeState.IsLeader = false
	nr.NodeState.TimerPaused = false  // Resume timer on recovery
	nr.NodeState.QuitCh = make(chan struct{})

	now := time.Now()
	nr.NodeState.lastHeartbeatAt = now
	nr.NodeState.timerExpired = false
	nr.NodeState.timerDue = now.Add(jitteredTimeout())
	if nr.NodeState.LiveNodesMu != nil && nr.NodeState.LiveNodes != nil {
		nr.NodeState.LiveNodesMu.Lock()
		nr.NodeState.LiveNodes[nr.NodeState.ID] = true
		nr.NodeState.LiveNodesMu.Unlock()
	}
	nr.NodeState.mu.Unlock()
	go nr.NodeState.RunTimer()
	go nr.NodeState.RunLeaderHeartbeat()
	return nil
}

func (nr *NodeRPC) StepDown(_ *struct{}, _ *struct{}) error {
	nr.NodeState.mu.Lock()
	defer nr.NodeState.mu.Unlock()
	nr.NodeState.IsLeader = false
	return nil
}

func (nr *NodeRPC) OnLiveSetChanged(_ *struct{}, _ *struct{}) error {
	if nr.NodeState == nil {
		return fmt.Errorf("node state is nil")
	}
	nr.NodeState.OnLiveSetChanged()
	return nil
}

func (nr *NodeRPC) HandleClientRequest(args *ClientRequest, reply *ClientReply) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	
	// RPC-level idempotence: Check last reply per ClientID+Timestamp (spec-compliant)
	nr.NodeState.mu.Lock()
	if lastReply, seen := nr.NodeState.LastReplies[args.ClientID]; seen && args.Timestamp <= lastReply.Timestamp {
		*reply = lastReply
		nr.NodeState.mu.Unlock()
		return nil
	}
	isLeader := nr.NodeState.IsLeader
	ballot := nr.NodeState.Ballot
	nr.NodeState.mu.Unlock()

	if !isLeader {
		if ballot.Number == 0 {
			go nr.NodeState.StartLeaderElection()
			return fmt.Errorf("no leader yet; election triggered")
		}
		leaderID := ballot.NodeID
		if leaderID == nr.NodeState.ID {
			return fmt.Errorf("transient: leader not established")
		}
		nr.NodeState.LiveNodesMu.Lock()
		isLive := nr.NodeState.LiveNodes[leaderID]
		addr := nr.NodeState.NodeAddrs[leaderID]
		nr.NodeState.LiveNodesMu.Unlock()
		if !isLive {
			return fmt.Errorf("leader %d not live", leaderID)
		}
		c, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			return fmt.Errorf("forward dial error: %w", err)
		}
		defer c.Close()
		var r ClientReply
		if err := c.Call(fmt.Sprintf("Node%d.HandleClientRequest", leaderID), args, &r); err != nil {
			return fmt.Errorf("forward call error: %w", err)
		}
		*reply = r
		return nil
	}

	replyChan := make(chan ClientReply, 1)
	nr.NodeState.mu.Lock()
	nr.NodeState.PendingRequests[args.RequestID] = replyChan
	nr.NodeState.mu.Unlock()
	nr.NodeState.HandleClientRequest(*args)

	select {
	case res := <-replyChan:
		*reply = res
		return nil
	case <-time.After(10 * time.Second):
		nr.NodeState.mu.Lock()
		delete(nr.NodeState.PendingRequests, args.RequestID)
		nr.NodeState.mu.Unlock()
		return fmt.Errorf("timeout waiting for commit")
	}
}

func (nr *NodeRPC) HandlePrepare(args *PrepareMsg, reply *PromiseMsg) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.mu.Lock()
	now := time.Now()
	nr.NodeState.lastPrepareSeenAt = now
	nr.NodeState.lastHeartbeatAt = now
	nr.NodeState.suppressUntil = now.Add(quietPeriod + 150*time.Millisecond)
	nr.NodeState.IsLeader = false
	h := nr.NodeState.highestSeenPrepare
	if args.Ballot.Number > h.Number || (args.Ballot.Number == h.Number && args.Ballot.NodeID > h.NodeID) {
		nr.NodeState.highestSeenPrepare = args.Ballot
	}
	due := nr.NodeState.timerDue
	nr.NodeState.mu.Unlock()

	for {
		wait := time.Until(due)
		if wait <= 0 {
			break
		}
		if wait > 50*time.Millisecond {
			wait = 50 * time.Millisecond
		}
		time.Sleep(wait)
		nr.NodeState.mu.Lock()
		due = nr.NodeState.timerDue
		expired := nr.NodeState.timerExpired
		nr.NodeState.mu.Unlock()
		if expired || time.Until(due) <= 0 {
			break
		}
	}

	nr.NodeState.mu.Lock()
	if nr.NodeState.highestSeenPrepare.Number > nr.NodeState.Ballot.Number ||
		(nr.NodeState.highestSeenPrepare.Number == nr.NodeState.Ballot.Number &&
			nr.NodeState.highestSeenPrepare.NodeID > nr.NodeState.Ballot.NodeID) {
		nr.NodeState.Ballot = nr.NodeState.highestSeenPrepare
	}

	newerOrEqual := args.Ballot.Number > nr.NodeState.Ballot.Number ||
		(args.Ballot.Number == nr.NodeState.Ballot.Number && args.Ballot.NodeID >= nr.NodeState.Ballot.NodeID)

	if newerOrEqual {
		nr.NodeState.Ballot = args.Ballot
		*reply = PromiseMsg{
			Ballot:    nr.NodeState.Ballot,
			NodeID:    nr.NodeState.ID,
			AcceptLog: nr.NodeState.AcceptLog,
		}
		nr.NodeState.mu.Unlock()
		return nil
	}

	*reply = PromiseMsg{Ballot: Ballot{Number: 0, NodeID: -1}}
	nr.NodeState.mu.Unlock()
	return nil
}

func (nr *NodeRPC) HandleAccept(args *AcceptMsg, reply *AcceptedMsg) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.mu.Lock()
	defer nr.NodeState.mu.Unlock()

	if args.Ballot.Number > nr.NodeState.Ballot.Number {
		nr.NodeState.IsLeader = false
	}
	if args.Ballot.Number >= nr.NodeState.Ballot.Number {
		select {
		case nr.NodeState.HeartbeatChan <- struct{}{}:
		default:
		}
		nr.NodeState.Ballot = args.Ballot
		nr.NodeState.AcceptLog[args.SeqNum] = *args
		*reply = AcceptedMsg{
			Ballot:  args.Ballot,
			SeqNum:  args.SeqNum,
			Request: args.Request,
			NodeID:  nr.NodeState.ID,
		}
		return nil
	}
	*reply = AcceptedMsg{NodeID: -1}
	return nil
}

func (nr *NodeRPC) HandleAccepted(args *AcceptedMsg, reply *bool) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.ProcessAccepted(*args)
	*reply = true
	return nil
}

func (nr *NodeRPC) HandleCommit(args *CommitMsg, reply *bool) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.mu.Lock()
	if args.Ballot.Number > nr.NodeState.Ballot.Number {
		nr.NodeState.IsLeader = false
	}
	nr.NodeState.mu.Unlock()

	select {
	case nr.NodeState.HeartbeatChan <- struct{}{}:
	default:
	}
	nr.NodeState.processCommit(*args)
	*reply = true
	return nil
}

func (nr *NodeRPC) HandleNewView(args *NewViewMsg, reply *bool) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.mu.Lock()
	now := time.Now()
	nr.NodeState.lastHeartbeatAt = now
	nr.NodeState.suppressUntil = now.Add(quietPeriod)
	nr.NodeState.mu.Unlock()

	go nr.NodeState.handleNewView(*args)
	*reply = true
	return nil
}

func (nr *NodeRPC) HandleHeartbeat(_ *struct{}, _ *struct{}) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.mu.Lock()
	now := time.Now()
	nr.NodeState.lastHeartbeatAt = now
	nr.NodeState.suppressUntil = now.Add(quietPeriod / 2)
	d := jitteredTimeout()
	nr.NodeState.timerExpired = false
	nr.NodeState.timerDue = now.Add(d)
	nr.NodeState.mu.Unlock()

	select {
	case nr.NodeState.HeartbeatChan <- struct{}{}:
	default:
	}
	return nil
}

func (nr *NodeRPC) PrintLog(_ *struct{}, _ *struct{}) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.PrintLog()
	return nil
}

func (nr *NodeRPC) PrintStatus(args *PrintStatusArgs, _ *struct{}) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.PrintStatus(args.SeqNum)
	return nil
}

func (nr *NodeRPC) PrintView(_ *struct{}, _ *struct{}) error {
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	nr.NodeState.PrintView()
	return nil
}

func (nr *NodeRPC) PrintDB(_ *struct{}, out *map[string]int) error {
	if nr.NodeState == nil {
		return fmt.Errorf("node state is nil")
	}
	if nr.NodeState.Crashed {
		return fmt.Errorf("node %d is down", nr.NodeState.ID)
	}
	snap, err := nr.NodeState.DB.GetDB()
	if err != nil {
		return err
	}
	*out = snap
	return nil
}

func (nr *NodeRPC) PauseTimer(_ *struct{}, _ *struct{}) error {
	nr.NodeState.mu.Lock()
	defer nr.NodeState.mu.Unlock()
	nr.NodeState.TimerPaused = true
	return nil
}

func (nr *NodeRPC) ResumeTimer(_ *struct{}, _ *struct{}) error {
	nr.NodeState.mu.Lock()
	nr.NodeState.TimerPaused = false
	nr.NodeState.mu.Unlock()
	
	// Send heartbeat signal to immediately reset timer to normal timeout
	select {
	case nr.NodeState.HeartbeatChan <- struct{}{}:
	default:
	}
	return nil
}

// FetchCommits allows followers to fetch a range of already-committed entries from the leader
func (nr *NodeRPC) FetchCommits(args *FetchCommitsArgs, reply *FetchCommitsReply) error {
	n := nr.NodeState
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.Crashed {
		return fmt.Errorf("node %d is down", n.ID)
	}
	
	out := make([]CommitMsg, 0, args.To-args.From+1)
	for s := args.From; s <= args.To; s++ {
		if cm, ok := n.CommitLog[s]; ok {
			out = append(out, cm)
		}
	}
	reply.Commits = out
	return nil
}
