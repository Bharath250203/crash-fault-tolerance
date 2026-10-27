package paxos

import (
	"fmt"
	"math/rand"
	"multipaxos/store"
	"net/rpc"
	"sync"
	"time"
)

const (
	heartbeatInterval = 300 * time.Millisecond
	electionTimeoutLo = 2 * time.Second
	electionTimeoutHi = 3 * time.Second
	quietPeriod       = 600 * time.Millisecond
)

func jitteredTimeout() time.Duration {
	return electionTimeoutLo + time.Duration(rand.Intn(int(electionTimeoutHi-electionTimeoutLo)))
}

type Node struct {
	mu                  sync.Mutex
	ID                  int
	IsLeader            bool
	SeqNum              int
	Ballot              Ballot
	NodeAddrs           []string
	AcceptLog           map[int]AcceptMsg
	AcceptedVotes       map[int]map[int]bool
	PromiseQuorum       map[int]PromiseMsg
	CommittedLog        map[int]bool
	ExecutedLog         map[int]bool
	ViewLog             []NewViewMsg
	DB                  *store.Datastore
	HeartbeatChan       chan struct{}
	LiveNodes           map[int]bool
	LiveNodesMu         *sync.Mutex
	PendingCommits      map[int]CommitMsg
	NextExecSeq         int
	PendingRequests     map[string]chan ClientReply
	LastReplies         map[string]ClientReply
	ExecutedReqIDs      map[string]bool  // Exactly-once execution guard (prevents duplicate DB updates)
	
	// Catch-up state for gap-tolerant commit processing
	lastCommitted       int                   // highest consecutively executed seq
	commitBuf           map[int]CommitMsg     // out-of-order commits buffered until gaps fill
	CommitLog           map[int]CommitMsg     // every node keeps commits it learns (leader serves to followers)
	
	QuitCh              chan struct{}
	Crashed             bool
	TimerPaused         bool
	lastPrepareSeenAt   time.Time
	lastPrepareSentAt   time.Time
	lastHeartbeatAt     time.Time
	suppressUntil       time.Time
	electionInProgress  bool
	timerExpired        bool
	timerDue            time.Time
	highestSeenPrepare  Ballot
	missedQuorumWindows int
	lastHeartbeatAcks   int
	lastHeartbeatQuorum bool
	lastQuorumState     bool  // Track previous quorum state for transition detection
}

func (n *Node) majorityRequired() int {
	return (len(n.NodeAddrs) / 2) + 1
}

func NewNode(id int, addrs []string, db *store.Datastore, liveNodes map[int]bool, mu *sync.Mutex) *Node {
	now := time.Now()
	firstDue := now.Add(jitteredTimeout())
	return &Node{
		ID:                  id,
		NodeAddrs:           addrs,
		DB:                  db,
		Ballot:              Ballot{Number: 0, NodeID: id},
		HeartbeatChan:       make(chan struct{}, 256),
		AcceptLog:           make(map[int]AcceptMsg),
		AcceptedVotes:       make(map[int]map[int]bool),
		CommittedLog:        make(map[int]bool),
		ExecutedLog:         make(map[int]bool),
		ViewLog:             make([]NewViewMsg, 0),
		LiveNodes:           liveNodes,
		LiveNodesMu:         mu,
		PendingCommits:      make(map[int]CommitMsg),
		NextExecSeq:         1,
		PendingRequests:     make(map[string]chan ClientReply),
		LastReplies:         make(map[string]ClientReply),
		ExecutedReqIDs:      make(map[string]bool),
		
		// Initialize catch-up state
		lastCommitted:       0,
		commitBuf:           make(map[int]CommitMsg),
		CommitLog:           make(map[int]CommitMsg),
		
		QuitCh:              make(chan struct{}),
		Crashed:             false,
		TimerPaused:         false,
		lastPrepareSeenAt:   now,
		lastPrepareSentAt:   now,
		lastHeartbeatAt:     now,
		suppressUntil:       now,
		timerExpired:        false,
		timerDue:            firstDue,
		highestSeenPrepare:  Ballot{Number: 0, NodeID: -1},
		missedQuorumWindows: 0,
		lastHeartbeatAcks:   0,
		lastHeartbeatQuorum: false,
		lastQuorumState:     true,  // Start assuming we have quorum (all 5 nodes begin live)
	}
}

// hasQuorum checks if enough live nodes are available for quorum
func (n *Node) hasQuorum() bool {
	n.LiveNodesMu.Lock()
	live := 0
	for _, ok := range n.LiveNodes {
		if ok {
			live++
		}
	}
	n.LiveNodesMu.Unlock()
	return live >= n.majorityRequired()
}

func (n *Node) StepDownAndTriggerElection() {
	n.mu.Lock()
	if n.IsLeader {
		n.IsLeader = false
	}
	n.mu.Unlock()
	go n.StartLeaderElection()
}

func (n *Node) OnLiveSetChanged() {
	n.mu.Lock()
	hadQuorum := n.lastQuorumState
	n.mu.Unlock()
	
	nowHas := n.hasQuorum()
	
	n.mu.Lock()
	n.lastQuorumState = nowHas
	n.mu.Unlock()
	
	// Trigger election only on quorum restoration to pull forward uncommitted transactions
	if !hadQuorum && nowHas {
		n.StepDownAndTriggerElection()
	}
}

func (n *Node) RunTimer() {
	timer := time.NewTimer(time.Until(n.timerDue))
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(time.Until(n.timerDue))

	for {
		select {
		case <-n.QuitCh:
			return
		default:
		}

		select {
		case <-n.HeartbeatChan:
			d := jitteredTimeout()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
			n.mu.Lock()
			n.timerExpired = false
			n.timerDue = time.Now().Add(d)
			n.mu.Unlock()

		case <-timer.C:
			now := time.Now()
			n.mu.Lock()
			crashed := n.Crashed
			paused := n.TimerPaused
			leader := n.IsLeader
			inProgress := n.electionInProgress
			n.timerExpired = true
			n.timerDue = now
			adopt := n.highestSeenPrepare.Number > n.Ballot.Number ||
				(n.highestSeenPrepare.Number == n.Ballot.Number && n.highestSeenPrepare.NodeID > n.Ballot.NodeID)
			if adopt {
				n.Ballot = n.highestSeenPrepare
				n.IsLeader = false
				n.lastHeartbeatAt = now
				n.suppressUntil = now.Add(quietPeriod)
			}
			n.mu.Unlock()

			n.mu.Lock()
			leaderFresh := now.Sub(n.lastHeartbeatAt) < (electionTimeoutLo / 2)
			tooSoon := now.Before(n.suppressUntil)
			n.mu.Unlock()

			n.LiveNodesMu.Lock()
			isLive := n.LiveNodes[n.ID]
			liveCount := 0
			for _, v := range n.LiveNodes {
				if v {
					liveCount++
				}
			}
			n.LiveNodesMu.Unlock()
			reachableMajority := liveCount >= n.majorityRequired()

			if paused {
				// Timer is paused, use long timeout to avoid frequent firing
			} else if !(crashed || leader || inProgress || !isLive || leaderFresh || tooSoon) {
				if reachableMajority {
					go n.StartLeaderElection()
				}
			}

			// Use longer timeout when paused to reduce CPU usage
			var d time.Duration
			if paused {
				d = 30 * time.Second // Long timeout when paused
			} else {
				d = jitteredTimeout() // Normal timeout
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(d)
			n.mu.Lock()
			n.timerExpired = false
			n.timerDue = time.Now().Add(d)
			n.mu.Unlock()
		}
	}
}

func (n *Node) RunLeaderHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	const demoteThreshold = 2

	for {
		select {
		case <-n.QuitCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.IsLeader
			isCrashed := n.Crashed
			n.mu.Unlock()

			n.LiveNodesMu.Lock()
			isLive := n.LiveNodes[n.ID]
			peerIDs := make([]int, 0, len(n.NodeAddrs))
			for i := range n.NodeAddrs {
				if i == n.ID {
					continue
				}
				if n.LiveNodes[i] {
					peerIDs = append(peerIDs, i)
				}
			}
			n.LiveNodesMu.Unlock()

			if !isLeader {
				continue
			}

			acks := 0
			if isLive && !isCrashed {
				acks = 1
				for _, pid := range peerIDs {
					if n.sendHeartbeatSync(pid) {
						acks++
					}
				}
			}

		n.mu.Lock()
		n.lastHeartbeatAcks = acks
		hasQuorum := acks >= n.majorityRequired()
		n.lastHeartbeatQuorum = hasQuorum
		if hasQuorum {
			n.missedQuorumWindows = 0
		} else {
			n.missedQuorumWindows++
		}
		// DO NOT demote leader based on heartbeat quorum
		// Leader should continue broadcasting messages even without quorum
		// Messages won't commit until quorum is restored, but they stay in AcceptLog
		// for later NEW-VIEW to pick up
		n.mu.Unlock()
		}
	}
}

func (n *Node) sendHeartbeatSync(nodeID int) bool {
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[nodeID]
	addr := n.NodeAddrs[nodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return false
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return false
	}
	defer client.Close()
	var reply struct{}
	method := fmt.Sprintf("Node%d.HandleHeartbeat", nodeID)
	if err := client.Call(method, &struct{}{}, &reply); err != nil {
		return false
	}
	return true
}

func (n *Node) HandleClientRequest(request ClientRequest) {
	n.mu.Lock()
	if !n.IsLeader {
		n.mu.Unlock()
		return
	}
	
	// Paxos-level deduplication to prevent duplicate instances on client retry
	for _, acceptMsg := range n.AcceptLog {
		if acceptMsg.Request.RequestID == request.RequestID {
			if n.CommittedLog[acceptMsg.SeqNum] {
				if reply, ok := n.LastReplies[request.ClientID]; ok {
					if replyChan, exists := n.PendingRequests[request.RequestID]; exists {
						replyChan <- reply
						delete(n.PendingRequests, request.RequestID)
					}
				}
			}
			n.mu.Unlock()
			return
		}
	}
	
	n.SeqNum++
	currentSeq := n.SeqNum
	n.AcceptedVotes[currentSeq] = make(map[int]bool)
	n.AcceptedVotes[currentSeq][n.ID] = true
	acceptMsg := AcceptMsg{
		Ballot:  n.Ballot,
		SeqNum:  currentSeq,
		Request: request,
	}
	n.AcceptLog[currentSeq] = acceptMsg
	n.mu.Unlock()
	for i, addr := range n.NodeAddrs {
		if i == n.ID {
			continue
		}
		go n.sendAccept(addr, i, acceptMsg)
	}
}

func (n *Node) ProcessAccepted(reply AcceptedMsg) {
	n.mu.Lock()
	
	// Re-send COMMIT for already-committed sequences (catch-up for recovered nodes)
	if reply.Ballot.Number == n.Ballot.Number && n.CommittedLog[reply.SeqNum] {
		if acceptMsg, ok := n.AcceptLog[reply.SeqNum]; ok {
		commitMsg := CommitMsg{
			Ballot:  n.Ballot,
			SeqNum:  reply.SeqNum,
			Request: acceptMsg.Request,
		}
		addr := n.NodeAddrs[reply.NodeID]
		nodeID := reply.NodeID
		n.mu.Unlock()
		n.sendCommit(addr, nodeID, commitMsg)
		} else {
			n.mu.Unlock()
		}
		return
	}
	defer n.mu.Unlock()
	
	if reply.Ballot.Number == n.Ballot.Number && !n.CommittedLog[reply.SeqNum] {
		currentSeq := reply.SeqNum
		if _, ok := n.AcceptedVotes[currentSeq]; !ok {
			n.AcceptedVotes[currentSeq] = make(map[int]bool)
		}
		n.AcceptedVotes[currentSeq][reply.NodeID] = true
		requiredVotes := n.majorityRequired()
		if len(n.AcceptedVotes[currentSeq]) >= requiredVotes {
			n.CommittedLog[currentSeq] = true
			commitMsg := CommitMsg{
				Ballot:  n.Ballot,
				SeqNum:  currentSeq,
				Request: n.AcceptLog[currentSeq].Request,
			}
			n.CommitLog[currentSeq] = commitMsg
			go n.broadcastCommit(commitMsg)
		}
	}
}

func (n *Node) StartLeaderElection() {
	n.mu.Lock()
	now := time.Now()
	if now.Sub(n.lastHeartbeatAt) < (electionTimeoutLo / 2) {
		n.mu.Unlock()
		return
	}
	if n.Crashed || n.IsLeader || n.electionInProgress || now.Before(n.suppressUntil) {
		n.mu.Unlock()
		return
	}
	n.LiveNodesMu.Lock()
	liveCount := 0
	for _, v := range n.LiveNodes {
		if v {
			liveCount++
		}
	}
	n.LiveNodesMu.Unlock()
	if liveCount < n.majorityRequired() {
		n.mu.Unlock()
		return
	}
	n.electionInProgress = true
	n.lastPrepareSentAt = now
	n.IsLeader = false
	n.Ballot = Ballot{Number: n.Ballot.Number + 1, NodeID: n.ID}
	prepareMsg := PrepareMsg{Ballot: n.Ballot}
	n.PromiseQuorum = make(map[int]PromiseMsg)
	n.PromiseQuorum[n.ID] = PromiseMsg{
		Ballot:    n.Ballot,
		NodeID:    n.ID,
		AcceptLog: n.AcceptLog,
	}
	requiredPromises := n.majorityRequired()
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		n.electionInProgress = false
		n.mu.Unlock()
	}()

	replies := make(chan PromiseMsg, len(n.NodeAddrs)-1)
	for i, addr := range n.NodeAddrs {
		if i == n.ID {
			continue
		}
		go n.sendPrepare(addr, i, prepareMsg, replies)
	}

	timeout := time.After(2 * time.Second)
	received := 1
	for {
		select {
		case reply := <-replies:
			if reply.NodeID != -1 && reply.Ballot.Number == n.Ballot.Number {
				n.mu.Lock()
				if _, ok := n.PromiseQuorum[reply.NodeID]; !ok {
					n.PromiseQuorum[reply.NodeID] = reply
					received++
				}
				if received >= requiredPromises {
					n.IsLeader = true
					n.lastHeartbeatAt = time.Now()
					n.suppressUntil = n.lastHeartbeatAt.Add(quietPeriod)
					n.mu.Unlock()
					n.sendHeartbeatBurst()
					n.formAndBroadcastNewView()
					return
				}
			n.mu.Unlock()
		}
	case <-timeout:
		return
		}
	}
}

func (n *Node) sendHeartbeatBurst() {
	n.LiveNodesMu.Lock()
	addrs := make([]string, len(n.NodeAddrs))
	copy(addrs, n.NodeAddrs)
	n.LiveNodesMu.Unlock()
	for i, addr := range addrs {
		if i == n.ID {
			continue
		}
		go n.sendHeartbeat(addr, i)
	}
}

func (n *Node) formAndBroadcastNewView() {
	n.mu.Lock()
	defer n.mu.Unlock()
	consolidatedLog := make(map[int]AcceptMsg)
	highestSeq := 0
	for _, promise := range n.PromiseQuorum {
		for seq, acceptedMsg := range promise.AcceptLog {
			if existingMsg, ok := consolidatedLog[seq]; ok {
				if acceptedMsg.Ballot.Number > existingMsg.Ballot.Number {
					consolidatedLog[seq] = acceptedMsg
				}
			} else {
				consolidatedLog[seq] = acceptedMsg
			}
			if seq > highestSeq {
				highestSeq = seq
			}
		}
	}

	newLog := make(map[int]AcceptMsg)
	for i := 1; i <= highestSeq; i++ {
		if msg, ok := consolidatedLog[i]; ok {
			msg.Ballot = n.Ballot
			newLog[i] = msg
		} else {
			newLog[i] = AcceptMsg{
				Ballot:  n.Ballot,
				SeqNum:  i,
				Request: ClientRequest{Transaction: Transaction{Sender: "no-op"}},
			}
		}
	}
	n.SeqNum = highestSeq
	newViewMsg := NewViewMsg{
		Ballot:    n.Ballot,
		AcceptLog: newLog,
	}
	n.ViewLog = append(n.ViewLog, newViewMsg)
	n.AcceptLog = newLog
	for i, addr := range n.NodeAddrs {
		if i == n.ID {
			go n.handleNewView(newViewMsg)
			continue
		}
		go n.sendNewView(addr, i, newViewMsg)
	}
}

func (n *Node) handleNewView(msg NewViewMsg) {
	n.mu.Lock()
	if msg.Ballot.NodeID != n.ID {
		n.IsLeader = false
	}
	n.Ballot = msg.Ballot
	n.AcceptLog = msg.AcceptLog
	n.ViewLog = append(n.ViewLog, msg)

	highestSeq := 0
	for seq := range msg.AcceptLog {
		if seq > highestSeq {
			highestSeq = seq
		}
	}
	n.SeqNum = highestSeq
	now := time.Now()
	n.lastHeartbeatAt = now
	n.suppressUntil = now.Add(quietPeriod)
	
	// Trigger catch-up for any gaps
	leaderID := msg.Ballot.NodeID
	addr := n.NodeAddrs[leaderID]
	lastComm := n.lastCommitted
	n.mu.Unlock()

	for _, acceptMsg := range msg.AcceptLog {
		ack := AcceptedMsg{
			Ballot:  acceptMsg.Ballot,
			SeqNum:  acceptMsg.SeqNum,
			Request: acceptMsg.Request,
			NodeID:  n.ID,
		}
		if msg.Ballot.NodeID == n.ID {
			n.ProcessAccepted(ack)
		} else {
			go n.sendAccepted(n.NodeAddrs[msg.Ballot.NodeID], ack)
		}
	}
	
	// Fetch any missing commits to close gaps quickly
	if highestSeq > lastComm {
		go n.fetchAndApplyRange(lastComm+1, highestSeq, leaderID, addr)
	}
}

// processCommit is gap-aware: it buffers out-of-order commits and fetches missing ranges
func (n *Node) processCommit(cm CommitMsg) {
	n.mu.Lock()
	if n.Crashed {
		n.mu.Unlock()
		return
	}

	n.CommitLog[cm.SeqNum] = cm

	switch {
	case cm.SeqNum == n.lastCommitted+1:
		n.applyAndAdvance(cm)
		for {
			next := n.lastCommitted + 1
			buffered, ok := n.commitBuf[next]
			if !ok {
				break
			}
			delete(n.commitBuf, next)
			n.applyAndAdvance(buffered)
		}

	case cm.SeqNum > n.lastCommitted+1:
		n.commitBuf[cm.SeqNum] = cm
		from := n.lastCommitted + 1
		to := cm.SeqNum - 1
		leaderID := n.Ballot.NodeID
		if leaderID < 0 || leaderID >= len(n.NodeAddrs) {
			n.mu.Unlock()
			return
		}
		addr := n.NodeAddrs[leaderID]
		n.mu.Unlock()

		go n.fetchAndApplyRange(from, to, leaderID, addr)
		return

	default:
	}

	n.mu.Unlock()
}

func (n *Node) applyAndAdvance(cm CommitMsg) {
	n.executeTransaction(cm.Request.Transaction, cm.SeqNum, cm.Request)
	n.lastCommitted = cm.SeqNum
}

// fetchAndApplyRange fetches missing commits from the leader and applies them
func (n *Node) fetchAndApplyRange(from, to, leaderID int, addr string) {
	if from > to || leaderID < 0 {
		return
	}
	
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()

	var reply FetchCommitsReply
	args := FetchCommitsArgs{From: from, To: to}
	method := fmt.Sprintf("Node%d.FetchCommits", leaderID)
	if err := client.Call(method, &args, &reply); err != nil {
		return
	}
	
	// Re-inject through the same handler; sequencing logic will apply in order
	for _, cm := range reply.Commits {
		n.processCommit(cm)
	}
}

func (n *Node) sendHeartbeat(addr string, nodeID int) {
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[nodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply struct{}
	method := fmt.Sprintf("Node%d.HandleHeartbeat", nodeID)
	_ = client.Call(method, &struct{}{}, &reply)
}

func (n *Node) sendAccept(addr string, nodeID int, msg AcceptMsg) {
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[nodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply AcceptedMsg
	method := fmt.Sprintf("Node%d.HandleAccept", nodeID)
	if client.Call(method, &msg, &reply) == nil && reply.NodeID != -1 {
		n.ProcessAccepted(reply)
	}
}

func (n *Node) broadcastCommit(msg CommitMsg) {
	n.processCommit(msg)
	for i, addr := range n.NodeAddrs {
		if i == n.ID {
			continue
		}
		go n.sendCommit(addr, i, msg)
	}
}

func (n *Node) sendCommit(addr string, nodeID int, msg CommitMsg) {
	// Note: We don't check isLive here because recovered nodes need to catch up
	// The RPC will fail naturally if the node is actually down
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply bool
	method := fmt.Sprintf("Node%d.HandleCommit", nodeID)
	_ = client.Call(method, &msg, &reply)
}

func (n *Node) sendPrepare(addr string, nodeID int, msg PrepareMsg, replyChan chan<- PromiseMsg) {
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[nodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply PromiseMsg
	method := fmt.Sprintf("Node%d.HandlePrepare", nodeID)
	if client.Call(method, &msg, &reply) == nil {
		replyChan <- reply
	}
}

func (n *Node) sendNewView(addr string, nodeID int, msg NewViewMsg) {
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[nodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply bool
	method := fmt.Sprintf("Node%d.HandleNewView", nodeID)
	_ = client.Call(method, &msg, &reply)
}

func (n *Node) sendAccepted(addr string, msg AcceptedMsg) {
	destNodeID := -1
	for i, a := range n.NodeAddrs {
		if a == addr {
			destNodeID = i
			break
		}
	}
	if destNodeID == -1 {
		return
	}
	n.LiveNodesMu.Lock()
	isLive := n.LiveNodes[destNodeID]
	n.LiveNodesMu.Unlock()
	if !isLive {
		return
	}
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()
	var reply bool
	method := fmt.Sprintf("Node%d.HandleAccepted", destNodeID)
	_ = client.Call(method, &msg, &reply)
}

func (n *Node) executeTransaction(tx Transaction, seqNum int, request ClientRequest) {
	if n.ExecutedLog[seqNum] {
		return
	}
	
	// Execution-level deduplication to prevent NEW-VIEW replays and catch-up duplicates
	if request.RequestID != "" {
		if n.ExecutedReqIDs[request.RequestID] {
			n.ExecutedLog[seqNum] = true
			return
		}
		n.ExecutedReqIDs[request.RequestID] = true
	}
	
	n.ExecutedLog[seqNum] = true
	
	success := true
	if tx.Sender != "no-op" {
		senderBalance, err := n.DB.GetBalance(tx.Sender)
		if err != nil {
			success = false
		} else if senderBalance < tx.Amount {
			success = false
		} else {
			receiverBalance, _ := n.DB.GetBalance(tx.Receiver)
			_ = n.DB.SetBalance(tx.Sender, senderBalance-tx.Amount)
			_ = n.DB.SetBalance(tx.Receiver, receiverBalance+tx.Amount)
		}
	}

	reply := ClientReply{
		Timestamp: request.Timestamp,
		ClientID:  request.ClientID,
		LeaderID:  n.ID,
		Ballot:    n.Ballot,
		Success:   success,
	}
	
	if request.ClientID != "" {
		n.LastReplies[request.ClientID] = reply
	}
	
	if n.IsLeader {
		if replyChan, ok := n.PendingRequests[request.RequestID]; ok {
			replyChan <- reply
			delete(n.PendingRequests, request.RequestID)
		}
	}
}

func (n *Node) PrintLog() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("--- AcceptLog for Node %d ---\n", n.ID)
	for seq, msg := range n.AcceptLog {
		fmt.Printf("Seq: %d, Ballot: <%d,%d>, ClientID: %s, ReqID: %s, Tx: %v\n", 
			seq, msg.Ballot.Number, msg.Ballot.NodeID, 
			msg.Request.ClientID, msg.Request.RequestID, msg.Request.Transaction)
	}
	fmt.Println("---------------------------")
}

func (n *Node) PrintStatus(seqNum int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	status := "X"
	if _, ok := n.ExecutedLog[seqNum]; ok {
		status = "E"
	} else if _, ok := n.CommittedLog[seqNum]; ok {
		status = "C"
	} else if _, ok := n.AcceptLog[seqNum]; ok {
		status = "A"
	}
	fmt.Printf("Status for Seq %d on Node %d: %s\n", seqNum, n.ID, status)
}

func (n *Node) PrintView() {
	n.mu.Lock()
	defer n.mu.Unlock()
	fmt.Printf("--- ViewLog for Node %d ---\n", n.ID)
	for i, view := range n.ViewLog {
		fmt.Printf("View %d: Ballot <%d,%d>, Log Size: %d\n", i+1, view.Ballot.Number, view.Ballot.NodeID, len(view.AcceptLog))
		if len(view.AcceptLog) > 0 {
			fmt.Printf("  Sequences: ")
			seqs := make([]int, 0, len(view.AcceptLog))
			for seq := range view.AcceptLog {
				seqs = append(seqs, seq)
			}
			// Sort for consistent output
			for i := 0; i < len(seqs); i++ {
				for j := i + 1; j < len(seqs); j++ {
					if seqs[i] > seqs[j] {
						seqs[i], seqs[j] = seqs[j], seqs[i]
					}
				}
			}
			for _, seq := range seqs {
				fmt.Printf("%d ", seq)
			}
			fmt.Println()
		}
	}
	fmt.Println("-------------------------")
}

