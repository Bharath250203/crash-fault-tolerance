package client

import (
	"fmt"
	"log"
	"multipaxos/paxos"
	"net/rpc"
	"time"
)

type Client struct {
	ID             string
	RequestSeq     int
	BelievedLeader int
	NodeAddrs      []string
}

func NewClient(id string, nodeAddrs []string) *Client {
	return &Client{
		ID:        id,
		NodeAddrs: nodeAddrs,
	}
}

// SendRequest sends a transaction and handles timeouts.
func (c *Client) SendRequest(tx paxos.Transaction) {
	c.RequestSeq++
	request := paxos.ClientRequest{
		RequestID:   fmt.Sprintf("%s-%d", c.ID, c.RequestSeq),
		ClientID:    c.ID,
		Transaction: tx,
		Timestamp:   time.Now().UnixNano(),
	}

	replyChan := make(chan *paxos.ClientReply, len(c.NodeAddrs))

	// Initially send to the believed leader.
	go c.sendToNode(c.BelievedLeader, request, replyChan)

	// Set a timer.
	timeout := time.NewTimer(5 * time.Second)

	select {
	case reply := <-replyChan:
		log.Printf("Client %s: Received reply for request.", c.ID)
		if reply != nil {
			c.BelievedLeader = reply.LeaderID // Update leader belief.
		}
		timeout.Stop()
	case <-timeout.C:
		log.Printf("Client %s: TIMED OUT. Broadcasting request to all nodes.", c.ID)
		// On timeout, broadcast to all nodes.
		for i := range c.NodeAddrs {
			go c.sendToNode(i, request, replyChan)
		}
		// Wait for the first successful reply from the broadcast.
		reply := <-replyChan
		log.Printf("Client %s: Received reply from broadcast.", c.ID)
		if reply != nil {
			c.BelievedLeader = reply.LeaderID
		}
	}
}

func (c *Client) sendToNode(nodeID int, request paxos.ClientRequest, replyChan chan<- *paxos.ClientReply) {
	addr := c.NodeAddrs[nodeID]
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return // Node is likely down.
	}
	defer client.Close()

	var reply paxos.ClientReply
	method := fmt.Sprintf("Node%d.HandleClientRequest", nodeID)
	err = client.Call(method, &request, &reply)
	if err != nil {
		return
	}

	// Send successful reply back to the main goroutine.
	if replyChan != nil {
		replyChan <- &reply
	}
	if reply.Ballot.NodeID != 0 {
		c.BelievedLeader = reply.Ballot.NodeID
	} else if reply.LeaderID != 0 {
		c.BelievedLeader = reply.LeaderID
	}

}
