package paxos

// Transaction represents a single banking operation.
type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

// Ballot represents a ballot number.
type Ballot struct {
	Number int
	NodeID int
}

// ClientRequest is the message a client sends to a node.
type ClientRequest struct {
	RequestID   string
	Transaction Transaction
	Timestamp   int64
	ClientID    string
}

// ClientReply is the message a leader sends back to a client.
type ClientReply struct {
	Timestamp int64
	ClientID  string
	LeaderID  int
	Ballot    Ballot
	Success   bool
	Message   string
}

// AcceptMsg is the (ACCEPT, b, s, m) message.
type AcceptMsg struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
}

// AcceptedMsg is the (ACCEPTED, b, s, m, n_b) message.
type AcceptedMsg struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
	NodeID  int
}

// CommitMsg is the (COMMIT, b, s, m) message.
type CommitMsg struct {
	Ballot  Ballot
	SeqNum  int
	Request ClientRequest
}

// PrepareMsg is the (PREPARE, b) message.
type PrepareMsg struct {
	Ballot Ballot
}

// PromiseMsg is the (ACK, b, AcceptLog) message.
type PromiseMsg struct {
	Ballot    Ballot
	NodeID    int
	AcceptLog map[int]AcceptMsg
}

// NewViewMsg is the (NEW-VIEW, b, AcceptLog) message.
type NewViewMsg struct {
	Ballot    Ballot
	AcceptLog map[int]AcceptMsg
}

// PrintStatusArgs is for the PrintStatus utility RPC.
type PrintStatusArgs struct {
	SeqNum int
}

// FetchCommitsArgs is for fetching a range of already-committed entries from a leader.
type FetchCommitsArgs struct {
	From int // inclusive
	To   int // inclusive
}

// FetchCommitsReply contains the requested commits.
type FetchCommitsReply struct {
	Commits []CommitMsg
}
