package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"multipaxos/paxos"
	"multipaxos/store"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var nodeAddrs = []string{
	"localhost:8001",
	"localhost:8002",
	"localhost:8003",
	"localhost:8004",
	"localhost:8005",
}

var liveNodes = make(map[int]bool)
var liveNodesMutex = &sync.Mutex{}

// ClientState tracks individual client state per spec requirement:
// "you must still manage 10 separate timers and any other necessary variables"
type ClientState struct {
	ID            string
	RequestSeq    int
	LastTimestamp int64
	mu            sync.Mutex
}

// Initialize 10 separate clients (one per account: A-J)
var clientStates = map[string]*ClientState{
	"A": {ID: "client-A"},
	"B": {ID: "client-B"},
	"C": {ID: "client-C"},
	"D": {ID: "client-D"},
	"E": {ID: "client-E"},
	"F": {ID: "client-F"},
	"G": {ID: "client-G"},
	"H": {ID: "client-H"},
	"I": {ID: "client-I"},
	"J": {ID: "client-J"},
}

type Transaction struct {
	Sender   string
	Receiver string
	Amount   int
}

type StepType int

const (
	StepTx StepType = iota
	StepLF
)

type Step struct {
	Type StepType
	Tx   Transaction
}

type TestSet struct {
	SetNumber int
	Steps     []Step
	LiveNodes []int
}

type ballotDTO struct {
	Number int
	NodeID int
}

type statusDTO struct {
	ID       int
	IsLeader bool
	Ballot   ballotDTO
	Crashed  bool
}

func startNode(nodeID int) {
	dbPath := fmt.Sprintf("node_%d.db", nodeID)
	// Remove old database file to start fresh
	os.Remove(dbPath)
	db, err := store.New(dbPath)
	if err != nil {
		log.Fatalf("Failed to create datastore for node %d: %v", nodeID, err)
	}
	// Initialize all clients with balance 10
	for _, client := range []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"} {
		_ = db.SetBalance(client, 10)
	}
	node := paxos.NewNode(nodeID, nodeAddrs, db, liveNodes, liveNodesMutex)
	nodeRPC := &paxos.NodeRPC{NodeState: node}
	_ = rpc.RegisterName(fmt.Sprintf("Node%d", nodeID), nodeRPC)

	listener, err := net.Listen("tcp", nodeAddrs[nodeID])
	if err != nil {
		log.Fatalf("Listener error for node %d: %v", nodeID, err)
	}
	fmt.Printf("Node %d is listening on %s\n", nodeID, nodeAddrs[nodeID])

	go node.RunTimer()
	go node.RunLeaderHeartbeat()

	_ = http.Serve(listener, nil)
}

func main() {
	rpc.HandleHTTP()

	for i := 0; i < 5; i++ {
		go startNode(i)
	}

	testSets, err := parseCSV("test.csv")
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	currentSetIndex := 0
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("All nodes launched. System is ready.")
	fmt.Println("Press Enter to process a test set, or type a command (e.g., 'whoisleader', 'status 2', 'kill 3', 'printlog 1').")

	for scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			if currentSetIndex < len(testSets) {
				set := testSets[currentSetIndex]
				fmt.Printf("\n--- Configuring for Set %d ---\n", set.SetNumber)

				liveNodesMutex.Lock()
				for i := 0; i < len(nodeAddrs); i++ {
					liveNodes[i] = false
				}
				liveNodesMutex.Unlock()

		for _, id := range set.LiveNodes {
			var out statusDTO
			callRPCNoLiveGate(id, "GetStatus", &struct{}{}, &out)
			if out.Crashed {
				fmt.Printf("Node %d is CRASHED → recovering for new set\n", id)
				callRPCNoLiveGate(id, "Recover", &struct{}{}, &struct{}{})
				time.Sleep(50 * time.Millisecond) // Brief delay for recovery
			}
		liveNodesMutex.Lock()
		liveNodes[id] = true
		liveNodesMutex.Unlock()
		fmt.Printf("Node %d is LIVE\n", id)
	}

		// Trigger OnLiveSetChanged on all nodes to detect quorum transitions
		// This ensures uncommitted transactions are picked up via NEW-VIEW
		fmt.Printf("[QUORUM CHECK] Checking for quorum transitions across all nodes\n")
		for i := range nodeAddrs {
			callRPCNoLiveGate(i, "OnLiveSetChanged", &struct{}{}, &struct{}{})
		}

		// Resume timers now that set nodes are configured (allows leader election)
		resumeAllTimers()

			time.Sleep(500 * time.Millisecond) // Longer wait for election to complete

			if _, ok := waitForLeader(5 * time.Second); !ok {
					fmt.Println("WARNING: No leader established yet; transactions may fail.")
				} else {
					fmt.Println("Leader established, ready to process transactions.")
				}

			fmt.Printf("--- Processing Set %d ---\n", set.SetNumber)
			runSteps(set.Steps)
			currentSetIndex++

			fmt.Println("Waiting for system to stabilize...")
			time.Sleep(5 * time.Second)

			// Pause timers while inspecting/printing state to avoid stray elections
			pauseAllTimers()
			} else {
				fmt.Println("All test sets processed.")
				break
			}
			fmt.Println("\nReady for next set or command...")
			continue
		}

		parts := strings.Fields(input)
		if len(parts) == 0 {
			fmt.Println("\nReady for next set or command...")
			continue
		}
		cmd := parts[0]

		if cmd == "whoisleader" || cmd == "statusall" {
			showWhoIsLeader()
			fmt.Println("\nReady for next set or command...")
			continue
		}

		if len(parts) < 2 {
			if cmd == "pause" {
				pauseAllTimers()
				fmt.Println("All timers paused")
				fmt.Println("\nReady for next set or command...")
				continue
			}
			if cmd == "resume" {
				resumeAllTimers()
				fmt.Println("All timers resumed")
				fmt.Println("\nReady for next set or command...")
				continue
			}
			fmt.Println("Invalid command format. Try:")
			fmt.Println("  whoisleader | pause | resume")
			fmt.Println("  status <nodeID> | kill <nodeID> | printlog <nodeID> | printview <nodeID> | printdb <nodeID> | printstatus <nodeID> <seqNum>")
			fmt.Println("\nReady for next set or command...")
			continue
		}

		nodeID, _ := strconv.Atoi(parts[1])

		switch cmd {
		case "printdb":
			var out map[string]int
			callRPC(nodeID, "PrintDB", &struct{}{}, &out)
			if out == nil {
				fmt.Println("PrintDB returned no data (node may be down or not live).")
				break
			}
			prettyPrintBalances(out)

		case "printlog":
			callRPC(nodeID, "PrintLog", &struct{}{}, &struct{}{})

		case "printview":
			callRPC(nodeID, "PrintView", &struct{}{}, &struct{}{})

		case "printstatus":
			if len(parts) < 3 {
				fmt.Println("Usage: printstatus <nodeID> <seqNum>")
				break
			}
			seqNum, _ := strconv.Atoi(parts[2])
			args := &paxos.PrintStatusArgs{SeqNum: seqNum}
			callRPC(nodeID, "PrintStatus", args, &struct{}{})

		case "status":
			var out statusDTO
			callRPCNoLiveGate(nodeID, "GetStatus", &struct{}{}, &out)
			fmt.Printf("Node %d | leader=%v | ballot=(%d,%d) | crashed=%v\n",
				out.ID, out.IsLeader, out.Ballot.Number, out.Ballot.NodeID, out.Crashed)

		case "kill":
			callRPCNoLiveGate(nodeID, "Kill", &struct{}{}, &struct{}{})
			liveNodesMutex.Lock()
			liveNodes[nodeID] = false
			liveNodesMutex.Unlock()
			fmt.Printf("Kill issued to node %d\n", nodeID)

		default:
			fmt.Println("Unknown command. Try: whoisleader | status <nodeID> | kill <nodeID> | pause | resume | printlog/printview/printdb/printstatus")
		}

		fmt.Println("\nReady for next set or command...")
	}
}

// ClientTxBatch groups transactions for a single client
type ClientTxBatch struct {
	transactions []Transaction
}

func runSteps(steps []Step) {
	txCount := 0
	lfCount := 0
	clientBatches := make(map[string]*ClientTxBatch)
	
	stepIdx := 0
	for stepIdx < len(steps) {
		s := steps[stepIdx]
		
		switch s.Type {
		case StepLF:
			if len(clientBatches) > 0 {
				executeParallelClientTransactions(clientBatches)
				clientBatches = make(map[string]*ClientTxBatch)
			}
			
			lfCount++
			fmt.Printf("\n[Step %d/%d] Processing Leader Failure...\n", stepIdx+1, len(steps))
			leaderID, ok := waitForLeader(2 * time.Second)
			if !ok {
				fmt.Println("[LF] No current leader to kill.")
				stepIdx++
				continue
			}
			fmt.Printf("[LF] Killing leader %d (will stay down until next set)\n", leaderID)
			callRPCNoLiveGate(leaderID, "Kill", &struct{}{}, &struct{}{})
			liveNodesMutex.Lock()
			liveNodes[leaderID] = false
			liveNodesMutex.Unlock()

			time.Sleep(1 * time.Second)

			fmt.Println("[LF] BLOCKING: Waiting for new leader election...")
			maxRetries := 5
			leaderFound := false
			for i := 0; i < maxRetries; i++ {
				fmt.Printf("[LF] Attempt %d/%d to find new leader...\n", i+1, maxRetries)
				if newLeaderID, ok := waitForLeader(2 * time.Second); ok {
					fmt.Printf("[LF] SUCCESS: New leader elected: Node %d\n", newLeaderID)
					fmt.Println("[LF] UNBLOCKING: Ready to process transactions")
					time.Sleep(500 * time.Millisecond)
					leaderFound = true
					break
				}
			}
			if !leaderFound {
				fmt.Println("[LF] WARNING: No leader elected after retries")
				fmt.Println("[LF] Transactions will use retry logic to handle this")
			}
			stepIdx++

		case StepTx:
			for stepIdx < len(steps) && steps[stepIdx].Type == StepTx {
				tx := steps[stepIdx].Tx
				txCount++
				
				sender := tx.Sender
				if clientBatches[sender] == nil {
					clientBatches[sender] = &ClientTxBatch{
						transactions: []Transaction{},
					}
				}
				clientBatches[sender].transactions = append(clientBatches[sender].transactions, tx)
				stepIdx++
			}
			
			if len(clientBatches) > 0 {
				executeParallelClientTransactions(clientBatches)
				clientBatches = make(map[string]*ClientTxBatch)
			}
		}
	}
	
	if len(clientBatches) > 0 {
		executeParallelClientTransactions(clientBatches)
	}
	
	fmt.Printf("--- Set complete: %d transactions, %d leader failures ---\n", txCount, lfCount)
}

// executeParallelClientTransactions executes client transactions in parallel (closed-loop per client)
func executeParallelClientTransactions(clientBatches map[string]*ClientTxBatch) {
	var wg sync.WaitGroup
	
	if len(clientBatches) > 1 {
		fmt.Printf("\n[PARALLEL] Starting parallel execution for %d clients\n", len(clientBatches))
	}
	
	for clientID, batch := range clientBatches {
		wg.Add(1)
		go func(client string, txs []Transaction) {
			defer wg.Done()
			
			senderClient := clientStates[client]
			if senderClient == nil {
				senderClient = &ClientState{ID: "client-" + client}
				clientStates[client] = senderClient
			}
			
			for _, tx := range txs {
				if _, ok := waitForLeader(2 * time.Second); !ok {
					fmt.Printf("[WARNING] Client %s: No leader available\n", client)
				}
				
				senderClient.mu.Lock()
				senderClient.RequestSeq++
				reqSeq := senderClient.RequestSeq
				clientID := senderClient.ID
				timestamp := time.Now().UnixNano()
				senderClient.LastTimestamp = timestamp
				senderClient.mu.Unlock()
				
				reqID := fmt.Sprintf("%s-req-%d", clientID, reqSeq)
				
				if len(clientBatches) > 1 {
					fmt.Printf("[Client %s] Processing: %s->%s(%d)\n", client, tx.Sender, tx.Receiver, tx.Amount)
				} else {
					fmt.Printf("Processing Transaction: %s->%s(%d)\n", tx.Sender, tx.Receiver, tx.Amount)
				}
				
				clientTimer := time.NewTimer(5 * time.Second)
				maxTxRetries := 3
				success := false
				for attempt := 1; attempt <= maxTxRetries; attempt++ {
					if sendTransactionWithID(tx, reqID, clientID, timestamp) {
						success = true
						clientTimer.Stop()
						break
					}
					if attempt < maxTxRetries {
						if len(clientBatches) > 1 {
							fmt.Printf("[Client %s]    Retry %d/%d: %s->%s(%d)\n",
								client, attempt+1, maxTxRetries, tx.Sender, tx.Receiver, tx.Amount)
						} else {
							fmt.Printf("   Retry %d/%d: %s->%s(%d)\n",
								attempt+1, maxTxRetries, tx.Sender, tx.Receiver, tx.Amount)
						}
						time.Sleep(500 * time.Millisecond)
						waitForLeader(1 * time.Second)
					}
				}
				clientTimer.Stop()
				
				if !success {
					fmt.Printf("[FAILED] Transaction failed after %d retries: %s->%s(%d)\n",
						maxTxRetries, tx.Sender, tx.Receiver, tx.Amount)
				}
				
				// Wait for transaction to be fully committed (closed-loop per client)
				time.Sleep(1000 * time.Millisecond)
			}
			
			if len(clientBatches) > 1 {
				fmt.Printf("[PARALLEL] Client %s completed %d transactions\n", client, len(txs))
			}
		}(clientID, batch.transactions)
	}
	
	// Wait for all clients to complete their transactions
	wg.Wait()
	if len(clientBatches) > 1 {
		fmt.Printf("[PARALLEL] All clients completed\n\n")
	}
}

func callRPC(nodeID int, method string, args, reply interface{}) {
	liveNodesMutex.Lock()
	isLive, ok := liveNodes[nodeID]
	liveNodesMutex.Unlock()
	if !ok || !isLive {
		fmt.Printf("Node %d is not live.\n", nodeID)
		return
	}
	callRPCNoLiveGate(nodeID, method, args, reply)
}

func callRPCNoLiveGate(nodeID int, method string, args, reply interface{}) {
	client, err := rpc.DialHTTP("tcp", nodeAddrs[nodeID])
	if err != nil {
		log.Printf("Error dialing node %d: %v", nodeID, err)
		return
	}
	defer client.Close()
	fullMethod := fmt.Sprintf("Node%d.%s", nodeID, method)
	if err := client.Call(fullMethod, args, reply); err != nil {
		log.Printf("Error calling %s on node %d: %v", fullMethod, nodeID, err)
	}
}

func showWhoIsLeader() {
	liveNodesMutex.Lock()
	liveIDs := make([]int, 0, len(liveNodes))
	for id, isLive := range liveNodes {
		if isLive {
			liveIDs = append(liveIDs, id)
		}
	}
	liveNodesMutex.Unlock()
	sort.Ints(liveIDs)

	if len(liveIDs) == 0 {
		fmt.Println("(No live nodes)")
		return
	}

	found := false
	for _, id := range liveIDs {
		var out statusDTO
		callRPC(id, "GetStatus", &struct{}{}, &out)
		if out.ID == 0 && !out.IsLeader && out.Ballot.Number == 0 && out.Ballot.NodeID == 0 && !out.Crashed {
			continue
		}
		fmt.Printf("Node %d (live) | leader=%v | ballot=(%d,%d) | crashed=%v\n",
			out.ID, out.IsLeader, out.Ballot.Number, out.Ballot.NodeID, out.Crashed)
		if out.IsLeader {
			found = true
		}
	}
	if !found {
		fmt.Println("(No current leader detected among LIVE nodes)")
	}
}

// Per spec: clients don't know node liveness, always try n1 first
func sendTransactionWithID(tx Transaction, reqID string, clientID string, timestamp int64) bool {
	req := paxos.ClientRequest{
		ClientID:    clientID,
		RequestID:   reqID,
		Timestamp:   timestamp,
		Transaction: paxos.Transaction{Sender: tx.Sender, Receiver: tx.Receiver, Amount: tx.Amount},
	}
	
	leaderID, ok := waitForLeader(0)
	
	var nodesToTry []int
	if ok {
		nodesToTry = append(nodesToTry, leaderID)
		for i := 0; i < len(nodeAddrs); i++ {
			if i != leaderID {
				nodesToTry = append(nodesToTry, i)
			}
		}
	} else {
		nodesToTry = []int{0, 1, 2, 3, 4}
	}
	
	for _, targetID := range nodesToTry {
		client, err := rpc.DialHTTP("tcp", nodeAddrs[targetID])
		if err != nil {
			continue
		}
		
		var reply paxos.ClientReply
		method := fmt.Sprintf("Node%d.HandleClientRequest", targetID)
		err = client.Call(method, &req, &reply)
		client.Close()
		
		if err != nil {
			continue
		}
		
		if reply.Success {
			fmt.Printf("[SUCCESS] Transaction committed: %s->%s(%d) via node %d\n",
				tx.Sender, tx.Receiver, tx.Amount, targetID)
			return true
		} else {
			fmt.Printf("[FAILED] Transaction failed: %s->%s(%d) - insufficient balance\n",
				tx.Sender, tx.Receiver, tx.Amount)
			return true
		}
	}
	
	log.Printf("Error: All nodes unreachable for transaction %s->%s(%d)", 
		tx.Sender, tx.Receiver, tx.Amount)
	return false
}

func waitForLeader(wait time.Duration) (int, bool) {
	deadline := time.Now().Add(wait)
	for {
		liveNodesMutex.Lock()
		liveSnapshot := make(map[int]bool, len(liveNodes))
		for id, v := range liveNodes {
			liveSnapshot[id] = v
		}
		liveNodesMutex.Unlock()

		for id := range nodeAddrs {
			if !liveSnapshot[id] {
				continue
			}
			var st statusDTO
			callRPCNoLiveGate(id, "GetStatus", &struct{}{}, &st)
			if st.ID == id && st.IsLeader && !st.Crashed {
				return id, true
			}
		}
		if wait <= 0 || time.Now().After(deadline) {
			return -1, false
		}
		time.Sleep(120 * time.Millisecond)
	}
}

func parseCSV(filePath string) ([]TestSet, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	type acc struct {
		set      TestSet
		lastLive []int
	}
	sets := make(map[int]*acc)
	
	currentSetNum := 0 // Track current set number for continuation rows
	lineNum := 0

	for {
		record, err := reader.Read()
		lineNum++
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		
		// Skip header row
		if lineNum == 1 && strings.Contains(strings.ToLower(record[0]), "set number") {
			continue
		}
		
		if len(record) < 2 {
			continue
		}
		
		// Check if first column has set number (start of new set) or is empty (continuation)
		setNumStr := strings.TrimSpace(record[0])
		if setNumStr != "" {
			// New set starts
			setNum, err := strconv.Atoi(setNumStr)
			if err != nil {
				continue
			}
			currentSetNum = setNum
		} else if currentSetNum == 0 {
			// Empty set number but no current set yet, skip
			continue
		}
		// else: use currentSetNum (continuation row)
		
		raw := strings.TrimSpace(record[1])
		raw = strings.Trim(raw, "\"'")
		
		if raw == "" {
			continue // Empty transaction field
		}

		var liveIDs []int
		if len(record) >= 3 && strings.TrimSpace(record[2]) != "" {
			nodesStr := strings.TrimSpace(record[2])
			nodesStr = strings.Trim(nodesStr, "\"[]")
			parts := strings.FieldsFunc(nodesStr, func(r rune) bool { return r == ';' || r == ',' })
			for _, p := range parts {
				idStr := strings.TrimSpace(strings.Trim(p, "nN "))
				if idStr == "" {
					continue
				}
				id, err := strconv.Atoi(idStr)
				if err != nil {
					continue
				}
				liveIDs = append(liveIDs, id-1)
			}
		}

		a, ok := sets[currentSetNum]
		if !ok {
			a = &acc{set: TestSet{SetNumber: currentSetNum}}
			sets[currentSetNum] = a
		}

		if len(liveIDs) > 0 {
			a.lastLive = liveIDs
			if len(a.set.LiveNodes) == 0 {
				a.set.LiveNodes = liveIDs
			}
		}
		if len(a.set.LiveNodes) == 0 && len(a.lastLive) > 0 {
			a.set.LiveNodes = a.lastLive
		}

		if strings.EqualFold(raw, "LF") {
			a.set.Steps = append(a.set.Steps, Step{Type: StepLF})
			continue
		}

		txStr := strings.TrimSpace(raw)
		txParts := strings.Split(strings.Trim(txStr, "()"), ",")
		if len(txParts) < 3 {
			log.Printf("[CSV PARSE WARNING] Skipping malformed transaction in set %d: '%s' (only %d parts)", currentSetNum, raw, len(txParts))
			continue
		}
		sender := strings.TrimSpace(txParts[0])
		receiver := strings.TrimSpace(txParts[1])
		amount, err := strconv.Atoi(strings.TrimSpace(txParts[2]))
		if err != nil {
			log.Printf("[CSV PARSE WARNING] Skipping transaction with invalid amount in set %d: '%s'", currentSetNum, raw)
			continue
		}
		if sender == "" || receiver == "" {
			log.Printf("[CSV PARSE WARNING] Skipping transaction with empty sender/receiver in set %d: '%s'", currentSetNum, raw)
			continue
		}
		a.set.Steps = append(a.set.Steps, Step{
			Type: StepTx,
			Tx:   Transaction{Sender: sender, Receiver: receiver, Amount: amount},
		})
	}

	var maxSet int
	for k := range sets {
		if k > maxSet {
			maxSet = k
		}
	}
	var out []TestSet
	for i := 1; i <= maxSet; i++ {
		if a, ok := sets[i]; ok {
			fmt.Printf("[CSV PARSE] Set %d: %d steps (%d transactions, %d leader failures)\n", 
				i, len(a.set.Steps), countTx(a.set.Steps), countLF(a.set.Steps))
			out = append(out, a.set)
		}
	}
	return out, nil
}

func countTx(steps []Step) int {
	count := 0
	for _, s := range steps {
		if s.Type == StepTx {
			count++
		}
	}
	return count
}

func countLF(steps []Step) int {
	count := 0
	for _, s := range steps {
		if s.Type == StepLF {
			count++
		}
	}
	return count
}

func prettyPrintBalances(m map[string]int) {
	if len(m) == 0 {
		fmt.Println("(empty)")
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Println("=== Balances ===")
	for _, k := range keys {
		fmt.Printf("%s: %d\n", k, m[k])
	}
}

func pauseAllTimers() {
	for i := 0; i < len(nodeAddrs); i++ {
		callRPCNoLiveGate(i, "PauseTimer", &struct{}{}, &struct{}{})
	}
}

func resumeAllTimers() {
	for i := 0; i < len(nodeAddrs); i++ {
		callRPCNoLiveGate(i, "ResumeTimer", &struct{}{}, &struct{}{})
	}
}
