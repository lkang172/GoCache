package main

import (
	pb "GoCache/raftpb"
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Command struct {
	Op    string
	Key   string
	Value []byte
	TTL   time.Duration
}

type Raft struct {
	pb.UnimplementedRaftServer

	mu    sync.Mutex
	id    int
	peers map[int]pb.RaftClient

	role     int
	currTerm int
	votedFor int

	cache    *LRUCache
	log      []*pb.LogEntry // 1-indexed: log[0] is a dummy entry
	applyCh  chan struct{}
	commitCh chan chan error // pending client writes waiting for commit

	commitIndex int
	lastApplied int

	nextIndex  map[int]int
	matchIndex map[int]int

	lastHeard time.Time
}

func (r *Raft) lastLogIndex() int {
	return len(r.log) - 1
}

func (r *Raft) lastLogTerm() int {
	return int(r.log[r.lastLogIndex()].Term)
}

func (r *Raft) ticker() {
	for {
		time.Sleep(30 * time.Millisecond)

		r.mu.Lock()

		role := r.role
		lastHeard := r.lastHeard

		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond

		if role == 0 {
			r.mu.Unlock()
			r.sendAppendEntries()
			time.Sleep(50 * time.Millisecond)
		} else {
			elapsed := time.Since(lastHeard) > timeout
			r.mu.Unlock()
			if elapsed {
				r.callElection()
			}
		}
	}
}

func (r *Raft) connectToPeers(peerAddrs map[int]string) {
	for id, addr := range peerAddrs {
		if id == r.id {
			continue
		}

		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to connect to peer %d at %s: %v", id, addr, err)
			continue
		}

		r.peers[id] = pb.NewRaftClient(conn)
	}
}

func (r *Raft) sendRequestVote(peerID int, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	client, ok := r.peers[peerID]
	if !ok {
		return nil, fmt.Errorf("peer %d not found", peerID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	return client.RequestVote(ctx, req)
}

func (r *Raft) callElection() {
	r.mu.Lock()

	r.role = 1
	r.currTerm += 1
	r.votedFor = r.id

	voteCounter := 1
	term := r.currTerm
	lastLogIndex := r.lastLogIndex()
	lastLogTerm := r.lastLogTerm()

	log.Printf("Node %d starting election for term %d", r.id, r.currTerm)

	r.mu.Unlock()

	for i := range r.peers {
		if i == r.id {
			continue
		}

		go func(peerID int) {
			req := &pb.RequestVoteRequest{
				Term:         int32(term),
				CandidateId:  int32(r.id),
				LastLogIndex: int32(lastLogIndex),
				LastLogTerm:  int32(lastLogTerm),
			}

			res, err := r.sendRequestVote(peerID, req)
			if err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.role != 1 {
				return
			}

			if res.VoteGranted {
				voteCounter++
				log.Printf("Node %d got vote from peer %d (%d/%d votes)", r.id, peerID, voteCounter, len(r.peers)+1)

				if voteCounter > (len(r.peers)+1)/2 {
					r.becomeLeader()
				}
			} else if int(res.Term) > r.currTerm {
				r.currTerm = int(res.Term)
				r.role = 2
				r.votedFor = -1
			}
		}(i)
	}
}

func (r *Raft) becomeLeader() {
	r.role = 0

	nextIdx := r.lastLogIndex() + 1
	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)

	for peerID := range r.peers {
		r.nextIndex[peerID] = nextIdx
		r.matchIndex[peerID] = 0
	}

	log.Printf("Node %d became leader for term %d", r.id, r.currTerm)
}

// appendEntry appends a command to the leader's log and returns a channel
// that will receive nil on successful commit or an error on failure.
func (r *Raft) appendEntry(cmd Command) chan error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != 0 {
		ch := make(chan error, 1)
		ch <- fmt.Errorf("not the leader")
		return ch
	}

	entry := &pb.LogEntry{
		Term:       int32(r.currTerm),
		Index:      int32(r.lastLogIndex() + 1),
		Op:         cmd.Op,
		Key:        cmd.Key,
		Value:      cmd.Value,
		TtlSeconds: int64(cmd.TTL / time.Second),
	}

	r.log = append(r.log, entry)

	ch := make(chan error, 1)
	r.commitCh <- ch

	return ch
}

func (r *Raft) sendAppendEntries() {
	r.mu.Lock()
	term := r.currTerm
	id := r.id
	commitIndex := r.commitIndex
	r.mu.Unlock()

	for peerID, client := range r.peers {
		go func(peerID int, client pb.RaftClient) {
			r.mu.Lock()
			if r.role != 0 {
				r.mu.Unlock()
				return
			}

			nextIdx := r.nextIndex[peerID]
			prevLogIndex := nextIdx - 1
			prevLogTerm := int(r.log[prevLogIndex].Term)

			var entries []*pb.LogEntry
			if nextIdx <= r.lastLogIndex() {
				entries = r.log[nextIdx:]
			}
			r.mu.Unlock()

			if len(entries) > 0 {
				log.Printf("Node %d replicating %d entries to peer %d", id, len(entries), peerID)
			} else {
				log.Printf("Node %d heartbeat to peer %d (term %d)", id, peerID, term)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			res, err := client.AppendEntries(ctx, &pb.AppendEntriesRequest{
				Term:         int32(term),
				LeaderId:     int32(id),
				PrevLogIndex: int32(prevLogIndex),
				PrevLogTerm:  int32(prevLogTerm),
				Entries:      entries,
				LeaderCommit: int32(commitIndex),
			})
			if err != nil {
				log.Printf("Node %d AppendEntries to peer %d failed: %v", id, peerID, err)
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if r.role != 0 {
				return
			}

			if int(res.Term) > r.currTerm {
				r.currTerm = int(res.Term)
				r.role = 2
				r.votedFor = -1
				log.Printf("Node %d stepping down, discovered higher term %d", id, res.Term)
				return
			}

			if res.Success {
				newMatchIndex := prevLogIndex + len(entries)
				if newMatchIndex > r.matchIndex[peerID] {
					r.matchIndex[peerID] = newMatchIndex
					r.nextIndex[peerID] = newMatchIndex + 1
				}

				r.advanceCommitIndex()
			} else {
				log.Printf("Node %d AppendEntries rejected by peer %d, backing up nextIndex", id, peerID)
				if r.nextIndex[peerID] > 1 {
					r.nextIndex[peerID]--
				}
			}
		}(peerID, client)
	}
}

// advanceCommitIndex checks if there's an N > commitIndex where a majority
// has matchIndex >= N and log[N].term == currentTerm. Must be called with mu held.
func (r *Raft) advanceCommitIndex() {
	for n := r.commitIndex + 1; n <= r.lastLogIndex(); n++ {
		if int(r.log[n].Term) != r.currTerm {
			continue
		}

		replicatedCount := 1 // count self
		for _, matchIdx := range r.matchIndex {
			if matchIdx >= n {
				replicatedCount++
			}
		}

		if replicatedCount > (len(r.peers)+1)/2 {
			r.commitIndex = n
		}
	}

	// Signal the apply goroutine
	select {
	case r.applyCh <- struct{}{}:
	default:
	}
}

// applyCommitted runs in its own goroutine, applying committed log entries to the cache.
func (r *Raft) applyCommitted() {
	for range r.applyCh {
		r.mu.Lock()
		commitIndex := r.commitIndex
		lastApplied := r.lastApplied

		var toApply []*pb.LogEntry
		for i := lastApplied + 1; i <= commitIndex; i++ {
			toApply = append(toApply, r.log[i])
		}
		r.lastApplied = commitIndex
		r.mu.Unlock()

		for _, entry := range toApply {
			var ttl time.Duration
			if entry.TtlSeconds > 0 {
				ttl = time.Duration(entry.TtlSeconds) * time.Second
			}

			switch entry.Op {
			case "put":
				r.cache.Put(entry.Key, entry.Value, ttl)
				log.Printf("Node %d applied put %s (index %d)", r.id, entry.Key, entry.Index)
			case "delete":
				r.cache.Delete(entry.Key)
				log.Printf("Node %d applied delete %s (index %d)", r.id, entry.Key, entry.Index)
			}
		}

		// Notify waiting clients on the leader
		r.mu.Lock()
		for range toApply {
			select {
			case ch := <-r.commitCh:
				ch <- nil
			default:
			}
		}
		r.mu.Unlock()
	}
}

func (r *Raft) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	res := &pb.AppendEntriesResponse{
		Term:    int32(r.currTerm),
		Success: false,
	}

	if int(req.Term) < r.currTerm {
		return res, nil
	}

	r.lastHeard = time.Now()

	if int(req.Term) > r.currTerm {
		r.currTerm = int(req.Term)
		r.votedFor = -1
	}

	r.role = 2

	// Log consistency check
	if int(req.PrevLogIndex) > r.lastLogIndex() {
		return res, nil
	}

	if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		// Delete conflicting entry and everything after it
		r.log = r.log[:req.PrevLogIndex]
		return res, nil
	}

	// Append new entries, overwriting conflicts
	for i, entry := range req.Entries {
		idx := int(req.PrevLogIndex) + 1 + i
		if idx <= r.lastLogIndex() {
			if r.log[idx].Term != entry.Term {
				r.log = r.log[:idx]
				r.log = append(r.log, entry)
			}
		} else {
			r.log = append(r.log, entry)
		}
	}

	// Advance commit index
	if int(req.LeaderCommit) > r.commitIndex {
		lastNewIdx := int(req.PrevLogIndex) + len(req.Entries)
		if int(req.LeaderCommit) < lastNewIdx {
			r.commitIndex = int(req.LeaderCommit)
		} else {
			r.commitIndex = lastNewIdx
		}

		// Signal the apply goroutine
		select {
		case r.applyCh <- struct{}{}:
		default:
		}
	}

	res.Success = true
	res.Term = int32(r.currTerm)
	return res, nil
}

func (r *Raft) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	res := &pb.RequestVoteResponse{
		VoteGranted: false,
		Term:        int32(r.currTerm),
	}

	if req.Term < int32(r.currTerm) {
		return res, nil
	}

	if req.Term > int32(r.currTerm) {
		r.currTerm = int(req.Term)
		r.role = 2
		r.votedFor = -1
		res.Term = int32(r.currTerm)
	}

	// Log up-to-date check: candidate's log must be at least as up-to-date as ours
	myLastTerm := r.lastLogTerm()
	myLastIndex := r.lastLogIndex()
	candidateUpToDate := int(req.LastLogTerm) > myLastTerm ||
		(int(req.LastLogTerm) == myLastTerm && int(req.LastLogIndex) >= myLastIndex)

	if (r.votedFor == -1 || r.votedFor == int(req.CandidateId)) && candidateUpToDate {
		r.votedFor = int(req.CandidateId)
		res.VoteGranted = true
		r.lastHeard = time.Now()
		log.Printf("Node %d voted for %d in term %d", r.id, req.CandidateId, req.Term)
	}

	return res, nil
}
