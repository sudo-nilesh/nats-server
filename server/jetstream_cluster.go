// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"time"
)

// jetStreamCluster holds information about the meta group and stream assignments.
type jetStreamCluster struct {
	// The metacontroller raftNode.
	meta RaftNode
	// Groups that this server is a direct member.
	nodes map[string]RaftNode
	// For raft groups assigmemnts. All servers will have this be the same.
	groups map[string]*raftGroup
	// For stream assignments. All servers will have this be the same.
	// ACC -> STREAM -> Assignment.
	streams map[string]map[string]*streamAssignment
}

// Define types of the entry.
type metaOp uint8

const (
	raftGroupOp metaOp = iota
	assignStreamOp
)

// raftGroup are controlled by the metagroup controller. The raftGroups will
// house streams and consumers.
type raftGroup struct {
	Name    string      `json:"name"`
	Peers   []string    `json:"peers"`
	Storage StorageType `json:"store"`
}

// streamAssignment is what the meta controller uses to assign streams to core groups.
type streamAssignment struct {
	Client *ClientInfo   `json:"client,omitempty"`
	Config *StreamConfig `json:"stream"`
	Group  string        `json:"raft_group"`
	Reply  string        `json:"reply"`
}

const (
	defaultStoreDirName  = "_js_"
	defaultMetaGroupName = "JMETA"
	defaultMetaFSBlkSize = 64 * 1024
)

// For validating clusters.
func validateJetStreamOptions(o *Options) error {
	// If not clustered no checks.
	if !o.JetStream || o.Cluster.Port == 0 {
		return nil
	}
	if o.ServerName == _EMPTY_ {
		return fmt.Errorf("jetstream cluster requires `server_name` to be set")
	}
	if o.Cluster.Name == _EMPTY_ {
		return fmt.Errorf("jetstream cluster requires `cluster_name` to be set")
	}

	fmt.Printf("o is %+v\n", o)
	return nil
}

func (s *Server) getJetStreamCluster() (*jetStream, *jetStreamCluster) {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	if js == nil {
		return nil, nil
	}
	js.mu.RLock()
	cc := js.cluster
	js.mu.RUnlock()
	if cc == nil {
		return nil, nil
	}
	return js, cc
}

func (s *Server) JetStreamIsClustered() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	js.mu.RLock()
	isClustered := js.cluster != nil
	js.mu.RUnlock()
	return isClustered
}

func (s *Server) JetStreamIsLeader() bool {
	js := s.getJetStream()
	if js == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isLeader()
}

// Read lock should be held.
func (cc *jetStreamCluster) isLeader() bool {
	if cc == nil {
		// Non-clustered mode
		return true
	}
	// FIXME(dlc) - Make this real and bootstrap manually with peers.
	lg := len(cc.groups)
	return cc.meta.Leader() && lg > 1
}

func (a *Account) getJetStreamFromAccount() (*Server, *jetStream, *jsAccount) {
	a.mu.RLock()
	jsa := a.js
	a.mu.RUnlock()
	if jsa == nil {
		return nil, nil, nil
	}
	jsa.mu.RLock()
	js := jsa.js
	jsa.mu.RUnlock()
	if js == nil {
		return nil, nil, nil
	}
	js.mu.RLock()
	s := js.srv
	js.mu.RUnlock()
	return s, js, jsa
}

// jetStreamReadAllowedForStream will check if we can report on any information
// regarding this stream like info, etc.
// TODO(dlc) - make sure we are up tpo date.
func (a *Account) jetStreamReadAllowedForStream(stream string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	return js.cluster.isStreamAssigned(a, stream)
}

func (a *Account) jetStreamIsStreamLeader(stream string) bool {
	s, js, jsa := a.getJetStreamFromAccount()
	if s == nil || js == nil || jsa == nil {
		return false
	}
	js.mu.RLock()
	defer js.mu.RUnlock()
	// FIXME(dlc) - Do groups here too.
	return js.cluster.isStreamAssigned(a, stream)
}

func (s *Server) jetStreamReadAllowed() bool {
	// FIXME(dlc) - Add in read allowed mode for readonly API.
	return s.JetStreamIsLeader()
}

func (s *Server) enableJetStreamClustering() error {
	if !s.isRunning() {
		return nil
	}
	js := s.getJetStream()
	if js == nil {
		return ErrJetStreamNotEnabled
	}
	// Already set.
	if js.cluster != nil {
		return nil
	}

	s.Noticef("Starting JetStream cluster")
	// We need to determine if we have a stable cluster name and expected number of servers.
	s.Debugf("JetStream cluster checking for stable cluster name and peers")
	if s.isClusterNameDynamic() || s.configuredRoutes() == 0 {
		return errors.New("JetStream cluster requires cluster name and explicit routes")
	}

	return js.setupMetaGroup()
}

func (js *jetStream) setupMetaGroup() error {
	s := js.srv
	fmt.Printf("creating metagroup!\n")
	fmt.Printf("cluster name is stable, numConfiguredRoutes is %d\n", s.configuredRoutes())

	// Setup our WAL for the metagroup.
	stateDir := path.Join(js.config.StoreDir, s.SystemAccount().Name, defaultStoreDirName, defaultMetaGroupName)
	fs, bootstrap, err := newFileStore(
		FileStoreConfig{StoreDir: stateDir, BlockSize: defaultMetaFSBlkSize},
		StreamConfig{Name: defaultMetaGroupName, Storage: FileStorage},
	)
	if err != nil {
		fmt.Printf("got err! %v\n", err)
		return err
	}

	if bootstrap {
		s.Noticef("JetStream cluster bootstrapping")
		fmt.Printf("Bootstrapping: active routes is %d\n", len(s.routes))
		// FIXME(dlc) - Make this real.
	}

	n, err := s.newRaftGroup(defaultMetaGroupName, stateDir, fs)
	if err != nil {
		return err
	}

	js.mu.Lock()
	defer js.mu.Unlock()
	js.cluster = &jetStreamCluster{
		meta:    n,
		nodes:   make(map[string]RaftNode),
		groups:  make(map[string]*raftGroup),
		streams: make(map[string]map[string]*streamAssignment),
	}

	js.srv.startGoRoutine(js.monitorCluster)
	return nil
}

func (js *jetStream) getMetaGroup() RaftNode {
	js.mu.RLock()
	defer js.mu.RUnlock()
	if js.cluster == nil {
		return nil
	}
	return js.cluster.meta
}

func (js *jetStream) server() *Server {
	js.mu.RLock()
	s := js.srv
	js.mu.RUnlock()
	return s
}

// streamAssigned informs us if this server has this stream assigned.
func (jsa *jsAccount) streamAssigned(stream string) bool {
	jsa.mu.RLock()
	defer jsa.mu.RUnlock()

	js, acc := jsa.js, jsa.account
	if js == nil {
		return false
	}
	return js.cluster.isStreamAssigned(acc, stream)
}

// Read lock should be held.
func (cc *jetStreamCluster) isStreamAssigned(a *Account, stream string) bool {
	// Non-clustered mode always return true.
	if cc == nil {
		return true
	}
	fmt.Printf("[%s] - Checking cc.streams of %+v\n", a.srv.Name(), cc.streams)
	as := cc.streams[a.Name]
	if as == nil {
		return false
	}
	sa := as[stream]
	if sa == nil {
		return false
	}
	fmt.Printf("sa is %+v\n", sa)
	rg := cc.groups[sa.Group]
	fmt.Printf("rg is %+v\n", rg)
	if rg == nil {
		return false
	}
	// Check if we are the leader of this raftGroup assigned to the stream.
	ourID := cc.meta.ID()
	for _, peer := range rg.Peers {
		// FIXME(dlc) - Need to be leader..
		if peer == ourID {
			if len(rg.Peers) == 1 {
				return true
			}
			if cc.nodes[sa.Group].Leader() {
				return true
			}
		}
	}
	return false
}

func (js *jetStream) monitorCluster() {
	fmt.Printf("[%s] Starting monitor cluster routine\n", js.srv.Name())
	defer fmt.Printf("[%s] Exiting monitor cluster routine\n", js.srv.Name())

	s, n := js.server(), js.getMetaGroup()
	qch, lch, ach := n.QuitC(), n.LeadChangeC(), n.ApplyC()

	defer s.grWG.Done()

	for {
		select {
		case <-s.quitCh:
			return
		case <-qch:
			return
		case ce := <-ach:
			js.applyEntries(ce.Entries)
			n.Applied(ce.Index)
		case isLeader := <-lch:
			js.processLeaderChange(isLeader)
		}
	}
}

func (js *jetStream) applyEntries(entries [][]byte) {
	fmt.Printf("[%s] JS HAS AN ENTRIES UPDATE TO APPLY!\n", js.srv.Name())

	for _, buf := range entries {
		switch metaOp(buf[0]) {
		case raftGroupOp:
			fmt.Printf("[%s] RAFT GROUP ENTRY\n", js.srv.Name())
			rg, err := decodeRaftGroup(buf[1:])
			if err != nil {
				js.srv.Errorf("JetStream cluster failed to decode raft group request: %q", buf[1:])
				return
			}
			js.processRaftGroup(rg)
		case assignStreamOp:
			fmt.Printf("[%s] STREAM ASSIGN ENTRY\n", js.srv.Name())
			sa, err := decodeStreamAssignment(buf[1:])
			if err != nil {
				js.srv.Errorf("JetStream cluster failed to decode add stream request: %q", buf[1:])
				return
			}
			js.processAssignStream(sa)
		default:
			panic("Unknow entry type!")
		}
	}
}

// Lock should be held.
func (js *jetStream) nodeID() string {
	if js.cluster == nil {
		return _EMPTY_
	}
	return js.cluster.meta.ID()
}

func (rg *raftGroup) isMember(id string) bool {
	if rg == nil {
		return false
	}
	for _, peer := range rg.Peers {
		if peer == id {
			return true
		}
	}
	return false
}

// processRaftGroup is called when followers have replicated a raft group assignment.
func (js *jetStream) processRaftGroup(rg *raftGroup) {
	js.mu.Lock()
	defer js.mu.Unlock()

	s, cc := js.srv, js.cluster
	if rg == nil {
		s.Warnf("JetStream cluster meta received empty raft group request")
		return
	}
	// We already have this assigned.
	if cc.groups[rg.Name] != nil {
		s.Debugf("JetStream cluster already has raft group %q assigned", rg.Name)
	} else {
		s.Debugf("JetStream cluster assigning raft group:%+v", rg)
		fmt.Printf("[%s:%s]\tJetStream cluster assigning raft group:%+v\n", s.Name(), js.nodeID(), rg)
		cc.groups[rg.Name] = rg
	}

	// Check if this is a multi-node raftGroup and if so create the group.
	if len(rg.Peers) > 1 && rg.isMember(cc.meta.ID()) {
		sysAcc := s.SystemAccount()
		if sysAcc == nil {
			s.Debugf("JetStream cluster detected shutdown processing raft group:%+v", rg)
			fmt.Printf("[%s] JetStream cluster detected shutdown processing raft group:%+v\n", s.Name(), rg)
			return
		}
		stateDir := path.Join(js.config.StoreDir, sysAcc.Name, defaultStoreDirName, rg.Name)
		fs, _, err := newFileStore(
			FileStoreConfig{StoreDir: stateDir},
			StreamConfig{Name: rg.Name, Storage: rg.Storage},
		)
		if err != nil {
			fmt.Printf("got err! %v\n", err)
			return
		}
		fmt.Printf("[%s] Will create raft group %q for %q\n", s.Name(), rg.Name, stateDir)
		n, err := s.newRaftGroup(rg.Name, stateDir, fs)
		if err != nil {
			fmt.Printf("ERROR CREATING RAFT GROUP!!!%v\n", err)
		}
		fmt.Printf("[%s] Created group %q\n", s.Name(), rg.Name)
		// Place in our nodes map.
		cc.nodes[rg.Name] = n
	}
}

// processAssignStream is called when followers have replicated an assignment.
func (js *jetStream) processAssignStream(sa *streamAssignment) {
	fmt.Printf("[%s] Got a stream request %+v\n", js.srv.Name(), sa)
	fmt.Printf("[%s] Assigned is %+v\n", js.srv.Name(), sa.Group)

	js.mu.Lock()
	s, cc := js.srv, js.cluster
	js.mu.Unlock()
	if s == nil || cc == nil {
		// TODO(dlc) - debug at least
		return
	}

	acc, err := s.LookupAccount(sa.Client.Account)
	if err != nil {
		// TODO(dlc) - log error
		return
	}
	stream := sa.Config.Name

	js.mu.Lock()
	// Check if we already have this assigned.
	accStreams := cc.streams[acc.Name]
	if accStreams != nil && accStreams[stream] != nil {
		// TODO(dlc) - Debug?
		js.mu.Unlock()
		return
	}
	if accStreams == nil {
		accStreams = make(map[string]*streamAssignment)
	}
	// Update our state.
	accStreams[stream] = sa
	cc.streams[acc.Name] = accStreams

	fmt.Printf("Assigned %+v\n", cc.streams)
	js.mu.Unlock()

	// Check if this is for us.
	if cc.isStreamAssigned(acc, stream) {
		fmt.Printf("[%s] Assigned to US!\n", js.srv.Name())
		fmt.Printf("Assigning stream to account %q\n", acc.Name)
		fmt.Printf("sa.Config is %+v\n", sa.Config)
		var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}
		mset, err := acc.AddStream(sa.Config)
		if err != nil {
			// TODO(dlc) - error response.
			fmt.Printf("Got an error!! %v\n\n", err)
			resp.Error = jsError(err)
			s.sendAPIResponse(sa.Client, acc, sa.Reply, _EMPTY_, _EMPTY_, s.jsonResponse(&resp))
		} else {
			fmt.Printf("\n\n[%s] - Successfully created our stream!!! %+v\n\n", js.srv.Name(), mset)
			fmt.Printf("s is %q, acc is %q\n", s.Name(), acc.Name)
			resp.StreamInfo = &StreamInfo{Created: mset.Created(), State: mset.State(), Config: mset.Config()}
			s.sendAPIResponse(sa.Client, acc, _EMPTY_, sa.Reply, _EMPTY_, s.jsonResponse(&resp))
		}
	}
}

func (js *jetStream) processLeaderChange(isLeader bool) {
	fmt.Printf("[%s] JS detected leadership change! %v\n", js.srv.Name(), isLeader)

	js.mu.Lock()
	defer js.mu.Unlock()

	fmt.Printf("[%s] Processing leader change!!\n\n", js.srv.Name())

	if !isLeader {
		// TODO(dlc) - stepdown.
		return
	}

	// FIXME(dlc) - Restored state.
	go js.assignRaftGroups()
}

func (js *jetStream) assignRaftGroups() {
	s, cc := js.srv, js.cluster
	js.mu.Lock()
	defer js.mu.Unlock()

	// Bail if no longer leader.
	if !cc.meta.Leader() {
		return
	}

	// TODO(dlc) - Right now assume all have JS, but that will not be the case longer term.
	clusterSize := s.configuredRoutes()
	peers := cc.meta.Peers()
	// Add in ourselves.
	peers = append(peers, &Peer{ID: cc.meta.ID()})

	// FIXME(dlc) - This is a hack for now, make bootstrap real.
	if len(peers) != clusterSize {
		s.Debugf("JetStream cluster waiting on peers")
		fmt.Printf("JetStream cluster waiting on peers, have %d, need %d\n", len(peers), clusterSize)
		time.AfterFunc(250*time.Millisecond, js.assignRaftGroups)
		return
	}

	// For each peer make sure we have a corresponding group of size 1 for each.
	for _, p := range peers {
		if cc.groups[p.ID] == nil {
			rg := &raftGroup{Name: p.ID, Storage: AnyStorage, Peers: []string{p.ID}}
			fmt.Printf("Proposing raftGroup %+v\n", rg)
			cc.meta.Propose(encodeAddRaftGroup(rg))
		}
	}

	// Now do some of the other multi-replica groups that will be real.
	// TODO(dlc)

	fmt.Printf("ClusterSize is %d\n", clusterSize)
	fmt.Printf("Peers is %d\n", len(peers))

	// Create a 3 node for now.
	if len(peers) < 3 {
		panic("Fixed at 3, need at least 3 peers")
	}

	var peerIds []string
	for i := 0; i < 3; i++ {
		peerIds = append(peerIds, peers[i].ID)
	}
	rg := &raftGroup{Name: "R3F-1", Storage: FileStorage, Peers: peerIds}
	fmt.Printf("Proposing raftGroup %+v\n", rg)
	cc.meta.Propose(encodeAddRaftGroup(rg))
}

// selectGroupForStream will select a group for assignment for the stream.
// Lock should be held.
func (cc *jetStreamCluster) selectGroupForStream(cfg *StreamConfig) *raftGroup {
	// TODO(dlc) - This of course can be way smarter.
	var rgs []*raftGroup
	replicas := cfg.Replicas
	if replicas == 0 {
		replicas = 1
	}
	for _, rg := range cc.groups {
		// Make sure we match on replica size and storage type.
		if len(rg.Peers) != replicas || (rg.Storage != cfg.Storage && rg.Storage != AnyStorage) {
			fmt.Printf("Skipping rg %+v\n", rg)
			fmt.Printf("len(peers) is %d\n", len(rg.Peers))
			fmt.Printf("stream config is %+v\n", cfg)
			continue
		}
		rgs = append(rgs, rg)
	}
	if len(rgs) == 0 {
		return nil
	}
	// We can be alot smarter here, for now do random placement.
	return rgs[rand.Int31n(int32(len(rgs)))]
}

func (s *Server) jsClusteredStreamRequest(ci *ClientInfo, subject, reply string, rmsg []byte, cfg *StreamConfig) {
	fmt.Printf("[%s:%s]\tWill answer stream create!\n", s.Name(), s.js.nodeID())
	js, cc := s.getJetStreamCluster()
	if js == nil || cc == nil {
		return
	}

	fmt.Printf("[%s:%s]\tSelect group from total of %d!\n", s.Name(), s.js.nodeID(), len(cc.groups))

	js.mu.Lock()
	defer js.mu.Unlock()

	rg := cc.selectGroupForStream(cfg)
	if rg == nil {
		fmt.Printf("[%s:%s]\tNo group selected!\n", s.Name(), s.js.nodeID())
		var resp = JSApiStreamCreateResponse{ApiResponse: ApiResponse{Type: JSApiStreamCreateResponseType}}
		resp.Error = jsInsufficientErr
		acc, _ := s.LookupAccount(ci.Account)
		s.sendAPIResponse(ci, acc, subject, reply, string(rmsg), s.jsonResponse(&resp))
		return
	}
	sa := &streamAssignment{Group: rg.Name, Config: cfg, Reply: reply, Client: ci}
	cc.meta.Propose(encodeAddStreamAssignment(sa))
}

func encodeAddStreamAssignment(sa *streamAssignment) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(assignStreamOp))
	json.NewEncoder(&bb).Encode(sa)
	return bb.Bytes()
}

func decodeStreamAssignment(buf []byte) (*streamAssignment, error) {
	var sr streamAssignment
	err := json.Unmarshal(buf, &sr)
	return &sr, err
}

func encodeAddRaftGroup(cg *raftGroup) []byte {
	var bb bytes.Buffer
	bb.WriteByte(byte(raftGroupOp))
	json.NewEncoder(&bb).Encode(cg)
	return bb.Bytes()
}

func decodeRaftGroup(buf []byte) (*raftGroup, error) {
	var rg raftGroup
	err := json.Unmarshal(buf, &rg)
	return &rg, err
}
