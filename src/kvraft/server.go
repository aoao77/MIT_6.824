package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(topic string, format string, a ...interface{}) (n int, err error) {
	raft.DebugPrint(raft.LogTopic(topic), format, a...)
	return
}

type Op struct {
	Op       string
	Key      string
	Value    string
	ClientId int64
	Seq      int
}

type pair struct {
	key   string
	value string
}

type tableItem struct {
	id  int64
	res result
}

type result struct {
	Seq     int
	Value   string
	Success bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database      map[string]string
	request_table map[int64]*result
	requestCh     map[int64]chan bool
	requestSeq    map[int64]int
	rfTerm        int
	// lastApplied   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	//initial
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = ""
	reply.Err = E_noError
	//idx verify
	val, ok := kv.request_table[args.ClientId]
	if ok {
		if args.Seq < val.Seq {
			reply.Err = E_seqTimeout
			DPrintf("SVRO", "S%d Error:%v", kv.me, reply.Err)
			return
		} else if args.Seq == val.Seq {
			if val.Success {
				reply.Value = kv.database[args.Key]
				reply.Err = E_noError
				DPrintf("SVRO", "S%d Error:%v", kv.me, reply.Err)
				return
			}
			// 	} else if args.Seq != val.Seq+1 {
			// 		reply.Err = E_seqNoLinear
			// 		DPrintf("SVRO", "S%d Error:%v argsSeq:%d valSeq:%d", kv.me, reply.Err, args.Seq, val.Seq)
			// 		return
			// 	}
			// } else {
			// 	if args.Seq != 1 {
			// 		reply.Err = E_seqNoLinear
			// 		DPrintf("SVRO", "S%d Error:%v argsSeq:%d ", kv.me, reply.Err, args.Seq)
			// 		return
			// 	} else {
		}
	}
	//start
	op := Op{op_get, args.Key, "", args.ClientId, args.Seq}
	_, inTerm, isleader := kv.rf.Start(op)
	//call
	if isleader {
		kv.rfTerm = inTerm
		//ch create
		kv.requestSeq[args.ClientId] = args.Seq
		kv.requestCh[args.ClientId] = make(chan bool, 1)
		ch := kv.requestCh[args.ClientId]
		kv.mu.Unlock()
		//return
	LOOP:
		for {
			select {
			case <-ch:
				DPrintf("LOCK", "S%d read ch id:%d", kv.me, args.ClientId)
				break LOOP
			// case <-time.After(10 * 100 * time.Millisecond):
			// reply.Err = E_execTimeout
			default:
				kv.mu.Lock()
				err, leaderState := kv.getLeaderState()
				kv.mu.Unlock()
				if !leaderState {
					reply.Err = err
					break LOOP
				} else {
					DPrintf("SVRO", "S%d sleep", kv.me)
					time.Sleep(20 * time.Millisecond)
				}
			}
		}

		kv.mu.Lock()
		//ch has read
		if reply.Err != E_noError {
			//error occur
		} else {
			//table seq turn to args seq
			if kv.request_table[args.ClientId].Success {
				reply.Value = kv.request_table[args.ClientId].Value
				DPrintf("SERV", "S%d Get finished Key:%v Val:%v at id:%d seq:%d", kv.me, args.Key, reply.Value, args.ClientId, args.Seq)
			} else {
				reply.Err = E_seqTimeout
			}
		}
		//close ch
		close(ch)
		kv.requestCh[args.ClientId] = nil
		kv.requestSeq[args.ClientId] = 0
	} else {
		reply.Err = E_noLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//initial
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = E_noError
	op := Op{}
	if args.Op == "Put" {
		op = Op{op_put, args.Key, args.Value, args.ClientId, args.Seq}
	} else if args.Op == "Append" {
		op = Op{op_append, args.Key, args.Value, args.ClientId, args.Seq}
	} else {
		reply.Err = E_noOp
		return
	}
	//idx verify
	val, ok := kv.request_table[args.ClientId]
	if ok {
		if args.Seq < val.Seq {
			reply.Err = E_seqTimeout
			DPrintf("SVRO", "S%d Error:%v", kv.me, reply.Err)
			return
		} else if args.Seq == val.Seq {
			if val.Success {
				reply.Err = E_noError
				DPrintf("SVRO", "S%d Error:%v", kv.me, reply.Err)
				return
			}
			// } else if args.Seq != val.Seq+1 {
			// 	reply.Err = E_seqNoLinear
			// 	DPrintf("SVRO", "S%d Error:%v argsSeq:%d valSeq:%d", kv.me, reply.Err, args.Seq, val.Seq)
			// 	return
			// }
			// } else {
			// 	if args.Seq != 1 {
			// 		reply.Err = E_seqNoLinear
			// 		DPrintf("SVRO", "S%d Error:%v argsSeq:%d ", kv.me, reply.Err, args.Seq)
			// 		return
			// 	} else {
		}
	}
	//start
	_, inTerm, isleader := kv.rf.Start(op)
	//call
	if isleader {
		kv.rfTerm = inTerm
		//ch create
		kv.requestSeq[args.ClientId] = args.Seq
		kv.requestCh[args.ClientId] = make(chan bool, 1)
		ch := kv.requestCh[args.ClientId]
		kv.mu.Unlock()
		//return
	LOOP:
		for {
			select {
			case <-ch:
				DPrintf("LOCK", "S%d read ch id:%d", kv.me, args.ClientId)
				break LOOP
			// case <-time.After(10 * 100 * time.Millisecond):
			// reply.Err = E_execTimeout
			default:
				kv.mu.Lock()
				err, leaderState := kv.getLeaderState()
				kv.mu.Unlock()
				if !leaderState {
					reply.Err = err
					break LOOP
				} else {
					DPrintf("SVRO", "S%d sleep", kv.me)
					time.Sleep(20 * time.Millisecond)
				}
			}
		}

		kv.mu.Lock()
		//ch has read
		if reply.Err != E_noError {
			//error occur
		} else {
			//table seq turn to args seq
			if kv.request_table[args.ClientId].Success {
				DPrintf("SERV", "S%d PutAppend finished Key:%v Val:%v at %d,%d", kv.me, args.Key, args.Value, args.ClientId, args.Seq)
			} else {
				reply.Err = E_seqTimeout
			}
		}
		//close ch
		close(ch)
		kv.requestCh[args.ClientId] = nil
		kv.requestSeq[args.ClientId] = 0
	} else {
		reply.Err = E_noLeader
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) get(key string) string {
	v, ok := kv.database[key]
	if ok {
		return v
	} else {
		return ""
	}
}

func (kv *KVServer) append(key string, Value string) {
	_, ok := kv.database[key]
	if ok {
		kv.database[key] += Value
	} else {
		kv.put(key, Value)
	}
}

func (kv *KVServer) put(key string, Value string) {
	kv.database[key] = Value
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unLock() {
	kv.mu.Unlock()
}

func (kv *KVServer) getLeaderState() (Err, bool) {
	err := E_noError
	inTerm, leaderState := kv.rf.GetState()
	if !leaderState {
		err = E_noLeader
		DPrintf("SVRO", "S%d no leader", kv.me)
	} else {
		if inTerm == kv.rfTerm {

		} else {
			leaderState = false
			err = E_execTimeout
			DPrintf("SVRO", "S%d leader timeout", kv.me)
		}
	}
	return err, leaderState
}

func (kv *KVServer) ticker() {
	for m := range kv.applyCh {
		if m.SnapshotValid {
			//snapshot
		} else if m.CommandValid {
			//update applied index
			// kv.lastApplied = m.CommandIndex
			//parse command
			p, ok := (m.Command).(Op)
			DPrintf("LOG1", "S%d apply cmd:%v", kv.me, p)
			if ok {
				kv.mu.Lock()
				//command execute
				request, ok := kv.request_table[p.ClientId]
				// not exist then create
				if !ok {
					kv.request_table[p.ClientId] = &result{Seq: 0, Value: "", Success: true}
					//request reassign
					request = kv.request_table[p.ClientId]
					DPrintf("LOG1", "S%d Id :%v  create :%v cmd:%v", kv.me, p.ClientId, *kv.request_table[p.ClientId], p)
				}
				//update by sequence
				if request.Seq+1 == p.Seq && kv.request_table[p.ClientId].Success {
					DPrintf("LOG1", "S%d new exec initial clientId :%v ", kv.me, p.ClientId)
					request.Seq = p.Seq
					request.Success = false
					request.Value = ""
				}
				/* leader */
				if request.Seq == p.Seq {
					if !kv.request_table[p.ClientId].Success {
						//execute command
						switch p.Op {
						case op_get:
							kv.request_table[p.ClientId].Value = kv.get(p.Key)
							kv.request_table[p.ClientId].Success = true
						case op_append:
							kv.append(p.Key, p.Value)
							kv.request_table[p.ClientId].Success = true
						case op_put:
							kv.put(p.Key, p.Value)
							kv.request_table[p.ClientId].Success = true
						default:
							kv.request_table[p.ClientId].Success = false
						}
						DPrintf("LOG1", "S%d exec result clientId :%v Seq:%d op:%v bool:%s key:%v val:%v", kv.me, p.ClientId, p.Seq, p.Op, kv.request_table[p.ClientId].Success, p.Key, kv.database[p.Key])
						/* snapshot */
						if kv.maxraftstate != -1 && kv.rf.GetRaftSize() >= kv.maxraftstate {
							w := new(bytes.Buffer)
							e := labgob.NewEncoder(w)
							//encode index
							e.Encode(m.CommandIndex)
							//encode database
							var xdatabase []interface{}
							for k, v := range kv.database {
								data := pair{k, v}
								xdatabase = append(xdatabase, data)
							}
							e.Encode(xdatabase)
							//encode request_table
							var xrequestTable []interface{}
							for k, v := range kv.request_table {
								res := *v
								item := tableItem{k, res}
								xrequestTable = append(xrequestTable, item)
							}
							e.Encode(xrequestTable)
							//call snapshot
							kv.rf.Snapshot(m.CommandIndex, w.Bytes())
						}
						/* notify request */
						ch, ok := kv.requestCh[p.ClientId]
						_, leaderstate := kv.getLeaderState()
						//ch is existed, only create by leader
						if ok && ch != nil && kv.requestSeq[p.ClientId] == p.Seq && leaderstate {
							DPrintf("LOCK", "S%d write begin ch id:%d", kv.me, p.ClientId)
							kv.mu.Unlock()
							ch <- true
							kv.mu.Lock()
							DPrintf("LOCK", "S%d write end ch id:%d", kv.me, p.ClientId)
						} else {
							DPrintf("LOG1", "S%d follower not write ch id:%d", kv.me, p.ClientId)
						}
					} else {
						DPrintf("LOG1", "S%d has been execed clientId :%v Seq:%d op:%v bool:%s val:%v", kv.me, p.ClientId, p.Seq, p.Op, kv.request_table[p.ClientId].Success, kv.database[p.Key])
					}
				} else {
					DPrintf("SVRO", "S%d cmd exec error Id:%d serveSeq:%d raftSeq:%d Success:%v", kv.me, p.ClientId, request.Seq, p.Seq, kv.request_table[p.ClientId].Success)
				}
				kv.mu.Unlock()
			} else {
				DPrintf("SVRO", "S%d op parse error Id:%d", kv.me, p.ClientId)
			}
		}
	}
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xdatabase []interface{}
	var xrequestTable []interface{}
	//decode
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xdatabase) != nil ||
		d.Decode(&xrequestTable) != nil {
		DPrintf("SVRO", "S%d snapshot Decode() error", kv.me)
		return
	}
	//assign database
	for i := 0; i < len(xdatabase); i++ {
		p, ok := (xdatabase[i]).(pair)
		if ok {
			kv.database[p.key] = p.value
		} else {
			DPrintf("SVRO", "S%d xdatabase Decode() error", kv.me)
		}
	}
	//assign request table
	for i := 0; i < len(xrequestTable); i++ {
		p, ok := (xrequestTable[i]).(tableItem)
		if ok {
			kv.request_table[p.id] = &p.res
		} else {
			DPrintf("SVRO", "S%d xrequestTable Decode() error", kv.me)
		}
	}
}

// func (kv *KVServer) replayLog() {
// 	xlog := kv.rf.GetLog(kv.lastApplied)
// 	for i := 0; i < len(xlog); i++ {
// 		p, ok := (xlog[i]).(Op)
// 		if ok {
// 			val, ok := kv.request_table[p.ClientId]
// 			//existed
// 			if ok {
// 				//duplicated
// 				if val.Seq == p.Seq {
// 					continue
// 				}
// 			}
// 			//apply to database
// 			switch p.Op {
// 			case op_get:
// 				kv.request_table[p.ClientId].Value = kv.get(p.Key)
// 				kv.request_table[p.ClientId].Success = true
// 			case op_append:
// 				kv.append(p.Key, p.Value)
// 				kv.request_table[p.ClientId].Success = true
// 			case op_put:
// 				kv.put(p.Key, p.Value)
// 				kv.request_table[p.ClientId].Success = true
// 			default:
// 				kv.request_table[p.ClientId].Success = false
// 			}
// 		} else {
// 			DPrintf("SVRO", "S%d replayLog Decode() error", kv.me)
// 		}
// 	}
// }

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/Value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = map[string]string{}
	kv.request_table = map[int64]*result{}
	kv.requestCh = make(map[int64]chan bool)
	kv.requestSeq = make(map[int64]int)
	kv.rfTerm = 0
	//snapshot
	kv.readSnapshot(kv.rf.GetSnapshot())
	DPrintf("LOG1", "S%d StartKVServer", kv.me)
	// start ticker goroutine to start elections
	go kv.ticker()

	return kv
}
