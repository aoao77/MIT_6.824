package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	op_put    = "P"
	op_append = "A"
	op_get    = "G"
)

const (
	E_noError     Err = "no_error"
	E_noLeader    Err = "no_leader"
	E_noOp        Err = "no_operation"
	E_seqTimeout  Err = "no_seqtimeout"
	E_seqNoLinear Err = "seqNoLinear"
	E_execTimeout Err = "no_exectimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq      int
}

type GetReply struct {
	Err   Err
	Value string
}
