package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const Debug = false

type LogTopic string

const (
	dClient   LogTopic = "CLNT"
	dCommit   LogTopic = "CMIT"
	dDrop     LogTopic = "DROP"
	dError    LogTopic = "ERRO"
	dTrans    LogTopic = "TRAN"
	dInfo     LogTopic = "INFO"
	dLeader   LogTopic = "LEAD"
	dLog      LogTopic = "LOG1"
	dLog2     LogTopic = "LOG2"
	dPersist  LogTopic = "PERS"
	dSnap     LogTopic = "SNAP"
	dTerm     LogTopic = "TERM"
	dTest     LogTopic = "TEST"
	dTimer    LogTopic = "TIMR"
	dTrace    LogTopic = "TRCE"
	dVote     LogTopic = "VOTE"
	dAppend   LogTopic = "APND"
	dRecv     LogTopic = "RECV"
	dWarn     LogTopic = "WARN"
	dServ     LogTopic = "SERV"
	dServErro LogTopic = "SVRO"
	dLock     LogTopic = "LOCK"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var debugStart time.Time
var debugVerbosity int
var debugIsStart bool

func debugInit() {
	if !debugIsStart {
		debugIsStart = true
		debugVerbosity = 1
		debugStart = time.Now()

		// f, err := os.OpenFile("raft.log", os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		// if err != nil {
		// 	return
		// }
		// multiWriter := io.MultiWriter(os.Stdout)
		// multiWriter := io.MultiWriter(f)
		// log.SetOutput(multiWriter)
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
}

// command   VERBOSE=1 go test -run TestInitialElection2A
// DebugPrint(dTimer, "S%d Leader, checking heartbeats", rf.me)
func DebugPrint(topic LogTopic, format string, a ...interface{}) {
	debugInit()
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}
