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

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dTrans   logTopic = "TRAN"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dAppend  logTopic = "APND"
	dRecv    logTopic = "RECV"
	dWarn    logTopic = "WARN"
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
func DebugPrint(topic logTopic, format string, a ...interface{}) {
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
