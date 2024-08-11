package raft

import (
	"fmt"
	"log"
	"time"
)

type logTopic string

const debug = true
const (
	dBug     logTopic = "DEBUG"
	dClient  logTopic = "CLNT" //
	dAE      logTopic = "AE"
	dApply   logTopic = "APPLY"
	dCommit  logTopic = "CMIT" // 提交log
	dDrop    logTopic = "DROP" // 丢弃log
	dError   logTopic = "ERRO" // 出现错误
	dInfo    logTopic = "INFO" // 信息
	dRole    logTopic = "ROLE" // 角色变更
	dLog     logTopic = "LOG"  // log相关
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM" // 任期变更
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR" // 计时器重置
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE" // 投票事件
	dWarn    logTopic = "WARN" // 警告事件
	dRoutine logTopic = "ROUTINE"
)

var debugStart time.Time = time.Now()

// var start chan bool = make(chan bool, 1)
// var debugVerbosity int

func Init() {
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Printf(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
