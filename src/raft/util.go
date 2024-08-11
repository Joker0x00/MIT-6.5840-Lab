package raft

import "log"

// Debugging
const Debug = false

func Printff(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
