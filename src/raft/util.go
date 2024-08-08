package raft

import "log"

// Debugging
const Debug = true

func Printff(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
