package raft

import "log"

func init() {
	log.SetFlags(log.Lmicroseconds)
}

// Debugging
const Debug = true
// const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
