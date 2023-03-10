package raft

import (
	"log"
	"math/rand"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func randElectionTimeout() int {
	// reset seed
	rand.Seed(makeSeed())
	return rand.Intn(ElectionTimeout) + ElectionTimeout
}
