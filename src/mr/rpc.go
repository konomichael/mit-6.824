package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RegisterArgs struct{}

type RegisterReply struct {
	WorkerId int `json:"worker_id"`
	NReduce  int `json:"n_reduce"`
}

type RequestTaskArgs struct {
	WorkerId int `json:"worker_id"`
}

type RequestTaskReply struct {
	Task Task `json:"task"`
}

type ReportTaskArgs struct {
	WorkerId int  `json:"worker_id"`
	Task     Task `json:"task"`
}

type ReportTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
