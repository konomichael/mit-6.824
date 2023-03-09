package mr

import (
	"context"
	"fmt"
	"log"
	"net/rpc"
	"os/signal"
	"syscall"
	"time"
)

type WorkStatus int

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := &worker{
		mapf:    mapf,
		reducef: reducef,
		stopCh:  make(chan struct{}),
	}

	w.register()

	go w.start()
	w.gracefullyShutdown()
}

type worker struct {
	id      int
	status  WorkStatus
	nReduce int

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	stopCh chan struct{}
}

func (w *worker) gracefullyShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		log.Printf("worker %d received signal %s, shutting down", w.id, ctx.Err())
	case <-w.stopCh:
		log.Printf("worker %d shutting down", w.id)
	}
}

func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}

	if ok := call("Coordinator.Register", args, reply); !ok {
		log.Fatal("register failed")
	}

	log.Printf("worker %d registered", reply.WorkerId)

	w.id = reply.WorkerId
	w.nReduce = reply.NReduce
}

func (w *worker) start() {
	for {
		requestTaskReply := &RequestTaskReply{}
		if ok := call("Coordinator.RequestTask", &RequestTaskArgs{WorkerId: w.id}, requestTaskReply); ok {
			log.Printf("worker %d received task %v", w.id, requestTaskReply.Task)
			task := &requestTaskReply.Task
			switch task.Type {
			case MapTask:
				w.doMap(task)
			case ReduceTask:
				w.doReduce(task)
			case ExitTask:
				w.stopCh <- struct{}{}
				return
			case WaitTask:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
