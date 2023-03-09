package mr

import (
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mapTasks    []*Task
	mapDone     bool
	reduceTasks []*Task
	reduceDone  bool

	sync.RWMutex
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	for i, file := range files {
		task := NewMapTask(i, file)
		c.mapTasks = append(c.mapTasks, &task)
	}
	for i := 0; i < nReduce; i++ {
		task := NewReduceTask(i)
		c.reduceTasks = append(c.reduceTasks, &task)
	}

	c.server()

	ticker := time.NewTicker(10 * time.Millisecond)
	go func() {
		for range ticker.C {
			if c.reduceDone {
				return
			}
			now := time.Now()
			before := now.Add(-10 * time.Second)
			c.Lock()
			if c.mapDone {
				for _, task := range c.reduceTasks {
					if task.Status == Running && task.StartTime.Before(before) {
						task.Status = Created
						task.StartTime = time.Time{}
					}
				}
			} else {
				for _, task := range c.mapTasks {
					if task.Status == Running && task.StartTime.Before(before) {
						task.Status = Created
						task.StartTime = time.Time{}
					}
				}
			}
			c.Unlock()
		}
	}()

	return &c
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.RLock()
	defer c.RUnlock()

	return c.reduceDone
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	id := rand.Intn(1000)
	reply.WorkerId = id
	reply.NReduce = len(c.reduceTasks)
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Lock()
	defer c.Unlock()
	if c.reduceDone {
		reply.Task = NewExitTask()
		return nil
	}

	if c.mapDone {
		for _, task := range c.reduceTasks {
			if task.Status == Created {
				task.WorkerId = args.WorkerId
				task.StartTime = time.Now()
				task.Status = Running
				reply.Task = *task
				return nil
			}
		}
		reply.Task = NewWaitTask()
		return nil
	}

	for _, task := range c.mapTasks {
		if task.Status == Created {
			task.WorkerId = args.WorkerId
			task.StartTime = time.Now()
			task.Status = Running
			reply.Task = *task
			return nil
		}
	}

	reply.Task = NewWaitTask()
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.Lock()
	defer c.Unlock()
	task := args.Task
	switch task.Type {
	case MapTask:
		if c.mapTasks[task.Id].Status != Running || c.mapTasks[task.Id].WorkerId != args.WorkerId {
			return nil
		}
		c.mapTasks[task.Id].Status = Completed
		mapDone := true
		for _, task := range c.mapTasks {
			if task.Status != Completed {
				mapDone = false
				break
			}
		}
		c.mapDone = mapDone
		for i, file := range task.Files {
			c.reduceTasks[i].Files = append(c.reduceTasks[i].Files, file)
		}
	case ReduceTask:
		if c.reduceTasks[task.Id].Status != Running || c.reduceTasks[task.Id].WorkerId != args.WorkerId {
			return nil
		}
		c.reduceTasks[task.Id].Status = Completed
		reduceDone := true
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				reduceDone = false
				break
			}
		}
		c.reduceDone = reduceDone
	}
	return nil
}
