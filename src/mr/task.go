package mr

import "time"

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskStatus int

const (
	Created TaskStatus = iota
	Running
	Completed
)

type Task struct {
	Id        int        `json:"id"`
	WorkerId  int        `json:"worker_id"`
	Type      TaskType   `json:"type"`
	Files     []string   `json:"file"`
	Status    TaskStatus `json:"status"`
	StartTime time.Time  `json:"start_time"`
}

func NewMapTask(id int, files ...string) Task {
	return Task{
		Id:     id,
		Type:   MapTask,
		Files:  files,
		Status: Created,
	}
}

func NewReduceTask(id int) Task {
	return Task{
		Id:     id,
		Type:   ReduceTask,
		Status: Created,
	}
}

func NewWaitTask() Task {
	return Task{
		Type:   WaitTask,
		Status: Created,
	}
}

func NewExitTask() Task {
	return Task{
		Type:   ExitTask,
		Status: Created,
	}
}
