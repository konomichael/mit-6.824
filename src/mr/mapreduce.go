package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
)

// KeyValue is the intermediate key/value pair for MapReduce.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ihash returns an int hash of the string s.
// use ihash(key) % NReduce to choose to the reduce-task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) doMap(task *Task) {
	file, id := task.Files[0], task.Id

	content, err := os.ReadFile(file)
	if err != nil {
		log.Fatal("read file failed")
	}

	byReduceFiles := make([][]KeyValue, w.nReduce)
	files := make([]string, w.nReduce)

	intermediate := w.mapf(file, string(content))

	for _, kv := range intermediate {
		index := ihash(kv.Key) % w.nReduce
		byReduceFiles[index] = append(byReduceFiles[index], kv)
	}

	for i := 0; i < w.nReduce; i++ {
		file := fmt.Sprintf("mr-%d-%d-%d", w.id, id, i)
		ofile, err := os.Create(file)
		if err != nil {
			log.Fatal("create file failed")
		}

		err = json.NewEncoder(ofile).Encode(byReduceFiles[i])
		if err != nil {
			log.Fatal("encode failed")
		}

		ofile.Close()

		files[i] = file
	}

	task.Files = files

	reportTaskArgs := &ReportTaskArgs{
		WorkerId: w.id,
		Task:     *task,
	}
	var reply ReportTaskReply
	if ok := call("Coordinator.ReportTask", reportTaskArgs, &reply); !ok {
		log.Fatal("report task failed")
	}
}

func (w *worker) doReduce(task *Task) {
	files := task.Files

	var intermediate []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("open file failed")
		}

		var kv []KeyValue
		err = json.NewDecoder(file).Decode(&kv)
		if err != nil {
			log.Fatal("decode failed")
		}

		intermediate = append(intermediate, kv...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d-%d", w.id, task.Id)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatal("create file failed")
	}
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	reportTaskArgs := &ReportTaskArgs{
		WorkerId: w.id,
		Task:     *task,
	}
	var reply ReportTaskReply
	if ok := call("Coordinator.ReportTask", reportTaskArgs, &reply); !ok {
		log.Fatal("report task failed")
	}
}
