package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var newTask TaskTodo
	var finishedTask TaskDone = TaskDone{DoneType: TaskTypeNone}

	for {
		newTask = CallMaster(&finishedTask)

		switch newTask.Type {
		case TaskTypeMap:
			f := newTask.Files[0]
			file, err := os.Open(f)
			if err != nil {
				log.Fatal("cannot open %v", f)
			}
			defer file.Close()
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatal("cannot read %v", f)
			}

			intermediate := mapf(f, string(content))

			byRedeceFiles := make(map[int][]KeyValue)
			for _, kv := range intermediate {
				idx := ihash(kv.Key) % newTask.NReduce
				byRedeceFiles[idx] = append(byRedeceFiles[idx], kv)
			}

			toReduceFiles := make([]string, newTask.NReduce)
			for reduceId, kvs := range byRedeceFiles {
				filename := fmt.Sprintf("mr-%d-%d", newTask.ID, reduceId)
				interfile, _ := os.Create(filename)
				defer interfile.Close()
				enc := json.NewEncoder(interfile)
				for _, kv := range kvs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}
				toReduceFiles[reduceId] = filename
			}
			finishedTask = TaskDone{DoneType: TaskTypeMap, ID: newTask.ID, Files: toReduceFiles}
		case TaskTypeReduce:
			intermediate := []KeyValue{}
			for _, filename := range newTask.Files {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal("cannot open %v", filename)
				}
				defer file.Close()

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", newTask.ID)
			ofile, _ := os.Create(oname)
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
				output := reducef(intermediate[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			finishedTask = TaskDone{DoneType: TaskTypeReduce, ID: newTask.ID, Files: []string{oname}}
		case TaskTypeSleep:
			time.Sleep(500 & time.Millisecond)
			finishedTask = TaskDone{DoneType: TaskTypeNone}
		case TaskTypeExit:
			return
		default:
			panic(fmt.Sprintf("unknown type: %v", newTask.Type))

		}

	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// CallMaster
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallMaster(finishedTask *TaskDone) TaskTodo {

	// declare a reply structure.
	reply := TaskTodo{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", finishedTask, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return reply
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
