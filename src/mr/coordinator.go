package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTask struct {
	id      int //task
	file    string
	startAt time.Time
	done    bool
}

type ReduceTask struct {
	id      int
	files   []string
	startAt time.Time
	done    bool
}

type Coordinator struct {
	// Your definitions here.
	mutex        sync.Mutex
	mapTasks     []MapTask
	mapRemain    int
	reduceTasks  []ReduceTask
	reduceRemain int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) getTask(args *TaskDone, reply *TaskTodo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	switch args.DoneType {
	case TaskTypeMap:
		if !c.mapTasks[args.ID].done {
			c.mapTasks[args.ID].done = true
			for reduceId, file := range args.Files {
				if len(file) > 0 {
					c.reduceTasks[reduceId].files = append(c.reduceTasks[reduceId].files, file)
				}
			}
			c.mapRemain--
		}
	case TaskTypeReduce:
		if !c.reduceTasks[args.ID].done {
			c.reduceTasks[args.ID].done = true
			c.reduceRemain--
		}
	}

	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if c.mapRemain > 0 {
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeMap
				reply.ID = t.id
				reply.Files = []string{t.file}
				reply.NReduce = len(c.reduceTasks)

				t.startAt = now

				return nil
			}
		}
		reply.Type = TaskTypeSleep
	} else if c.reduceRemain > 0 {
		for idx := range c.reduceTasks {
			t := c.reduceTasks[idx]
			if !t.done && t.startAt.Before(timeoutAgo) {
				reply.Type = TaskTypeReduce
				reply.ID = t.id
				reply.Files = t.files

				t.startAt = now

				return nil
			}
		}
		reply.Type = TaskTypeSleep
	} else {
		reply.Type = TaskTypeExit
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.mapRemain == 0 && c.reduceRemain == 0
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{
		mapTasks:     make([]MapTask, len(files)),
		reduceTasks:  make([]ReduceTask, nReduce),
		mapRemain:    len(files),
		reduceRemain: nReduce,
	}

	// Your code here.

	for i, f := range files {
		c.mapTasks[i] = MapTask{id: i, file: f, done: false}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{id: i, done: false}
	}

	c.server()
	return &c
}
