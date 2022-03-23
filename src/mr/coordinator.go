package mr

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

func init() {
	log.SetLevel(log.WarnLevel)
}

type Coordinator struct {
	mutex               sync.Mutex
	cond                *sync.Cond
	mapFiles            []string
	nMapTasks           int
	nReduceTasks        int
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time
	isDone              bool
}

func (c *Coordinator) AssignTask(arg *TaskArgs, reply *TaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.NReduce = c.nReduceTasks
	reply.NMap = c.nMapTasks
	log.Println("开始分配任务")

	for {
		mapDone := true
		for m, done := range c.mapTasksFinished {
			if !done {
				if c.mapTasksIssued[m].IsZero() ||
					time.Since(c.mapTasksIssued[m]).Seconds() > 10 {
					reply.Type = MapTask
					reply.TaskNum = m
					reply.Filename = c.mapFiles[m]
					c.mapTasksIssued[m] = time.Now()
					log.Infof("放出一个map任务 %+v", reply)
					return nil
				} else {
					mapDone = false
				}
			}
		}

		if mapDone {
			break
		} else {
			c.cond.Wait()
		}
	}

	for {
		redDone := true
		for r, done := range c.reduceTasksFinished {
			if !done {
				if c.reduceTasksIssued[r].IsZero() ||
					time.Since(c.reduceTasksIssued[r]).Seconds() > 10 {
					reply.Type = ReduceTask
					reply.TaskNum = r
					c.reduceTasksIssued[r] = time.Now()
					log.Infof("放出一个Reduce任务 %+v", reply)
					return nil
				} else {
					redDone = false
				}

			}
		}
		if redDone {
			break
		} else {
			c.cond.Wait()
		}
	}

	reply.Type = Done
	c.isDone = true
	log.Infof("通知任务完成 %+v", reply)
	return nil
}

func (c *Coordinator) HandleFinishedTask(arg *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	switch arg.TaskType {
	case MapTask:
		c.mapTasksFinished[arg.TaskNum] = true
	case ReduceTask:
		c.reduceTasksFinished[arg.TaskNum] = true
	default:
		log.Fatalf("错误的任务类型 %s", arg.TaskType)
	}
	log.Infof("work通知一个任务完成 %+v", arg)
	c.cond.Broadcast()
	return nil
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.isDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("开始运行")
	c := Coordinator{}
	c.cond = sync.NewCond(&c.mutex)
	c.mapFiles = files
	c.nMapTasks = len(files)
	log.Infof("files %v", files)
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))

	c.nReduceTasks = nReduce
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)
	go func() {
		for {
			c.mutex.Lock()
			c.cond.Broadcast()
			c.mutex.Unlock()
			time.Sleep(time.Second)
		}
	}()
	c.server()
	return &c
}
