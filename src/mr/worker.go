package mr

import (
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"os/exec"
	"sort"
)

import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := TaskArgs{}
		reply := TaskReply{}
		log.Info("开始调用任务分配")
		call("Coordinator.AssignTask", &args, &reply)
		log.Info("返回任务 %+v", reply)
		switch reply.Type {
		case MapTask:
			doMap(reply, mapf)
		case ReduceTask:
			doReduce(reply, reducef)
		case Done:
			os.Exit(0)
		default:
			log.Errorf("无效的任务类型 %s", reply.Type)
		}

		finTaskArgs := FinishedTaskArgs{TaskType: reply.Type, TaskNum: reply.TaskNum}
		finTaskReply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finTaskArgs, &finTaskReply)
	}
}

func doMap(reply TaskReply, mapf func(string, string) []KeyValue) {
	log.Infof("domap%+v\n", reply)
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("无法打开%v", reply.Filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("无法读取%v", reply.Filename)
	}
	defer file.Close()

	kva := mapf(reply.Filename, string(content))
	var tmpFiles []*os.File
	var tmpFilenames []string
	var encoders []*json.Encoder
	for r := 0; r < reply.NReduce; r++ {
		log.Infof("生成临时文件")
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("无法创建临时文件 %v", err)
		}
		tmpFiles = append(tmpFiles, tmpFile)
		log.Infof(tmpFile.Name())
		tmpFilenames = append(tmpFilenames, tmpFile.Name())
		encoders = append(encoders, json.NewEncoder(tmpFile))
	}

	log.Infof("写入临时文件共 %d", len(kva))
	for _, kv := range kva {
		r := ihash(kv.Key) % reply.NReduce
		encoders[r].Encode(&kv)

	}

	for _, f := range tmpFiles {
		f.Close()
	}

	for r := 0; r < reply.NReduce; r++ {
		finalizeIntermediateFile(tmpFilenames[r], reply.TaskNum, r)
	}

}

func doReduce(reply TaskReply, reducef func(string, []string) string) {
	log.Infof("开始Reduce %v", reply)
	var kva []KeyValue
	for m := 0; m < reply.NMap; m++ {
		iFilename := getIntermediateFile(m, reply.TaskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("无法打开中间文件:%v## %v", iFilename, err)
		}
		doc := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := doc.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("无法创建临时文件 %v", err)
	}
	tmpFilename := tmpFile.Name()

	keyBegin := 0
	for keyBegin < len(kva) {
		keyEnd := keyBegin + 1
		for keyEnd < len(kva) && kva[keyEnd].Key == kva[keyBegin].Key {
			keyEnd++
		}
		var values []string
		for k := keyBegin; k < keyEnd; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[keyBegin].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[keyBegin].Key, output)
		keyBegin = keyEnd
	}
	log.Infof("输出Reduce最终文件 %v", reply)
	finalizeReduceFile(tmpFilename, reply.TaskNum)
}

func finalizeReduceFile(tempFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	var cmd *exec.Cmd
	cmd = exec.Command("mv", tempFile, finalFile)
	_, err := cmd.Output()
	if err != nil {
		log.Fatalf("改名失败 %v", err)
	}
	//os.Rename(tempFile, finalFile)
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tempFile string, mapTaskN int, reduceTaskN int) {

	finalFile := getIntermediateFile(mapTaskN, reduceTaskN)
	log.Infof("临时文件改名 %s--> %s", tempFile, finalFile)
	var cmd *exec.Cmd
	cmd = exec.Command("mv", tempFile, finalFile)
	_, err := cmd.Output()
	if err != nil {
		log.Fatalf("改名失败 %v", err)
	}
	//err := os.Rename(tempFile, finalFile)
	//if err != nil {
	//	log.Fatalf("改名失败 %v", err)
	//}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
