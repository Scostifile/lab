package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d\n", taskN)
	os.Rename(tmpFile, finalFile)
}

func getIntermediateFile(mapTaskN int, reduceTaskN int) string {
	//log.Printf("Getting intermediate file for map %d and reduce %d\n", mapTaskN, reduceTaskN)
	return fmt.Sprintf("mr-%d-%d\n", mapTaskN, reduceTaskN)
}

func finalizeIntermediateFile(tmpFile string, mapTaskN int, reduceTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, reduceTaskN)
	os.Rename(tmpFile, finalFile)
}

// Implementation of map task
func performMap(fileName string,
	taskNum int,
	nReduceTasks int,
	mapf func(string, string) []KeyValue) {

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	kva := mapf(fileName, string(content))

	// create temporary files and encoders for NReduceTasks file
	tmpFiles := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot open tmpfile")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFileName := tmpFile.Name()
		tmpFileNames = append(tmpFileNames, tmpFileName)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	// write output keys to temp files, using the ihash function
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTasks
		encoders[r].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}

	// atomically rename temp files to final intermediate files
	for r := 0; r < nReduceTasks; r++ {
		finalizeIntermediateFile(tmpFileNames[r], taskNum, r)
	}
}

func performReduce(taskNum int,
	nMapTasks int,
	reducef func(string, []string) string) {

	// get all intermediate files corresponding to this reduce task,
	// and collect the corresponding key-value pairs
	kva := []KeyValue{}
	for m := 0; m < nMapTasks; m++ {
		iFileName := getIntermediateFile(m, taskNum)
		file, err := os.Open(iFileName)
		if err != nil {
			log.Fatalf("cannot open %v", iFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	// get temporary reduce file to write values
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot open tmpFIle")
	}
	tmpFileName := tmpFile.Name()

	// apply reduce function once to all values of the same key
	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin + 1
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		outPut := reducef(kva[key_begin].Key, values)

		// write output to reduce task temp file
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, outPut)

		// go to the next key
		key_begin = key_end
	}

	// atomically rename reduce file to final reduce file
	finalizeReduceFile(tmpFileName, taskNum)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		call("Coordinator.HandleGetTask", &args, &reply)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %s", reply.TaskType)
		}

		finalArgs := FinishedTaskArgs{
			TaskType: reply.TaskType,
			TaskNum:  reply.TaskNum,
		}
		finalReply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finalArgs, &finalReply)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
