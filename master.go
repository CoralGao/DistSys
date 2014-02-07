

//
// Task ventilator
// Binds PUSH socket to tcp://localhost:5557
// Sends batch of tasks to workers via that socket
//
package DistSys

import (
        "fmt"
        zmq "github.com/alecthomas/gozmq"
        "flag"
        "os"
        "bufio"
)

var Debug bool

type Interfacemaster interface {
        AnalyzeResult(pattern []byte)
        ProduceMsg(line []byte, count int, filename string) []byte
}

func Startmaster(data Interfacemaster) {
        var queries_file = flag.String("q", "", "queries file")
        var index_file = flag.String("i", "", "index file")
        flag.Parse()

        // ventilator
        context, _ := zmq.NewContext()
        defer context.Close()

        sender, _ := context.NewSocket(zmq.PUSH)
        defer sender.Close()
        sender.Bind("tcp://*:5557")
        fmt.Println("Sending tasks to workers...")

        //Sink
        receiver, _ := context.NewSocket(zmq.PULL)
        defer receiver.Close()
        receiver.Bind("tcp://*:5558")

        vent_quit := make(chan int)
        if *queries_file!="" && *index_file!=""{
                f, err := os.Open(*queries_file)
                if err != nil { panic("error opening file " + *queries_file) }
                r := bufio.NewReader(f)
                go func() {
                        count := 0
                        for {
                                line, err := r.ReadBytes('\n')
                                if err != nil { break }
                                if len(line) > 1 {
                                        msg := data.ProduceMsg(line, count, *index_file)
                                        sender.Send(msg, 0)
                                }
                                count++
                        }
                        vent_quit <- count
                }()
        }

        read_count := 0
        result_count := 0

        // receving results from workers
        for result_count < read_count || read_count <= 0 {
                select {
                        case read_count = <- vent_quit:
                                break
                        default:
                                break
                }
                msgbytes, _ := receiver.Recv(0)
                result_count++

                data.AnalyzeResult(msgbytes)
        }
}