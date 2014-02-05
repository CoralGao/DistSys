

//
// Task ventilator
// Binds PUSH socket to tcp://localhost:5557
// Sends batch of tasks to workers via that socket
//
package DistSys

import (
        "fmt"
        zmq "github.com/alecthomas/gozmq"
        "strconv"
        "flag"
        "os"
        "bufio"
)

var Debug bool

type Interfacemaster interface {
        AnalyzeResult(pattern []byte)
}

func Startmaster(data Interfacemaster) {

        // ventilator
        var queries_file = flag.String("q", "", "queries file")
        flag.BoolVar(&Debug, "debug", false, "Turn on debug mode.")
        flag.Parse()

        // ventilator
        context, _ := zmq.NewContext()
        defer context.Close()

        // Socket to send messages On
        sender, _ := context.NewSocket(zmq.PUSH)
        defer sender.Close()
        sender.Bind("tcp://*:5557")

        fmt.Println("Sending tasks to workers...")

        //Sink
        // Socket to receive messages on
        receiver, _ := context.NewSocket(zmq.PULL)
        defer receiver.Close()
        receiver.Bind("tcp://*:5558")

        // pub
        // Socket to receive messages on
        publisher, _ := context.NewSocket(zmq.PUB)
        defer publisher.Close()
        publisher.Bind("tcp://*:5556")

        vent_quit := make(chan int)

        if *queries_file!="" {
                f, err := os.Open(*queries_file)
                if err != nil { panic("error opening file " + *queries_file) }
                r := bufio.NewReader(f)
                go func() {
                        count := 0
                        for {
                                line, err := r.ReadBytes('\n')
                                if err != nil { break }
                                if len(line) > 1 {
                                        line = line[0:len(line)-1]
                                        msg := line
                                        sender.Send(append([]byte(msg),[]byte(strconv.Itoa(count+1))...), 0)
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

        for {
                publisher.Send([]byte("finish"), 0)
                // fmt.Println("finish")
        }
}