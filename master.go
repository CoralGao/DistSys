

//
// Task ventilator
// Binds PUSH socket to tcp://localhost:5557
// Sends batch of tasks to workers via that socket
//
package DistSys

import (
        "fmt"
        zmq "github.com/alecthomas/gozmq"
        // "time"
        "flag"
        "os"
        "bufio"
        // "runtime"
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

        contextv, _ := zmq.NewContext()
        defer contextv.Close()

        // Socket to send messages On
        sender, _ := contextv.NewSocket(zmq.PUSH)
        defer sender.Close()

        sender.Bind("tcp://*:5557")

        /*fmt.Print("Press Enter when the workers are ready: ")

        var line string
        fmt.Scanln(&line)*/

        fmt.Println("Sending tasks to workers...")

        //Sink
        contexts, _ := zmq.NewContext()
        defer contexts.Close()

        // Socket to receive messages on
        receiver, _ := contexts.NewSocket(zmq.PULL)
        defer receiver.Close()
        receiver.Bind("tcp://*:5558")

        vent_quit := make(chan int)
        // sink_quit := make(chan int)

        if *queries_file!="" {
                f, err := os.Open(*queries_file)
                if err != nil { panic("error opening file " + *queries_file) }
                r := bufio.NewReader(f)
                // vent_quit := make(chan int)
                // sink_quit := make(chan int)
                go func() {
                        count := 0
                        for {
                                line, err := r.ReadBytes('\n')
                                if err != nil { break }
                                if len(line) > 1 {
                                        line = line[0:len(line)-1]
                                        msg := line
                                        sender.Send([]byte(msg), 0)
                                }
                                count++
                        }
                        vent_quit <- count
                        // vent_quit <- 1
                }()
        }

        read_count := 0
        result_count := 0

        // receving results from workers
        for {
                select {
                        case read_count = <- vent_quit:
                                break
                        default:
                                break
                }
                msgbytes, _ := receiver.Recv(0)
                result_count++

                data.AnalyzeResult(msgbytes)
                if result_count >= read_count && read_count > 0{
                        break
                }
        }
}