

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
        "runtime"
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

        fmt.Print("Press Enter when the workers are ready: ")

        var line string
        fmt.Scanln(&line)

        fmt.Println("Sending tasks to workers...")

        //Sink
        contexts, _ := zmq.NewContext()
        defer contexts.Close()

        // Socket to receive messages on
        receiver, _ := contexts.NewSocket(zmq.PULL)
        defer receiver.Close()
        receiver.Bind("tcp://*:5558")

        runtime.GOMAXPROCS(8)
        if *queries_file!="" {
                f, err := os.Open(*queries_file)
                if err != nil { panic("error opening file " + *queries_file) }
                r := bufio.NewReader(f)
                vent_quit := make(chan int)
                go func() {
                        for {
                                line, err := r.ReadBytes('\n')
                                if err != nil { break }
                                if len(line) > 1 {
                                        line = line[0:len(line)-1]
                                        msg := line
                                        fmt.Printf("Sending %s\n", msg)
                                        sender.Send([]byte(msg), 0)
                                        // time.Sleep(1e9)
                                }
                        }
                        vent_quit <- 1
                }()

		// receving results from workers
                go func() {
                        for {
                                msgbytes, _ := receiver.Recv(0)
                                // fmt.Println("Sync received: ",string(msgbytes))
                                data.AnalyzeResult(msgbytes)
                        }
                }()
                <- vent_quit
        }

}