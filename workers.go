package DistSys

import (
	"fmt"
	"os"
	"bufio"
	zmq "github.com/alecthomas/gozmq"
)

type Interfaceworkers interface {
	Analyze(message []byte) []byte
}

func Startworkers(data Interfaceworkers) {

	context, _ := zmq.NewContext()
	defer context.Close()

	// read configure file
	c, err := os.Open("configure.txt")
	if err != nil { 
		fmt.Println(err)
		panic("error opening file " + "configure.txt") 
	}
	cr := bufio.NewReader(c)
	// ventilator
	line, err := cr.ReadBytes('\n')
	fmt.Println(string(line))
	cline, _ := cr.ReadBytes('\n')
	if err != nil { fmt.Println(err) }
	fmt.Println(string(cline))

	receiver, _ := context.NewSocket(zmq.PULL)
	defer receiver.Close()
	receiver.Connect(string(cline))

	// Sink
	line, err = cr.ReadBytes('\n')
	fmt.Println(string(line))
	cline, _ = cr.ReadBytes('\n')
	if err != nil { fmt.Println(err) }
	fmt.Println(string(cline))	

	sender, _ := context.NewSocket(zmq.PUSH)
	defer sender.Close()
	sender.Connect(string(cline))

	for {
	    msgbytes, err := receiver.Recv(0)
	    if err != nil { fmt.Println(err) }
	    // fmt.Println(string(msgbytes))
		msg := data.Analyze(msgbytes)
	    sender.Send(msg, 0)
    }
}