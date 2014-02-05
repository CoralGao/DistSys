package DistSys

import (
	"fmt"
	"os"
	"bufio"
	zmq "github.com/alecthomas/gozmq"
	"strconv"
	"bytes"
)

type Interfaceworkers interface {
	Analyze(pattern []byte) []int
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

	// Subscribe
	line, err = cr.ReadBytes('\n')
	fmt.Println(string(line))

	cline, _ = cr.ReadBytes('\n')
	if err != nil { fmt.Println(err) }
	fmt.Println(string(cline))

	Subscriber, _ := context.NewSocket(zmq.SUB)
	defer Subscriber.Close()
	Subscriber.SetSubscribe("finish")
	Subscriber.Connect(string(cline))

	work_quit := make(chan int)
	go func() {
		// for {
			signal, _ := Subscriber.Recv(0)
			fmt.Println(signal)
		    if bytes.Equal(signal, []byte("finish")) {
		    	fmt.Println("finish")
		    	// fmt.Println(stop)
		    	work_quit <- 1
		    // }
		}
	}()

	stop := 0
	for {
		select {
			case stop = <- work_quit:
				fmt.Println("finish")
				fmt.Println(stop)
				break
			default:
				break
	    }
	    // fmt.Println(stop)
	    if stop == 1 {
	    	break
	    }
	    msgbytes, err := receiver.Recv(0)
	    if err != nil { fmt.Println(err) }
		result := data.Analyze(msgbytes[:100])
		fmt.Println(string(msgbytes[:100]))
		fmt.Println(string(append(append(msgbytes[100:], byte(' ')), int_byte(result)...)))
	    sender.Send(append(append(msgbytes[100:], byte(' ')), int_byte(result)...) , 0)
	    // fmt.Println(stop)
    }
}

func int_byte(intarray []int) []byte{
	bytearray := make([]byte, 0)
	for i := 0; i < len(intarray); i++ {
		bytearray = append(append(bytearray, []byte(strconv.Itoa(intarray[i]))...), byte(' '))
	}
	return bytearray
}