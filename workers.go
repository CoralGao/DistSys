package DistSys

import (
	"fmt"
	"os"
	"bufio"
	zmq "github.com/alecthomas/gozmq"
	"strconv"
	/*"encoding/gob"
	"bytes"*/
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
		panic("error opening file " + "configure.txt") }
	cr := bufio.NewReader(c)
	line, err := cr.ReadBytes('\n')
	fmt.Println(string(line))

	cline, _ := cr.ReadBytes('\n')
	if err != nil { fmt.Println(err) }
	// Socket to send messages On
	fmt.Println(string(cline))

	// Socket to receive messages on

	receiver, _ := context.NewSocket(zmq.PULL)
	defer receiver.Close()
	receiver.Connect(string(cline))

	line, err = cr.ReadBytes('\n')
	fmt.Println(string(line))

	cline, _ = cr.ReadBytes('\n')
	if err != nil { fmt.Println(err) }
	// Socket to send messages On
	fmt.Println(string(cline))	

	// Socket to send messages to task sink
	sender, _ := context.NewSocket(zmq.PUSH)
	defer sender.Close()
	sender.Connect(string(cline))

	for {
		msgbytes, _ := receiver.Recv(0)
		// fmt.Println(msgbytes)
		// fmt.Printf("%s.\n", string(msgbytes[:100]))
		result := data.Analyze(msgbytes[:100])
		// fmt.Println(string(msgbytes) + " " + strconv.Itoa(result[0]))
		/*var fout bytes.Buffer
		enc := gob.NewEncoder(&fout)
		err := enc.Encode(msgbytes + result)*/

		// Send results to sink
		// fmt.Println(string(append(append(msgbytes[100:], byte(' ')), int_byte(result)...)))
		// fmt.Println(string(msgbytes) + " " + strconv.Itoa(result[0]))
        sender.Send(append(append(msgbytes[100:], byte(' ')), int_byte(result)...) , 0)
    }
}

func int_byte(intarray []int) []byte{
	bytearray := make([]byte, 0)
	for i := 0; i < len(intarray); i++ {
		bytearray = append(append(bytearray, []byte(strconv.Itoa(intarray[i]))...), byte(' '))
	}
	return bytearray
}