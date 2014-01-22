package workers

import (
	"fmt"
	"os"
	"bufio"
	zmq "github.com/alecthomas/gozmq"
	"strconv"
)

type Interface interface {
	Analyze(pattern []byte) []int
}

func Start(data Interface) {

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
		fmt.Printf("%s.\n", string(msgbytes))
		
		result := data.Analyze(msgbytes)
		fmt.Println(result)

		// Send results to sink
		sender.Send([]byte(string(msgbytes) + " " + strconv.Itoa(result[0])) , 0)
	}
}
