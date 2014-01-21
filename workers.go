package workers

import (
	// fmi "myproject/fmi-0mq"
	"fmt"
	"os"
	"bufio"
	zmq "github.com/alecthomas/gozmq"
	// "flag"
	"strconv"
)

type Interface interface {
	// Analyze(I *Index, pattern []byte) []int
	Analyze(pattern []byte) []int
	/*Connect(configure string)
	Load(dir string) *Index*/
	// Connect()
	// Indexload() *Index
	// Read() string
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
	// receiver.Connect("tcp://141.225.10.43:5557")
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
	// sender.Connect("tcp://141.225.9.77:5558")
	sender.Connect(string(cline))

	for {
		msgbytes, _ := receiver.Recv(0)
		fmt.Printf("%s.\n", string(msgbytes))
		// msec, _ := strconv.ParseInt(string(msgbytes), 10, 64)

		// seed
//////////////////////////////////////
		
		result := data.Analyze(msgbytes)
		fmt.Println(result)
		// type interface X { Analyze(read []bytes) string }
		// output = A.Analyze(msgbytes)
		// time.Sleep(time.Duration(msec) * 1e6)

		// Send results to sink
		sender.Send([]byte(string(msgbytes) + " " + strconv.Itoa(result[0])) , 0)
	}
}
