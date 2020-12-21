package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Broker struct {
	host string
	port int
}

func (b Broker) String() string {
	return fmt.Sprintf("%v:%v", b.host, b.port)
}

func main() {

	// set broker
	broker := setBroker(os.Args)
	fmt.Printf("Broker: %v\n", broker)

	// set topic
	topic := "kafka-go"

	// doAdmin
	if err := doAdmin(broker); err != nil {
		fmt.Printf("Error in calling Admin Client:\n\t%v\n", err)
	} else {
		fmt.Printf("(Admin Client is running ... )\n")
		// doProduce
		if msg, err := doProduce(broker, topic); err != nil {
			fmt.Printf("Error in calling Producer:\n\t%v\n", err)
		} else {
			fmt.Printf("(Producer is running ... )\n")
			// doConsume
			if err := doConsume(broker, topic, msg); err != nil {
				fmt.Printf("(Consumer is running ... )\n")
			}
		}
	}
}

func setBroker(i []string) Broker {
	b := Broker{
		host: "localhost",
		port: 9092,
	}

	// get broker detail from commandline argument
	if len(i) == 2 {
		arg := i[1]
		// check if there's a colon
		if p := strings.Split(arg, ":"); len(p) == 2 {
			// check if port is int
			if _, err := strconv.Atoi(p[1]); err == nil {
				b.port, _ = strconv.Atoi(p[1])
				b.host = p[0]
			} else {
				fmt.Printf("%v should be integer, default is %v", p[1], b)
			}
		} else {
			fmt.Printf("(Argument %v is not host:port, default is %v\n", arg, b)
		}
	} else {
		fmt.Printf("Broker not found. Default is %v\n", b)
	}
	return b
}
