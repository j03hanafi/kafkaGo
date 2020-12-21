package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func doConsume(broker Broker, topic, message string) error {

	fmt.Printf("Starting consumer, message: %v\n", message)

	// store the Consumer Config
	cm := kafka.ConfigMap{
		"bootstrap.servers":    "localhost:9092",
		"group.id":             "test_go_kafka",
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
	}

	// holding the Consumer instance
	c, err := kafka.NewConsumer(&cm)

	// check if there's error in creating The Consumer
	if err != nil {
		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				return fmt.Errorf("Can't create consumer because wrong configuration (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md\n", ec, err)
			default:
				return fmt.Errorf("Can't create consumer (code: %d)!\n\t%v\n", ec, err)
			}
		} else {
			// Not Kafka Error occurs
			return fmt.Errorf("Can't create consumer because generic error! \n\t%v\n", err)
		}
	} else {

		// subscribe to the topic
		if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
			return fmt.Errorf("There's an Error subscribing to the topic:\n\t%v\n", err)
		}

		fmt.Printf("Topic %v subscribed", topic)
		doTerm := false

		for !doTerm {
			ev := c.Poll(1000)
			if ev == nil {
				// the Poll timed out and we got nothing
				fmt.Printf("....\n")
				a, _ := c.Assignment()
				p, _ := c.Position(a)

				for _, x := range p {
					fmt.Printf("Partition %v position %v\n", x.Partition, x.Offset)
				}

				continue
			} else {

				switch ev.(type) {

				case *kafka.Message:
					// It's a message
					km := ev.(*kafka.Message)
					fmt.Printf("Message '%v' received from topic '%v' (partition %d at offset %d)\n",
						string(km.Value),
						*km.TopicPartition.Topic,
						km.TopicPartition.Partition,
						km.TopicPartition.Offset)
					if message == string(km.Value) {
						fmt.Printf(("Reading the message. Closing the costumer"))
						doTerm = true
					}

				case kafka.PartitionEOF:
					pe := ev.(kafka.PartitionEOF)
					fmt.Printf("Got to the end of partition %v on topic %v at offset %v\n",
						pe.Partition,
						*pe.Topic,
						pe.Offset)

				case kafka.OffsetsCommitted:
					continue

				case kafka.Error:
					// It's an error
					em := ev.(kafka.Error)
					return fmt.Errorf("☠️ Uh oh, caught an error:\n\t%v\n", em)

				default:
					// It's not anything we were expecting
					fmt.Printf("Got an event that's not a Message, Error, or PartitionEOF 👻\n\t%v\n", ev)

				}
			}
		}

		// exit the Consumer when finish
		c.Close()
	}
	return nil
}
