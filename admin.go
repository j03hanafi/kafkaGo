package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

func doAdmin(broker Broker) error {

	// store config
	cm := kafka.ConfigMap{
		"bootstrap.servers": broker.String(),
	}

	// holding the AdminClient instance
	a, err := kafka.NewAdminClient(&cm)
	// close when done
	defer a.Close()

	// check error for admin creation
	if err != nil {
		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				fmt.Printf("Can't create Admin because wrong configuration (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md\n", ec, err)
			default:
				fmt.Printf("Can't create Admin (code: %d)!\n\t%v\n", ec, err)
			}
		} else {
			fmt.Printf("Can't create Admin because generic error!\n\t%v\n", err)
		}
	} else {
		fmt.Printf("Admin created! \n")

		// get metadatas
		if md, err := a.GetMetadata(nil, false, int(5*time.Second)); err != nil {
			if ke, ok := err.(kafka.Error); ok == true {
				switch ec := ke.Code(); ec {
				case kafka.ErrTransport:
					return fmt.Errorf("Error (%v) getting metadata\n"+
						"Is %v a valid broker and reachable from the machine on which this is running?", err, broker)
				default:
					return fmt.Errorf("Error getting metadata\n\tError: %v", err)
				}
			} else {
				return fmt.Errorf("Error getting metadata\n\tError: %v", err)
			}
		} else {
			// Originating Broker
			fmt.Printf("Metadata [Originating Broker]\n")
			b := md.OriginatingBroker
			fmt.Printf("\t[ID %d] %v\n", b.ID, b.Host)

			// Print broker
			fmt.Printf("Metadata [brokers]\n")
			f := false
			for _, b := range md.Brokers {
				fmt.Printf("\t[ID %d] %v:%d\n", b.ID, b.Host, b.Port)
				if (b.Host == broker.host) && (b.Port == broker.port) {
					f = true
				}
			}
			if f == false {
				fmt.Printf("Not matched broker (%v)\n", broker)
			}
		}

		// Create a context for use when calling some of these functions
		// This lets you set a variable timeout on invoking these calls
		// If the timeout passes then an error is returned.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// get ClusterID
		if c, err := a.ClusterID(ctx); err != nil {
			return fmt.Errorf("Error getting ClusterID\n\tError: %v\n", err)
		} else {
			fmt.Printf("ClusterID: %v\n", c)
		}

		// start the context time again
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)

		// get ControllerID
		if c, err := a.ControllerID(ctx); err != nil {
			return fmt.Errorf("Error getting ControllerID\n\tError: %v\n", err)
		} else {
			fmt.Printf("ControllerID: %v\n", c)
		}
	}
	return nil
}
