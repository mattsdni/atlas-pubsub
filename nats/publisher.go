package nats

import (
	"github.com/infobloxopen/atlas-pubsub"
	"github.com/nats-io/go-nats-streaming"
	"context"
	"log"
	//"fmt"
)

// NewPublisher creates a new NATS message broker that will publish
// messages to the given topic.
//
// Topic names must be made up of only uppercase and lowercase
// ASCII letters, numbers, underscores, and hyphens, and must be between 1 and
// 247 characters long.
//var ytho = 0
func NewPublisher(URL, clusterID, clientID, topic string) (pubsub.Publisher, error) {
	if clusterID == "" {
		clusterID = "test-cluster"
	}
	if clientID == "" {
		clientID = "fooo"
	}
	//clientID += fmt.Sprintf("%v", ytho)
	//ytho++
	log.Println(clientID)
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL))
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		})
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	//defer sc.Close()


	return &publisher{
		nats:  sc,
		topic: topic,
	}, nil
}

type publisher struct {
	nats  stan.Conn
	topic string
}

func (p publisher) Publish(ctx context.Context, msg []byte, metadata map[string]string) error {
	defer p.nats.Close()
	return p.nats.Publish(p.topic, msg)
}
