package nats

import (
	"github.com/infobloxopen/atlas-pubsub"
	"github.com/nats-io/go-nats-streaming"
	"time"
	"context"
	"github.com/nats-io/go-nats-streaming/pb"
	"log"
	"os"
	"os/signal"
	"fmt"
)

// NewSubscriber creates a NATS message broker that will subscribe to
// the given topic with at-least-once message delivery semantics for the given
// subscriptionID
//var ytho2 = 0
func NewSubscriber(URL, clusterID, topic, subscriptionID string) (pubsub.Subscriber, error) {
	if clusterID == "" {
		clusterID = "test-cluster"
	}
	//subscriptionID += fmt.Sprintf("%v", ytho2)
	//ytho2--
	sc, err := stan.Connect(clusterID, subscriptionID, stan.NatsURL(URL),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			log.Fatalf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		log.Printf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	log.Printf("Connected to %s clusterID: [%s] clientID: [%s]\n", URL, clusterID, subscriptionID)

	return &subscriber{
		nats:           sc,
		topic:          topic,
		subscriptionID: subscriptionID,
	}, nil
}

type subscriber struct {
	nats stan.Conn
	topic string
	subscriptionID string
}

type natsMessage struct {
	ctx        context.Context
	messageID  string
	message    []byte
	metadata   map[string]string
	subscriber *subscriber
}

func (m natsMessage) MessageID() string {
	return m.messageID
}

func (m natsMessage) Message() []byte {
	return m.message
}

func (m natsMessage) Metadata() map[string]string {
	return m.metadata
}

func (m natsMessage) ExtendAckDeadline(time.Duration) error {
	panic("implement me")
}

func (m natsMessage) Ack() error {
	return m.subscriber.AckMessage(m.ctx, m.messageID)
}

func (s subscriber) Start(ctx context.Context, options ...pubsub.Option) (<-chan pubsub.Message, <-chan error) {
	messageChannel := make(chan pubsub.Message)
	errorChannel := make(chan error)

	mcb := func(msg *stan.Msg) {
		messageChannel <- &natsMessage{
			ctx:       ctx,
			messageID: "todo", // TODO
			message:   msg.Data,
			metadata:  nil, // TODO
		}
	}

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)

	//if startSeq != 0 {
	//	startOpt = stan.StartAtSequence(startSeq)
	//} else if deliverLast {
	//	startOpt = stan.StartWithLastReceived()
	//} else if deliverAll {
	//	log.Print("subscribing with DeliverAllAvailable")
	//	startOpt = stan.DeliverAllAvailable()
	//} else if startDelta != "" {
	//	ago, err := time.ParseDuration(startDelta)
	//	if err != nil {
	//		s.nats.Close()
	//		log.Fatal(err)
	//	}
	//	startOpt = stan.StartAtTimeDelta(ago)
	//}

	qgroup := s.subscriptionID
	clientID := s.subscriptionID
	durable := "" // Durable subscriber name
	unsubscribe := false // Unsubscribe the durable on exit

	sub, err := s.nats.QueueSubscribe(s.topic, qgroup, mcb, startOpt, stan.DurableName(durable))
	if err != nil {
		s.nats.Close()
		log.Fatal(err)
	}

	log.Printf("Listening on [%s], clientID=[%s], qgroup=[%s] durable=[%s]\n", s.topic, clientID, qgroup, durable)

	// Wait for a SIGINT (perhaps triggered by user with CTRL-C)
	// Run cleanup when signal is received
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			fmt.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			// Do not unsubscribe a durable on exit, except if asked to.
			if durable == "" || unsubscribe {
				sub.Unsubscribe()
			}
			s.nats.Close()
			cleanupDone <- true
		}
	}()
	return messageChannel, errorChannel
	//<-cleanupDone
}

func (subscriber) AckMessage(ctx context.Context, messageID string) error {
	return nil
	//panic("implement me")
}

func (subscriber) ExtendAckDeadline(ctx context.Context, messageID string, newDuration time.Duration) error {
	panic("implement me")
}
