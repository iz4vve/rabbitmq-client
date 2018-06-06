package rabbit

/* rabbit contains all the functions necessary to connect, publish,
 * and subscribe to a RabbitMQ queue. Examples described below and
 * in the README.
 *
 * Author: Pietro Mascolo
 * Email: iz4vve@gmail.com
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE', which is part of this source code package.
 * If the LICENSE is not present, it can be retrieved at
 * http://www.apache.org/licenses/
 */

import (
	"fmt"

	logging "github.com/hhkbp2/go-logging"
	"github.com/iz4vve/logger"
	"github.com/streadway/amqp"
)

// Connector is the object that implements the amqp connection
// to the queue. It also contains an utility logger that can
// be used to report on the internal status or the operations.
//
//  Example usage:
//    connector := NewConnector()
//    connector.Dial('connection string')
//    connector.PublishOnQueue([]byte("I'm a message!"), "queue name", "")  // exchangeName can be omitted
//    // make sure you close the listening routing when subscribing
//    closeCh := make(chan bool)
//    defer func() { closeCh <- true }()
//    connector.SubscribeToQueue("queue name", "consumer name", callback, closeCh)
//
//
type Connector struct {
	conn   *amqp.Connection
	logger logging.Logger
}

func NewConnector() *Connector {
	conn := Connector{
		&amqp.Connection{},
		logger.GetCustomLogger("./log-config.yaml"),
	}

	return &conn
}

func (rabbit *Connector) Dial(connectionString string) {
	if connectionString == "" {
		panic("empty connection string")
	}

	var err error
	rabbit.conn, err = amqp.Dial(fmt.Sprintf("%s/", connectionString))
	if err != nil {
		panic("failed to connect: " + connectionString + ". " + err.Error())
	}
}

func (rabbit *Connector) PublishOnQueue(body []byte, queueName, exchangeName string) error {
	if rabbit.conn == nil {
		panic("Tried to send message before connection was initialized. Don't do that.")
	}
	ch, err := rabbit.conn.Channel()
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		queueName,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	// Publishes a message onto the queue.
	err = ch.Publish(
		exchangeName,
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	rabbit.logger.Debugf("message sent to queue %v: %v", queueName, body)
	return err
}

func (rabbit *Connector) SubscribeToQueue(
	qName, consumerName string, handlerFunc func(amqp.Delivery), closeCh chan bool,
) error {
	ch, err := rabbit.conn.Channel()
	failOnError(err, "Failed to open a channel")

	queue, err := ch.QueueDeclare(
		qName, // name of the queue
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to register an Queue")

	msgs, err := ch.Consume(
		queue.Name,   // queue
		consumerName, // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	go consumeLoop(msgs, handlerFunc, closeCh)
	return nil
}

func (rabbit *Connector) Close() {
	if rabbit.conn != nil {
		rabbit.conn.Close()
	}
}

func consumeLoop(
	deliveries <-chan amqp.Delivery, handlerFunc func(d amqp.Delivery), closeChan chan bool,
) {
	for {
		select {
		case d, ok := <-deliveries:
			if ok {
				handlerFunc(d)
			}
		case <-closeChan:
			return
		default:
		}
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
