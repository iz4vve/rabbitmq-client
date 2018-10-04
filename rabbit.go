// Package rabbit contains all the functions necessary to connect, publish,
// and subscribe to a RabbitMQ queue. Examples described below and
// in the README.
//
// Copyright (c) 2018 - Pietro Mascolo
//
// Author: Pietro Mascolo
// Email: iz4vve@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rabbit

import (
	"errors"
	"fmt"
	"log"
	"time"

	// logging "github.com/hhkbp2/go-logging"
	// "github.com/iz4vve/logger"

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
	conn *amqp.Connection
}

// NewConnector returns a default Connector.
// logger is set to a default configuration, on level Info
func NewConnector() *Connector {
	conn := Connector{
		&amqp.Connection{},
	}
	return &conn
}

// Dial connects the Connector to the RabbitMQ instance.
// It panics in case the connection cannot be stablished
//
// connectionString is in the form:
// amqp://<username>:<password>@<host>:<port>
//
// timeout is the timeout between subsequent attempts at reconnecting
// maxTries is the maximum number of times the client will try to reconnect
//
func (rabbit *Connector) Dial(connectionString string, timeout, maxTries int) error {
	if connectionString == "" {
		errStr := "empty connection string"
		return errors.New(errStr)
	}

	var err error
	attempts := 0
	for {
		rabbit.conn, err = amqp.Dial(fmt.Sprintf("%s/", connectionString))
		if err == nil {
			return nil
		}

		attempts++
		log.Printf("Connection attempt failed. Number of attemps: %d/%d\n", attempts, maxTries)
		if attempts > maxTries {
			log.Println("Max number of reconnects reached")
			return fmt.Errorf("failed to connect to %s after %d tries", connectionString, maxTries)
		}
		time.Sleep(time.Second * time.Duration(timeout))
	}
}

// PublishOnQueue publishes a message on a specific queue in the
// RabbitMQ instance exchangeName can be an empty string,
// in which case, the default exchange will be used
func (rabbit *Connector) PublishOnQueue(
	body []byte, queueName, exchangeName string, durable bool,
) error {

	if rabbit.conn == nil {
		errStr := "connection not initialised"
		panic(errStr)
	}
	ch, err := rabbit.conn.Channel()
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		queueName,
		durable, // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	err = ch.Publish(
		exchangeName,
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	return err
}

// SubscribeToQueue binds the client to a specific queue
// and starts a listener on that queue.
// Whenever a message is consumed on the queue,
// handlerFunc will be invoked on the message.
// closeCh is the channel that broadcasts a closing signal
// to the listening goroutine.
// MaxTO is the maximum time out that the subscription routine is allowed
// to await.
func (rabbit *Connector) SubscribeToQueue(
	qName, consumerName string, handlerFunc func(amqp.Delivery) error, closeCh chan bool, durable bool,
) error {
	ch, err := rabbit.conn.Channel()
	failOnError(err, "Failed to open a channel")

	queue, err := ch.QueueDeclare(
		qName,   // name of the queue
		durable, // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // noWait
		nil,     // arguments
	)
	failOnError(err, "Failed to register an Queue")

	msgs, err := ch.Consume(
		queue.Name,   // queue
		consumerName, // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	go rabbit.consumeLoop(msgs, handlerFunc, closeCh)
	return nil
}

// Close closes the connection
func (rabbit *Connector) Close() {
	if rabbit.conn != nil {
		rabbit.conn.Close()
	}
}

// consumeLoop is the listening function spawned by SubscribeToQueue
// it consumes messages on a specific topic until it receives the
// closing signal
func (rabbit *Connector) consumeLoop(
	deliveries <-chan amqp.Delivery, handlerFunc func(d amqp.Delivery) error, closeChan chan bool,
) {
	for {
		select {
		case d, ok := <-deliveries:
			if ok {
				err := handlerFunc(d)
				if err != nil {
					d.Reject(true)
				}
				d.Ack(true)
			}
		case <-closeChan:
			return
		default:
		}
	}
}

// failOnError panics in the presence of an unrecoverable error
// everything dies
func failOnError(err error, msg string) {
	if err != nil {
		errStr := fmt.Sprintf("%s: %s", msg, err)
		panic(errStr)
	}
}
