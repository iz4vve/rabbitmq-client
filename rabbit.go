package rabbit

import (
	"fmt"
	"log"

	logging "github.com/hhkbp2/go-logging"
	"github.com/iz4vve/logger"
	"github.com/streadway/amqp"
)

// Connector takes care of negotiating with the queue
type Connector struct {
	URL              string
	Port             string
	conn             *amqp.Connection
	queue            amqp.Queue
	subscribedTopics []string
	topics           []string
	logger           logging.Logger
}

// NewConnector generates a new Conector and sets up the logger
func NewConnector(url, port string) Connector {
	connector := Connector{}
	connector.logger = logger.GetCustomLogger("./log-config.yaml")
	connector.Port = port
	connector.URL = url
	return connector
}

// Connect dials the connection to the RabbitMQ instance
func (rabbit *Connector) Connect() {
	conn, err := amqp.Dial(rabbit.URL + ":" + rabbit.Port)
	failOnError(err, "could not connect to RabbitMQ")
	rabbit.conn = conn
}

// DeclareQueue declares a queue
func (rabbit *Connector) DeclareQueue(queueName string) {
	ch := getChan(rabbit.conn)
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "could not declare queue")
	rabbit.queue = q
}

//////////////////////////////////////////////////////////////////////////////
/////////////////////////////////  CONSUMING  ////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

// Bind binds topics to the queue and returns a slice of errors
func (rabbit *Connector) Bind() []error {

	var errors []error
	ch := getChan(rabbit.conn)

	for _, topic := range rabbit.topics {
		rabbit.logger.Infof("Binding to topic %s", topic)
		err := ch.QueueBind(
			rabbit.queue.Name, // queue name
			topic,             // routing key
			"logs_topic",      // exchange
			false,
			nil)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

// Subscribe adds a topic to the list of subscriptions
func (rabbit *Connector) Subscribe(topic string) {
	rabbit.topics = append(rabbit.topics, topic)
}

// Unsubscribe removes a topic to the list of subscriptions
func (rabbit *Connector) Unsubscribe(topic string) {
	var newTopics []string

	for _, oldTopic := range rabbit.topics {
		if oldTopic == topic {
			continue
		}
		newTopics = append(newTopics, topic)
	}

	rabbit.topics = newTopics
}

// Consume consumes topics
func (rabbit *Connector) Consume(outCh chan amqp.Delivery, closeChan chan bool) {
	ch := getChan(rabbit.conn)
	msgs, err := ch.Consume(
		rabbit.queue.Name, // queue
		"",                // consumer
		true,              // auto ack
		false,             // exclusive
		false,             // no local
		false,             // no wait
		nil,               // args
	)
	failOnError(err, "failed to register a consumer")

	// consume channel forever
	for {
		select {
		case msg := <-msgs:
			go func() {
				outCh <- msg
			}()
		case <-closeChan:
			return
		default:
		}
	}
}

//////////////////////////////////////////////////////////////////////////////
////////////////////////////////  PUBLISHING  ////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

// Publish publishes a message on a certain topic on a queue
func (rabbit *Connector) Publish(topicName, contentType string, payload []byte) error {
	ch := getChan(rabbit.conn)
	err := ch.ExchangeDeclare(
		topicName, // name
		"topic",   // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	err = ch.Publish(
		topicName,        // exchange
		"anonymous.info", // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        payload,
		})
	if err != nil {
		return err
	}
	return nil
}

//////////////////////////////////////////////////////////////////////////////
////////////////////////////////  UTILITIES  /////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

// Close closes a connection
func (rabbit *Connector) Close() {
	rabbit.conn.Close()
}

// failOnError reports errors and panics
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// getChan returns the channel of an open connection
func getChan(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "could not get connection channel")
	return ch
}
