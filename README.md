# rabbitmq-client

**Pietro Mascolo**  
**iz4vve@gmail.com**

A lightweight client for publishing/subscribing on RabbitMQ.

#### Installation

```bash
$ go get -u -v github.com/iz4vve/rabbitmq-client
```

#### Examples

##### Connection

```go
connector := rabbit.NewConnector()
connString := "amqp://<user>:<password>@<host>:<port>"
connector.Dial(connString)
```

##### Publishing

```go
connector.PublishOnQueue(jsonPayload, qName, exchName)
```


##### Subscribing

```go
connector.SubscribeToQueue(qName, consumerName, callback, closeCh)
```


### Closing connections

Remember to close a connection when not needed anymore.
To do so, send a value to the close channel you used for the subscription, and call close on the connection.  
This is best setup when instantiating the connector.

```go
connector := rabbit.NewConnector()
defer connector.Close()

...

connector.SubscribeToQueue(qName, consumerName, callback, closeCh)
defer func() {closeCh <- true} ()
```