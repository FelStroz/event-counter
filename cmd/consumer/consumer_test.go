package main

import (
	"context"
	"encoding/json"
	"fmt"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestConsumer_NewConsumer(t *testing.T) {
	consumer := NewConsumer()
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.Conn)
	assert.NotNil(t, consumer.Channel)
	assert.NotNil(t, consumer.Mt)
	assert.NotNil(t, consumer.Channels)
	assert.NotNil(t, consumer.Counters)
	assert.NotNil(t, consumer.CheckMessages)
}

func TestConsumer_SetConsumerConnection(t *testing.T) {
	consumer := NewConsumer()

	// Simulate successful connection
	conn := &amqp091.Connection{}
	channel := &amqp091.Channel{}
	consumer.Conn = conn
	consumer.Channel = channel

	err := consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	// Simulate connection error
	consumer.Conn = nil
	amqpUrl = ""
	err = consumer.SetConsumerConnection()
	assert.Error(t, err)

	// Simulate channel error
	consumer.Conn = conn
	consumer.Channel = nil
	err = consumer.SetConsumerConnection()
	assert.Error(t, err)
}

func TestConsumer_Declare(t *testing.T) {
	consumer := NewConsumer()

	err := consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	// Test ExchangeDeclare returns no error
	assert.NoError(t, consumer.Channel.ExchangeDeclare(
		amqpExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	))

	// Test QueueDeclare returns no error
	queue, err := consumer.Channel.QueueDeclare(
		QueueName,
		true,
		false,
		false,
		false,
		nil,
	)

	assert.NoError(t, err)
	assert.Equal(t, QueueName, queue.Name)

	// Test QueueBind returns no error
	assert.NoError(t, consumer.Channel.QueueBind(
		QueueName,
		Key,
		amqpExchange,
		false,
		nil,
	))

	// Test Declare returns no error
	assert.NoError(t, consumer.Declare())

	// Test Channel.ExchangeDeclare returns error
	amqpExchange = ""
	err = consumer.Declare()
	assert.Error(t, err)
}

func TestConsumer_PublishAndConsume(t *testing.T) {
	t.Skip("skipping test")

	conn, err := amqp091.Dial(amqpUrl)
	assert.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	assert.NoError(t, err)
	defer ch.Close()

	consumer := NewConsumer()

	err = consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	err = consumer.Declare()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)

		rountingKey := "user_a.event.created"
		err := ch.PublishWithContext(ctx,
			amqpExchange, // exchange
			rountingKey,  // routing key
			false,        // mandatory
			false,        // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf(`{"id":"%s"}`, "1")),
			})
		assert.NoError(t, err)

		time.Sleep(1 * time.Second)

		cancel()
	}()
	wg.Wait()

	// Act
	err = consumer.Consume(context.Background(), wg)

	// Assert
	assert.NoError(t, err)
}

func TestConsumer_ConsumeMessagesOnQueue(t *testing.T) {
	t.Skip("skipping test because consume the queue")
	// Arrange
	consumer := NewConsumer()

	err := consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	// Test ExchangeDeclare returns no error
	assert.NoError(t, consumer.Channel.ExchangeDeclare(
		amqpExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	))

	// Test QueueDeclare returns no error
	queue, err := consumer.Channel.QueueDeclare(
		QueueName,
		true,
		false,
		false,
		false,
		nil,
	)

	assert.NoError(t, err)
	assert.Equal(t, QueueName, queue.Name)

	// Test QueueBind returns no error
	assert.NoError(t, consumer.Channel.QueueBind(
		QueueName,
		Key,
		amqpExchange,
		false,
		nil,
	))

	// Test Declare returns no error
	assert.NoError(t, consumer.Declare())

	var wg sync.WaitGroup
	wg.Add(1)

	// Act
	err = consumer.Consume(context.Background(), &wg)

	// Assert
	assert.NoError(t, err)
}

func TestConsumer_SetupCloseHandler(t *testing.T) {
	consumer := NewConsumer()

	err := consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	// Defines a channel to receive signals from the system
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	// Executa a função em uma goroutine
	go consumer.SetupCloseHandler()

	//Run the function in a goroutine
	signals <- os.Interrupt

	// Waits for the SetupCloseHandler function to execute
	// The function should exit the program without errors
	assert.NoError(t, consumer.Shutdown())
}

func TestConsumer_Shutdown(t *testing.T) {
	consumer := NewConsumer()
	err := consumer.SetConsumerConnection()
	assert.NoError(t, err)
	assert.False(t, consumer.Channel.IsClosed())

	assert.Nil(t, consumer.Shutdown())

	assert.True(t, consumer.Channel.IsClosed())
}

func TestConsumer_Created(t *testing.T) {
	consumer := NewConsumer()
	msg := eventcounter.MessageConsumed{
		Id:        "1",
		EventType: eventcounter.EventType("created"),
		User:      "user_a",
	}

	ctx := context.WithValue(context.Background(), "msg", msg)
	err := consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	consumer.CheckMessages[msg.Id] = true
	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 1, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 1, consumer.Counters[msg.EventType][msg.User])
}

func TestConsumer_Deleted(t *testing.T) {
	consumer := NewConsumer()
	msg := eventcounter.MessageConsumed{
		Id:        "1",
		EventType: eventcounter.EventType("deleted"),
		User:      "user_a",
	}

	ctx := context.WithValue(context.Background(), "msg", msg)
	err := consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	consumer.CheckMessages[msg.Id] = true
	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 1, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 1, consumer.Counters[msg.EventType][msg.User])
}

func TestConsumer_Updated(t *testing.T) {
	consumer := NewConsumer()
	msg := eventcounter.MessageConsumed{
		Id:        "1",
		EventType: eventcounter.EventType("updated"),
		User:      "user_a",
	}

	ctx := context.WithValue(context.Background(), "msg", msg)
	err := consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 0, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 0, consumer.Counters[msg.EventType][msg.User])

	consumer.CheckMessages[msg.Id] = true
	err = consumer.Created(ctx, "1")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(consumer.Counters))
	assert.Equal(t, 1, len(consumer.Counters[msg.EventType]))
	assert.Equal(t, 1, consumer.Counters[msg.EventType][msg.User])
}

func TestCount_Write(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "example")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	counter := map[eventcounter.EventType]map[string]int{
		"event_type_1": {
			"user1": 10,
			"user2": 20,
		},
		"event_type_2": {
			"user3": 30,
			"user4": 40,
		},
	}

	err = Write(tmpDir, counter)
	assert.NoError(t, err)

	for k, v := range counter {
		filePath := fmt.Sprintf("%s/%s-consumer.json", tmpDir, k)

		_, err := os.Stat(filePath)
		assert.NoError(t, err)

		fileContent, err := ioutil.ReadFile(filePath)
		assert.NoError(t, err)

		var fileCounter map[string]int
		err = json.Unmarshal(fileContent, &fileCounter)
		assert.NoError(t, err)

		assert.Equal(t, v, fileCounter)
	}
}
