package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	amqp091 "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

var (
	conn    *amqp091.Connection
	channel *amqp091.Channel
)

func getChannel() (*amqp091.Channel, error) {
	var err error
	if channel != nil && !channel.IsClosed() {
		return channel, nil
	}

	conn, err = amqp091.Dial(amqpUrl)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	ch := make(chan *amqp091.Error, 2)
	channel.NotifyClose(ch)
	go func() {
		for {
			msg := <-ch
			log.Printf("channel closed, err: %s", msg)
		}
	}()

	if channel.IsClosed() {
		return nil, errors.New("channel closed")
	}

	return channel, nil
}

func Declare() error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if err := channel.ExchangeDeclare(amqpExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := channel.QueueDeclare("eventcountertest", true, false, false, false, nil); err != nil {
		return err
	}

	if err := channel.QueueBind("eventcountertest", "*.event.*", amqpExchange, false, nil); err != nil {
		return err
	}

	return nil
}

func Publish(ctx context.Context, msgs []*eventcounter.Message) error {
	channel, err := getChannel()
	if err != nil {
		return err
	}

	if channel.IsClosed() {
		return errors.New("channel is closed")
	}

	notifyPublish := make(chan amqp091.Confirmation, len(msgs))
	channel.Confirm(false)
	channel.NotifyPublish(notifyPublish)
	wait := make(chan any)
	count := 0
	go func() {
		for {
			_, ok := <-notifyPublish
			if !ok {
				wait <- errors.New("closed")
			}
			count++
			if count == len(msgs) {
				wait <- nil
				return
			}
		}
	}()

	for _, v := range msgs {
		routingKey := fmt.Sprintf("%s.event.%s", v.UserID, v.EventType)

		if err := channel.PublishWithContext(ctx, amqpExchange, routingKey, false, false, amqp091.Publishing{
			ContentEncoding: "application/json",
			Body:            []byte(fmt.Sprintf(`{"id":"%s"}`, v.UID)),
		}); err != nil {
			log.Printf("can`t publish message %s, err: %s", v.UID, err)
		}
	}

	publishedErr := <-wait
	if publishedErr != nil {
		return publishedErr.(error)
	}

	return nil
}
