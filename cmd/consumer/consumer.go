package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	eventcounter "github.com/reb-felipe/eventcounter/pkg"
)

const (
	QueueName string = "eventcountertest"
	Key       string = "*.event.*"
)

type Consumer struct {
	Conn          *amqp091.Connection
	Channel       *amqp091.Channel
	Mt            *sync.RWMutex
	Channels      map[eventcounter.EventType]chan eventcounter.MessageConsumed
	Counters      map[eventcounter.EventType]map[string]int
	CheckMessages map[string]bool
}

func NewConsumer() *Consumer {
	return &Consumer{
		Conn:          &amqp091.Connection{},
		Channel:       &amqp091.Channel{},
		Mt:            &sync.RWMutex{},
		Channels:      make(map[eventcounter.EventType]chan eventcounter.MessageConsumed),
		Counters:      make(map[eventcounter.EventType]map[string]int),
		CheckMessages: make(map[string]bool),
	}
}

func (c *Consumer) SetConsumerConnection() error {
	var err error
	c.Conn, err = amqp091.Dial(amqpUrl)
	if err != nil {
		return err
	}

	c.Channel, err = c.Conn.Channel()
	if err != nil {
		return err
	}

	go func() {
		log.Printf("Canal fechado, err: %s", <-c.Channel.NotifyClose(make(chan *amqp091.Error, 2)))
	}()

	if c.Channel.IsClosed() {
		return errors.New("Canal fechado")
	}

	return nil
}

func (c *Consumer) Declare() error {
	if err := c.Channel.ExchangeDeclare(amqpExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}

	log.Printf("Exchange declarada, declarando Queue %q", QueueName)

	queue, err := c.Channel.QueueDeclare(QueueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("Queue: (%q %d messages, %d consumers), Exchange sendo escolhida (key %q)", "eventcountertest", queue.Messages, queue.Consumers, Key)

	if err := c.Channel.QueueBind(QueueName, Key, amqpExchange, false, nil); err != nil {
		return err
	}

	log.Print("Queue da Exchange foi escolhida")

	return nil
}

func (c *Consumer) Consume(ctx context.Context, wg *sync.WaitGroup) error {
	if c.Channel.IsClosed() {
		return errors.New("Canal está fechado")
	}

	c.SetupCloseHandler()

	msgs, err := c.Channel.Consume(QueueName, "", true, false, false, false, nil)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	for {
		select {
		case <-ctx.Done():
			cancel()
			log.Print("Queue vazia por mais de 5 segundos! Retornando")
			if err := c.Shutdown(); err != nil {
				log.Printf("Erro ao encerrar o canal, err: %v", err)
			}
			return nil
		case msg := <-msgs:
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(lifetime))
			wg.Add(1)
			go c.handleMessage(msg, wg)
		default:
		}
	}
}

func (c *Consumer) handleMessage(msg amqp091.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	var message *eventcounter.MessageConsumed
	if err := json.Unmarshal(msg.Body, &message); err != nil {
		log.Printf("Falha ao decodificar a mensagem: %s", err)
		return
	}

	message.User = msg.RoutingKey[0:strings.Index(msg.RoutingKey, ".")]
	message.EventType = eventcounter.EventType(msg.RoutingKey[strings.LastIndex(msg.RoutingKey, ".")+1:])

	if ok := c.CheckMessages[message.Id]; !ok {
		// Se a mensagem não foi processada ainda, ela é enviada para o canal correspondente
		c.Mt.Lock()
		if _, ok := c.Channels[message.EventType]; !ok {
			// Criar um novo canal para este tipo de evento
			c.Channels[message.EventType] = make(chan eventcounter.MessageConsumed, 0)
		}

		ch := c.Channels[message.EventType]
		c.Mt.Unlock()

		ch <- *message
		c.Mt.Lock()
		c.CheckMessages[message.Id] = true
		c.Mt.Unlock()
		log.Printf("Evento %s emitido com o usuário %s", message.EventType, message.User)
	}
}

func (c *Consumer) SetupCloseHandler() {
	s := make(chan os.Signal, 2)
	signal.Notify(s, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-s
		log.Printf("Ctrl+C pressionado no Terminal")
		if err := c.Shutdown(); err != nil {
			log.Fatalf("Erro ao desligar: %s", err)
		}
		os.Exit(0)
	}()
}

func (c *Consumer) Shutdown() error {
	if err := c.Channel.Cancel("", true); err != nil {
		return fmt.Errorf("Cancelamento do consumer falhou: %s", err)
	}

	if err := c.Channel.Close(); err != nil {
		return fmt.Errorf("AMQP erro de conexão: %s", err)
	}

	log.Printf("AMQP desligando OK")

	// espera pelo handleMessage() sair
	return nil
}

// Métodos de contagem e controle descritos no pkg

func (c *Consumer) Created(ctx context.Context, uid string) error {
	msg := ctx.Value("msg").(eventcounter.MessageConsumed)
	c.Mt.Lock()
	defer c.Mt.Unlock()
	if _, ok := c.Counters[msg.EventType]; !ok {
		c.Counters[msg.EventType] = make(map[string]int)
	}
	if c.CheckMessages[uid] {
		c.Counters[msg.EventType][msg.User]++
	}
	return nil
}

func (c *Consumer) Updated(ctx context.Context, uid string) error {
	msg := ctx.Value("msg").(eventcounter.MessageConsumed)
	c.Mt.Lock()
	defer c.Mt.Unlock()
	if _, ok := c.Counters[msg.EventType]; !ok {
		c.Counters[msg.EventType] = make(map[string]int)
	}
	if c.CheckMessages[uid] {
		c.Counters[msg.EventType][msg.User]++
	}
	return nil
}

func (c *Consumer) Deleted(ctx context.Context, uid string) error {
	msg := ctx.Value("msg").(eventcounter.MessageConsumed)
	c.Mt.Lock()
	defer c.Mt.Unlock()
	if _, ok := c.Counters[msg.EventType]; !ok {
		c.Counters[msg.EventType] = make(map[string]int)
	}
	if c.CheckMessages[uid] {
		c.Counters[msg.EventType][msg.User]++
	}
	return nil
}
