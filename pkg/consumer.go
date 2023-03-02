package eventcounter

import (
	"context"
	"math/rand"
	"time"
)

type Consumer interface {
	Created(ctx context.Context, uid string) error
	Updated(ctx context.Context, uid string) error
	Deleted(ctx context.Context, uid string) error
}

type ConsumerWrapper struct {
	Consumer Consumer
}

func (c *ConsumerWrapper) randomSleep() {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(999)))
}

func (c *ConsumerWrapper) Created(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Consumer.Created(ctx, uid)
}

func (c *ConsumerWrapper) Updated(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Consumer.Updated(ctx, uid)
}

func (c *ConsumerWrapper) Deleted(ctx context.Context, uid string) error {
	c.randomSleep()
	return c.Consumer.Deleted(ctx, uid)
}
