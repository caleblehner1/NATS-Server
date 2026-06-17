package main

import (
	"fmt"
	"sync"
	"time"
)

type Message struct {
	ID        string
	Data      []byte
	Timestamp time.Time
}

type PendingMessage struct {
	Msg          *Message
	DeliveryTime time.Time
}

type Consumer struct {
	mu           sync.Mutex
	id           string
	ackWait      time.Duration
	pending      map[string]*PendingMessage
	isLeader     bool
	ackTimer     *time.Timer
	deliveries   chan *Message
	redeliveries chan *Message
}

func NewConsumer(id string, ackWait time.Duration) *Consumer {
	return &Consumer{
		id:           id,
		ackWait:      ackWait,
		pending:      make(map[string]*PendingMessage),
		deliveries:   make(chan *Message, 100),
		redeliveries: make(chan *Message, 100),
	}
}

func (c *Consumer) Deliver(msg *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pm := &PendingMessage{
		Msg:          msg,
		DeliveryTime: time.Now(),
	}
	c.pending[msg.ID] = pm

	if c.isLeader {
		c.resetAckTimer()
	}
}

func (c *Consumer) Ack(msgID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pending, msgID)
	if c.isLeader {
		c.resetAckTimer()
	}
}

func (c *Consumer) SetLeader(isLeader bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.isLeader = isLeader
	if isLeader {
		c.resetAckTimer()
	} else {
		if c.ackTimer != nil {
			c.ackTimer.Stop()
			c.ackTimer = nil
		}
	}
}

func (c *Consumer) resetAckTimer() {
	if c.ackTimer != nil {
		c.ackTimer.Stop()
		c.ackTimer = nil
	}

	if len(c.pending) == 0 {
		return
	}

	var nextExpire time.Time
	first := true
	for _, pm := range c.pending {
		expire := pm.DeliveryTime.Add(c.ackWait)
		if first || expire.Before(nextExpire) {
			nextExpire = expire
			first = false
		}
	}

	dur := time.Until(nextExpire)
	if dur < 0 {
		dur = 0
	}

	c.ackTimer = time.AfterFunc(dur, c.checkPending)
}

func (c *Consumer) checkPending() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isLeader {
		return
	}

	now := time.Now()
	var expired []*PendingMessage

	for id, pm := range c.pending {
		if now.After(pm.DeliveryTime.Add(c.ackWait)) {
			expired = append(expired, pm)
			delete(c.pending, id)
		}
	}

	for _, pm := range expired {
		select {
		case c.redeliveries <- pm.Msg:
		default:
		}
	}

	c.resetAckTimer()
}

func main() {
	fmt.Println("NATS JetStream Consumer Redelivery Simulation")
	c := NewConsumer("test-consumer", 2*time.Second)
	c.SetLeader(true)

	msg := &Message{ID: "msg-1", Data: []byte("hello")}
	c.Deliver(msg)

	fmt.Println("Delivered message, waiting for 1 second...")
	time.Sleep(1 * time.Second)

	fmt.Println("Simulating leader election (stepping down)...")
	c.SetLeader(false)

	fmt.Println("Waiting for 1.5 seconds (AckWait expires during election)...")
	time.Sleep(1.5 * time.Second)

	fmt.Println("Simulating new leader election (assuming leadership)...")
	c.SetLeader(true)

	select {
	case redelivered := <-c.redeliveries:
		fmt.Printf("Success! Message %s redelivered by the new leader.\n", redelivered.ID)
	case <-time.After(2 * time.Second):
		fmt.Println("Failed: Message was not redelivered.")
	}
}