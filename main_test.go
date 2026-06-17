package main

import (
	"testing"
	"time"
)

func TestConsumerRedeliveryOnLeaderElection(t *testing.T) {
	c := NewConsumer("test-consumer", 2*time.Second)
	c.SetLeader(true)

	msg := &Message{ID: "msg-1", Data: []byte("hello")}
	c.Deliver(msg)

	time.Sleep(1 * time.Second)

	c.SetLeader(false)

	time.Sleep(1.5 * time.Second)

	c.SetLeader(true)

	select {
	case redelivered := <-c.redeliveries:
		if redelivered.ID != "msg-1" {
			t.Errorf("Expected redelivered message ID to be 'msg-1', got '%s'", redelivered.ID)
		}
	case <-time.After(2 * time.Second):
		t.Error("Expected message to be redelivered, but it was not")
	}
}

func TestNoDuplicateRedeliveryStorms(t *testing.T) {
	c := NewConsumer("test-consumer", 2*time.Second)
	c.SetLeader(true)

	msg := &Message{ID: "msg-1", Data: []byte("hello")}
	c.Deliver(msg)

	c.SetLeader(false)

	time.Sleep(500 * time.Millisecond)

	c.SetLeader(true)

	select {
	case <-c.redeliveries:
		t.Error("Message redelivered prematurely")
	case <-time.After(500 * time.Millisecond):
	}

	select {
	case redelivered := <-c.redeliveries:
		if redelivered.ID != "msg-1" {
			t.Errorf("Expected redelivered message ID to be 'msg-1', got '%s'", redelivered.ID)
		}
	case <-time.After(2 * time.Second):
		t.Error("Expected message to be redelivered after AckWait expired, but it was not")
	}
}