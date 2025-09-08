package store

import (
	"sync"
)

type PubSubMessage struct {
	Channel string
	Message string
}

type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan PubSubMessage]struct{} // channel -> set of subscriber channels
}

func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string]map[chan PubSubMessage]struct{}),
	}
}

func (ps *PubSub) Subscribe(channels []string, out chan PubSubMessage) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for _, channel := range channels {
		if ps.subscribers[channel] == nil {
			ps.subscribers[channel] = make(map[chan PubSubMessage]struct{})
		}
		ps.subscribers[channel][out] = struct{}{}
	}
}

func (ps *PubSub) Unsubscribe(channels []string, out chan PubSubMessage) []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	var removed []string
	if len(channels) == 0 {
		// Unsubscribe from all channels
		for channel := range ps.subscribers {
			if _, ok := ps.subscribers[channel][out]; ok {
				delete(ps.subscribers[channel], out)
				removed = append(removed, channel)
				if len(ps.subscribers[channel]) == 0 {
					delete(ps.subscribers, channel)
				}
			}
		}
	} else {
		for _, channel := range channels {
			if _, ok := ps.subscribers[channel][out]; ok {
				delete(ps.subscribers[channel], out)
				removed = append(removed, channel)
				if len(ps.subscribers[channel]) == 0 {
					delete(ps.subscribers, channel)
				}
			}
		}
	}
	return removed
}

func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	count := 0
	msg := PubSubMessage{
		Channel: channel,
		Message: message,
	}
	for out := range ps.subscribers[channel] {
		select {
		case out <- msg:
			count++
		default:
			// If the subscriber's channel is full, we skip sending to it
		}
	}
	return count
}
