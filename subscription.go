package mqtt

import (
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"regexp"
	"strings"
	"sync"
)

const (
	SubackSuccessQoS0 = 0
	SubackSuccessQoS1 = 1
	SubackSuccessQoS2 = 2
	SubackFailure     = 128
)

var ErrSubscriptionAlreadyRegistered = errors.New("subscription already registered")
var ErrNoSubscription = errors.New("no subscription matches packet topic name")

type subscriptionManager interface {
	Register(topic string, handler OnReceiveHandler) error
	Unregister(topic string)
	OnReceive(client Client, packet packets.PublishPacket) error
	Flush()
}

type matcher interface {
	Match(b []byte) bool
}

type subscriptionHandler struct {
	matcher matcher
	handler OnReceiveHandler
}

type subscriptionManagerImpl struct {
	mutex    sync.RWMutex
	handlers map[string]subscriptionHandler
}

func newSubscriptionManager() subscriptionManager {
	return &subscriptionManagerImpl{handlers: map[string]subscriptionHandler{}}
}

func (s *subscriptionManagerImpl) Register(topic string, handler OnReceiveHandler) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, existing := s.handlers[topic]
	if existing {
		return ErrSubscriptionAlreadyRegistered
	}

	regex, err := topicNameToRegex(topic)
	if err != nil {
		return err
	}

	s.handlers[topic] = subscriptionHandler{
		matcher: regex,
		handler: handler,
	}

	return nil
}

func (s *subscriptionManagerImpl) Unregister(topic string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.handlers, topic)
}

func (s *subscriptionManagerImpl) OnReceive(client Client, packet packets.PublishPacket) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, handler := range s.handlers {
		if handler.matcher.Match([]byte(packet.TopicName)) {
			return handler.handler(client, packet)
		}
	}
	return ErrNoSubscription
}

func (s *subscriptionManagerImpl) Flush() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.handlers = map[string]subscriptionHandler{}
}

func topicNameToRegex(topic string) (*regexp.Regexp, error) {
	topic = strings.ReplaceAll(topic, "+", "[^/]+")
	topic = strings.ReplaceAll(topic, "#", ".*")
	return regexp.Compile("^" + topic + "$")
}
