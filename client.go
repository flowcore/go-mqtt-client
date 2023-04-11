package mqtt

import (
	"context"
	"errors"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"golang.org/x/exp/slog"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAlreadyRunning = errors.New("client is already running")
var ErrEmptyBroker = errors.New("broker must be provided")
var ErrConnectionClosedForReconnect = errors.New("connection closed for reconnect")
var ErrConnectionStopped = errors.New("connection stopped")
var ErrNotConnack = errors.New("not connack packet")
var ErrSubackReturnedFailureCode = errors.New("suback returned failure")

type QosLevel = byte

const (
	QosLevel0 QosLevel = 0
	QosLevel1 QosLevel = 1
)

type ConnectionStatusCode = int

const (
	Stopped ConnectionStatusCode = iota
	Connecting
	Connected
	Disconnected
)

type ConnectionStatus struct {
	Code ConnectionStatusCode
	Err  error
}

type OnConnectionStatusHandler func(client Client, status ConnectionStatus)
type OnReceiveHandler func(client Client, packet packets.PublishPacket) error

type Client interface {
	Run(ctx context.Context) error
	Publish(ctx context.Context, topic string, payload []byte, qos QosLevel) Future[packets.ControlPacket]
	Subscribe(ctx context.Context, topic string, qos QosLevel, handler OnReceiveHandler) Future[packets.SubackPacket]
	Unsubscribe(ctx context.Context, topic string) Future[packets.UnsubackPacket]
	ReconnectWithCredentials(username string, password string)
	Reconnect()
	Status() ConnectionStatus
}

func NewClient(broker string, opts ...ClientOption) (Client, error) {
	client := &client{
		config: &config{broker: broker},
	}
	for _, opt := range opts {
		opt(client)
	}

	if client.config.logger == nil {
		client.config.logger = slog.Default()
	}
	client.logger = client.config.logger

	if client.config.broker == "" {
		return nil, ErrEmptyBroker
	}
	if client.config.clientId == "" {
		client.config.clientId = fmt.Sprintf("%s-%d", "go", time.Now().Unix())
	}
	if client.config.connectTimeout == 0 {
		client.config.connectTimeout = time.Second * 10
	}
	if client.config.connectBackoffMaxInterval == 0 {
		client.config.connectBackoffMaxInterval = time.Minute * 10
	}
	if client.config.disconnectTimeout == 0 {
		client.config.disconnectTimeout = time.Second * 5
	}
	if client.config.keepAliveTimeout == 0 {
		client.config.keepAliveTimeout = time.Second * 10
	}
	if client.config.connectionFactory == nil {
		client.config.connectionFactory = newDefaultFactory()
	}

	client.signaler = newSignaler()
	client.subscriptionManager = newSubscriptionManager()

	client.reconnectChan = make(chan any)
	client.sendChan = make(chan packets.ControlPacket)

	client.status = ConnectionStatus{
		Code: Stopped,
		Err:  nil,
	}

	return client, nil
}

type client struct {
	running atomic.Bool
	status  ConnectionStatus

	config *config
	logger *slog.Logger

	reconnectChan chan any
	sendChan      chan packets.ControlPacket

	signaler            signaler
	subscriptionManager subscriptionManager

	nextMessageId      uint16
	nextMessageIdMutex sync.Mutex
}

func (c *client) ReconnectWithCredentials(username string, password string) {
	c.config.username = username
	c.config.password = password
	c.Reconnect()
}

func (c *client) Reconnect() {
	select {
	case c.reconnectChan <- true:
	default:
	}
}

func (c *client) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		c.logger.Error(ErrAlreadyRunning.Error(), slogErr(ErrAlreadyRunning))
		return ErrAlreadyRunning
	}
	defer c.running.Store(false)
	return c.run(ctx)
}

func (c *client) Publish(ctx context.Context, topic string, payload []byte, qos QosLevel) Future[packets.ControlPacket] {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.Qos = qos
	packet.TopicName = topic
	packet.Payload = payload
	packet.MessageID = c.getNextMessageId()
	return c.send(ctx, packet, qos >= QosLevel1)
}

func (c *client) Subscribe(ctx context.Context, topic string, qos QosLevel, handler OnReceiveHandler) Future[packets.SubackPacket] {
	returnFuture := NewFuture[packets.SubackPacket]()

	packet := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	packet.Qos = QosLevel1
	packet.Qoss = []QosLevel{qos}
	packet.Topics = []string{topic}
	packet.MessageID = c.getNextMessageId()

	err := c.subscriptionManager.Register(topic, handler)
	if err != nil {
		return returnFuture.Complete(packets.SubackPacket{}, err)
	}

	l := c.logger.With(slog.String("topic", topic))
	l.Debug("subscribing")

	sendFuture := c.send(ctx, packet, true)
	go func() {
		<-sendFuture.Done()
		packet, ok := sendFuture.Value().(*packets.SubackPacket)
		err := sendFuture.Error()

		if ok && err == nil {
			for _, code := range packet.ReturnCodes {
				if code == SubackFailure {
					err = ErrSubackReturnedFailureCode
				}
			}
		}

		if err != nil {
			l.Error("subscription failed", slogErr(err))
			c.subscriptionManager.Unregister(topic)
			returnFuture.Complete(packets.SubackPacket{}, err)
			return
		}

		l.Info("subscribed")
		returnFuture.Complete(*packet, nil)

	}()
	return returnFuture
}

func (c *client) Unsubscribe(ctx context.Context, topic string) Future[packets.UnsubackPacket] {
	returnFuture := NewFuture[packets.UnsubackPacket]()

	packet := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	packet.Qos = QosLevel1
	packet.Topics = []string{topic}
	packet.MessageID = c.getNextMessageId()

	l := c.logger.With(slog.String("topic", topic))
	l.Debug("subscribing")

	sendFuture := c.send(ctx, packet, true)
	go func() {
		<-sendFuture.Done()
		packet, _ := sendFuture.Value().(*packets.UnsubackPacket)
		err := sendFuture.Error()

		if err != nil {
			l.Error("unsubscribe failed", slogErr(err))
			returnFuture.Complete(packets.UnsubackPacket{}, err)
			return
		}

		c.subscriptionManager.Unregister(topic)
		l.Info("unsubscribed")

		returnFuture.Complete(*packet, nil)
	}()
	return returnFuture
}

func (c *client) Status() ConnectionStatus {
	return c.status
}

func (c *client) connect() (net.Conn, error) {
	connectPacket, err := c.buildConnectPacket(c.config)
	if err != nil {
		c.logger.Error("failed to build CONNECT packet", slogErr(err))
		return nil, err
	}

	conn, err := c.config.connectionFactory.Create(c.config.broker)
	if err != nil {
		c.logger.Error("failed to create connection", slogErr(err))
		return nil, err
	}

	_ = conn.SetDeadline(time.Now().Add(c.config.connectTimeout))

	err = connectPacket.Write(conn)
	if err != nil {
		c.logger.Error("failed to write CONNECT packet", slogErr(err))
		_ = conn.Close()
		return nil, err
	}

	connackControlPacket, err := packets.ReadPacket(conn)
	if err != nil {
		c.logger.Error("failed to read CONNACK packet", slogErr(err))
		_ = conn.Close()
		return nil, err
	}

	connackPacket, ok := connackControlPacket.(*packets.ConnackPacket)
	if !ok {
		c.logger.Error("failed to read CONNACK packet", slogErr(ErrNotConnack))
		_ = conn.Close()
		return nil, ErrNotConnack
	}

	if connackPacket.ReturnCode != packets.Accepted {
		err := packets.ConnErrors[connackPacket.ReturnCode]
		c.logger.Error("server rejected connection", slogErr(err))
		_ = conn.Close()
		return nil, err
	}

	_ = conn.SetDeadline(time.Time{})

	return conn, nil
}

func (_ *client) buildConnectPacket(config *config) (*packets.ConnectPacket, error) {
	connectPacket := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connectPacket.ClientIdentifier = config.clientId

	if config.username != "" {
		connectPacket.Username = config.username
		connectPacket.UsernameFlag = true
	}
	if config.password != "" {
		connectPacket.Password = []byte(config.password)
		connectPacket.PasswordFlag = true
	}

	connectPacket.Keepalive = uint16(config.keepAliveInterval.Seconds())
	connectPacket.CleanSession = config.cleanSession
	connectPacket.ProtocolName = "MQTT"
	connectPacket.ProtocolVersion = 4

	errCode := connectPacket.Validate()
	if errCode != packets.Accepted {
		return nil, packets.ConnErrors[errCode]
	}

	return connectPacket, nil
}

func (_ *client) buildDisconnectPacket() *packets.DisconnectPacket {
	return packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
}

func (c *client) run(ctx context.Context) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = c.config.connectBackoffMaxInterval
	bo.Stop = c.config.connectBackoffMaxInterval
	bo.MaxElapsedTime = time.Duration(math.MaxInt64)
	bo.Reset()

Connect:
	for {
		// connect to the mqtt server
		c.logger.Debug("connecting")
		c.setConnectionStatus(Connecting, nil)
		conn, err := c.connect()
		if err != nil {
			c.setConnectionStatus(Disconnected, err)

			nextBackOff := bo.NextBackOff()
			c.logger.Debug("next connection attempt in", slog.Duration("backoff", nextBackOff))

			// backoff the next attempt
			select {
			case <-time.After(nextBackOff):
				continue Connect
			case <-ctx.Done():
				return nil
			}
		} else {
			bo.Reset() // reset the backoff counter
		}

		// start sending, receiving, and keepalive
		connectionContext, connectionContextCancel := context.WithCancel(ctx)
		senderErrChan, senderDoneChan := c.sender(connectionContext, conn, c.sendChan)
		receiverErrChan, receiverDoneChan := c.receiver(connectionContext, conn)
		keepaliverErrChan, keepaliverDoneChan := c.keepaliver(connectionContext)

		c.logger.Info("connection established")
		c.setConnectionStatus(Connected, nil)

		// wait for an error or context cancellation
		var sendReceiveReconnect = true
		var sendReceiveErr error
		select {
		case err := <-senderErrChan:
			c.logger.Warn("error while sending", slogErr(err))
			sendReceiveErr = err
		case err := <-receiverErrChan:
			c.logger.Warn("error while receiving", slogErr(err))
			sendReceiveErr = err
		case err := <-keepaliverErrChan:
			c.logger.Warn("keepalive error", slogErr(err))
			sendReceiveErr = err
		case <-c.reconnectChan:
			c.logger.Info("reconnect signal received")
		case <-ctx.Done():
			sendReceiveReconnect = false
		}

		c.logger.Debug("stopping")

		// shutdown sending, receiving, and keepalive
		connectionContextCancel()
		<-senderDoneChan
		<-receiverDoneChan
		<-keepaliverDoneChan

		// close the connection
		if conn != nil {
			_ = conn.SetWriteDeadline(time.Now().Add(c.config.disconnectTimeout))
			err := c.buildDisconnectPacket().Write(conn)
			if err != nil {
				c.logger.Debug("failed to send disconnect packet", slogErr(err))
			}
			err = conn.Close()
			if err != nil {
				c.logger.Debug("failed to close connection", slogErr(err))
			}
		}

		c.subscriptionManager.Flush()

		// update the connection status and flush the signaler
		if !sendReceiveReconnect {
			c.logger.Info("stopped")
			_ = c.signaler.Flush(ErrConnectionStopped)
			c.setConnectionStatus(Stopped, nil)
			return sendReceiveErr
		} else {
			c.logger.Info("disconnected")
			if sendReceiveErr != nil {
				_ = c.signaler.Flush(sendReceiveErr)
				c.setConnectionStatus(Disconnected, sendReceiveErr)
			} else {
				_ = c.signaler.Flush(ErrConnectionClosedForReconnect)
				c.setConnectionStatus(Disconnected, nil)
			}
		}
	}
}

func (c *client) getNextMessageId() uint16 {
	c.nextMessageIdMutex.Lock()
	defer c.nextMessageIdMutex.Unlock()
	c.nextMessageId++
	if c.nextMessageId == 0 {
		c.nextMessageId++
	}
	return c.nextMessageId
}

func (c *client) setConnectionStatus(code ConnectionStatusCode, err error) {
	status := ConnectionStatus{
		Code: code,
		Err:  err,
	}
	c.status = status
	if c.config.onConnectionStatusHandler != nil {
		go c.config.onConnectionStatusHandler(c, status)
	}
}

func (c *client) send(ctx context.Context, packet packets.ControlPacket, signal bool) Future[packets.ControlPacket] {
	if signal {
		f := c.signaler.OnSend(ctx, packet)
		select {
		case c.sendChan <- packet:
			return f
		case <-f.Done():
			return f
		}
	} else {
		f := NewFuture[packets.ControlPacket]()
		select {
		case c.sendChan <- packet:
			return f.Complete(nil, nil)
		case <-ctx.Done():
			return f.Complete(nil, ctx.Err())
		}
	}
}

func (c *client) sender(ctx context.Context, conn io.Writer, sendChan <-chan packets.ControlPacket) (<-chan error, <-chan any) {
	c.logger.Debug("sender started")

	errChan := make(chan error)
	doneChan := make(chan any)
	go func() {
		defer func() {
			close(doneChan)
			c.logger.Debug("sender stopped")
		}()
		for {
			select {
			case packet := <-sendChan:
				c.logger.Debug("writing packet", slog.String("packet", packet.String()))
				err := packet.Write(conn)
				if err != nil {
					errChan <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errChan, doneChan
}

func (c *client) receiver(ctx context.Context, conn io.Reader) (<-chan error, <-chan any) {
	c.logger.Debug("receiver started")

	errChan := make(chan error)
	doneChan := make(chan any)
	go func() {
		defer func() {
			close(doneChan)
			c.logger.Debug("receiver stopped")
		}()
		resultChan := make(chan Result[packets.ControlPacket])
		for {
			go func() {
				packet, err := packets.ReadPacket(conn)
				resultChan <- NewResult[packets.ControlPacket](packet, err)
			}()
			select {
			case result := <-resultChan:
				if result.Error() != nil {
					errChan <- result.Error()
					return
				}

				packet := result.Value()
				c.logger.Debug("received packet", slog.String("packet", packet.String()))

				switch p := packet.(type) {
				case *packets.PublishPacket:
					go func() {
						err := c.subscriptionManager.OnReceive(c, *p)
						if err != nil {
							c.logger.Warn("failed to handle received packet", slogErr(err), slog.String("packet", p.String()))
							return
						}
						if p.Qos >= QosLevel1 {
							c.ack(ctx, *p)
						}
					}()
				default:
					err := c.signaler.OnReceive(packet)
					if err != nil {
						c.logger.Warn("failed to signal receive", slogErr(err))
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errChan, doneChan
}

func (c *client) ack(ctx context.Context, p packets.PublishPacket) {
	puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	puback.MessageID = p.MessageID

	f := c.send(ctx, puback, false)
	<-f.Done()

	l := slog.With(slog.String("packet", p.String()))
	if f.Error() != nil {
		l.Warn("failed to send puback", slogErr(ctx.Err()))
	} else {
		l.Debug("sending puback")
	}
}

func (c *client) keepaliver(ctx context.Context) (<-chan error, <-chan any) {
	errChan := make(chan error)
	doneChan := make(chan any)

	if c.config.keepAliveInterval <= 0 {
		c.logger.Warn("keepaliver interval is <= 0, not sending keep alive packets")
		close(doneChan)
		return errChan, doneChan
	}

	c.logger.Debug("keepaliver started")

	go func() {
		defer func() {
			close(doneChan)
			c.logger.Debug("keepaliver stopped")
		}()

		firstRun := true
		ticker := time.NewTicker(c.config.keepAliveInterval)
		defer ticker.Stop()

		for {
			if !firstRun {
				select {
				case <-ticker.C:
					break
				case <-ctx.Done():
					return
				}
			}
			firstRun = false

			sendContext, cancel := context.WithDeadline(ctx, time.Now().Add(c.config.keepAliveTimeout))
			future := c.send(sendContext, packets.NewControlPacket(packets.Pingreq), true)

			done := false
			select {
			case <-future.Done():
				if future.Error() != nil {
					errChan <- future.Error()
					done = true
					break
				}
			case <-ctx.Done():
				done = true
			}

			cancel()
			if done {
				return
			}
		}
	}()
	return errChan, doneChan
}
