package mqtt

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type shutdownTestWriter func([]byte) (int, error)

func (w shutdownTestWriter) Write(p []byte) (int, error) { return w(p) }

func newShutdownTestClient(t *testing.T, opts ...ClientOption) *client {
	t.Helper()
	opts = append([]ClientOption{
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithDisconnectTimeout(25 * time.Millisecond),
	}, opts...)
	c, err := NewClient("tcp://test", opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c.(*client)
}

func TestWorkersExitWithoutErrorConsumer(t *testing.T) {
	for _, worker := range []string{"sender", "receiver", "keepaliver"} {
		t.Run(worker, func(t *testing.T) {
			c := newShutdownTestClient(t, WithKeepAliveInterval(time.Hour), WithKeepAliveTimeout(time.Millisecond))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var errChan <-chan error
			var doneChan <-chan any
			switch worker {
			case "sender":
				sendChan := make(chan packets.ControlPacket, 1)
				sendChan <- packets.NewControlPacket(packets.Pingreq)
				errChan, doneChan = c.sender(ctx, shutdownTestWriter(func([]byte) (int, error) {
					return 0, io.ErrClosedPipe
				}), sendChan)
			case "receiver":
				errChan, doneChan = c.receiver(ctx, bytes.NewReader(nil))
			case "keepaliver":
				// No sender consumes the ping, so its request times out.
				errChan, doneChan = c.keepaliver(ctx)
			}
			select {
			case <-doneChan:
			case <-time.After(time.Second):
				// Drain the error so a failing test also releases its worker.
				select {
				case <-errChan:
				case <-time.After(time.Second):
				}
				t.Fatal("worker could not exit without an error consumer")
			}
			if err := <-errChan; err == nil {
				t.Fatal("expected worker error")
			}
		})
	}
}

type shutdownTestFactory func(string) (net.Conn, error)

func (f shutdownTestFactory) Create(broker string) (net.Conn, error) { return f(broker) }

type shutdownTestConn struct {
	net.Conn
	writes       atomic.Int32
	writeStarted chan struct{}
	readFailure  chan struct{}
	readBytes    int
}

func (c *shutdownTestConn) Read(p []byte) (int, error) {
	// CONNACK is four bytes. After the handshake, allow a read failure
	// without closing the peer, leaving the sender blocked in Write.
	if c.readFailure != nil && c.readBytes >= 4 {
		<-c.readFailure
		return 0, io.EOF
	}
	n, err := c.Conn.Read(p)
	c.readBytes += n
	return n, err
}

func (c *shutdownTestConn) Write(p []byte) (int, error) {
	// The first write is CONNECT. The next write will block because the
	// simulated broker deliberately stops reading after CONNACK.
	if c.writes.Add(1) == 2 {
		close(c.writeStarted)
	}
	return c.Conn.Write(p)
}

func shutdownTestHandshake(conn net.Conn) error {
	if _, err := packets.ReadPacket(conn); err != nil {
		return err
	}
	return packets.NewControlPacket(packets.Connack).Write(conn)
}

func waitShutdownTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitShutdownTestFuture[T any](t *testing.T, future Future[T]) {
	t.Helper()
	select {
	case <-future.Done():
	case <-time.After(time.Second):
		t.Fatal("pending request was not completed")
	}
}

func TestClientShutdownWithBlockedWrite(t *testing.T) {
	for _, action := range []string{"cancel", "reconnect", "io failure", "read failure"} {
		t.Run(action, func(t *testing.T) {
			firstConn, firstPeer := net.Pipe()
			observed := &shutdownTestConn{Conn: firstConn, writeStarted: make(chan struct{})}
			readFailure := make(chan struct{})
			var failRead sync.Once
			if action == "read failure" {
				observed.readFailure = readFailure
			}
			peerRelease := make(chan struct{})
			go func() {
				_ = shutdownTestHandshake(firstPeer)
				<-peerRelease
			}()
			secondConn, secondPeer := net.Pipe()
			disconnected := make(chan struct{})
			go func() {
				if shutdownTestHandshake(secondPeer) != nil {
					return
				}
				for {
					packet, err := packets.ReadPacket(secondPeer)
					if err != nil {
						return
					}
					switch p := packet.(type) {
					case *packets.PublishPacket:
						ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
						ack.MessageID = p.MessageID
						if ack.Write(secondPeer) != nil {
							return
						}
					case *packets.DisconnectPacket:
						close(disconnected)
						return
					}
				}
			}()
			connected := make(chan struct{}, 2)
			attempt := 0
			c := newShutdownTestClient(t,
				WithConnectionFactory(shutdownTestFactory(func(string) (net.Conn, error) {
					attempt++
					if attempt == 1 {
						return observed, nil
					}
					return secondConn, nil
				})),
				WithOnConnectionStatusHandler(func(_ Client, status ConnectionStatus) {
					if status.Code == Connected {
						connected <- struct{}{}
					}
				}),
			)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(func() {
				cancel()
				failRead.Do(func() { close(readFailure) })
				_ = firstConn.Close()
				_ = firstPeer.Close()
				_ = secondConn.Close()
				_ = secondPeer.Close()
				close(peerRelease)
			})
			runDone := make(chan error, 1)
			go func() { runDone <- c.Run(ctx) }()
			waitShutdownTestSignal(t, connected, "first connection")
			pending := c.Publish(ctx, "test", []byte("blocked"), QosLevel1)
			waitShutdownTestSignal(t, observed.writeStarted, "blocked write")
			switch action {
			case "cancel":
				cancel()
			case "reconnect":
				c.Reconnect()
			case "io failure":
				// Closing the peer fails both its pending read and write.
				_ = firstPeer.Close()
			case "read failure":
				failRead.Do(func() { close(readFailure) })
			}
			waitShutdownTestFuture(t, pending)
			if pending.Error() == nil {
				t.Fatal("expected the interrupted publish to fail")
			}
			if action != "cancel" {
				waitShutdownTestSignal(t, connected, "reconnection")
				published := c.Publish(ctx, "test", []byte("recovered"), QosLevel1)
				waitShutdownTestFuture(t, published)
				if err := published.Error(); err != nil {
					t.Fatalf("publish after recovery failed: %v", err)
				}
				cancel()
			}
			select {
			case err := <-runDone:
				if err != nil {
					t.Fatalf("shutdown failed: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("client did not stop after cancellation")
			}
			if action != "cancel" {
				waitShutdownTestSignal(t, disconnected, "graceful DISCONNECT")
			}
		})
	}
}

func TestClientShutdownReleasesPendingSubscriptionHandler(t *testing.T) {
	conn, peer := net.Pipe()
	go func() {
		if shutdownTestHandshake(peer) == nil {
			_, _ = packets.ReadPacket(peer) // graceful DISCONNECT
		}
	}()
	connected := make(chan struct{}, 1)
	c := newShutdownTestClient(t,
		WithConnectionFactory(shutdownTestFactory(func(string) (net.Conn, error) { return conn, nil })),
		WithOnConnectionStatusHandler(func(_ Client, status ConnectionStatus) {
			if status.Code == Connected {
				connected <- struct{}{}
			}
		}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = conn.Close(); _ = peer.Close() })
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()
	waitShutdownTestSignal(t, connected, "connection")
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.MessageID = 1
	pending := c.signaler.OnSend(context.Background(), packet)
	handlerStarted := make(chan struct{})
	handlerDone := make(chan struct{})
	if err := c.subscriptionManager.Register("test", func(Client, packets.PublishPacket) error {
		close(handlerStarted)
		<-pending.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = c.subscriptionManager.OnReceive(c, packets.PublishPacket{TopicName: "test"})
		close(handlerDone)
	}()
	waitShutdownTestSignal(t, handlerStarted, "subscription handler")
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		// Release the handler if shutdown regresses, to avoid leaking it.
		_ = c.signaler.Flush(ErrConnectionStopped)
		t.Fatal("subscription cleanup blocked completion of pending requests")
	}
	waitShutdownTestSignal(t, handlerDone, "subscription handler exit")
	if !errors.Is(pending.Error(), ErrConnectionStopped) {
		t.Fatalf("expected stopped connection error, got %v", pending.Error())
	}
}
