package mqtt_test

// Basic imports
import (
	"context"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/flowcore/go-mqtt-client"
	mochi "github.com/mochi-co/mqtt/v2"
	mochiPackets "github.com/mochi-co/mqtt/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"

	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/listeners"
)

// Define the suite, and absorb the built-in basic suite
// functionality from testify - including a T() method which
// returns the current testing context
type IntegrationTestSuite struct {
	suite.Suite
	mqttPort int
	server   *mochi.Server
	notifer  *notifier
}

func (suite *IntegrationTestSuite) SetupSuite() {
	suite.mqttPort = 4987
	suite.notifer = newNotifier()
	err := suite.startMqttServer()
	if err != nil {
		suite.FailNow("failed to start mqtt server for test", err.Error())
	}
}

func (suite *IntegrationTestSuite) startMqttServer() error {
	server := mochi.New(nil)
	err := server.AddHook(new(auth.Hook), &auth.Options{
		Ledger: &auth.Ledger{
			Auth: auth.AuthRules{ // Auth disallows all by default
				{Username: "test", Password: "test", Allow: true},
				{Username: "test2", Password: "test2", Allow: true},
			},
			ACL: auth.ACLRules{
				{
					Username: "test", Filters: auth.Filters{
						"#": auth.ReadWrite,
					},
				},
				{
					Username: "test2", Filters: auth.Filters{
						"#": auth.ReadWrite,
					},
				},
				{
					Filters: auth.Filters{
						"#": auth.Deny,
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}

	err = server.AddHook(suite.notifer, nil)
	if err != nil {
		return err
	}

	err = server.AddListener(listeners.NewTCP("t1", fmt.Sprintf(":%d", suite.mqttPort), nil))
	if err != nil {
		return err
	}

	suite.server = server

	return server.Serve()
}

func (suite *IntegrationTestSuite) TearDownSuite() {
	_ = suite.server.Close()
}

func (suite *IntegrationTestSuite) TestConnectWithBadUsername() {
	statusChan := make(chan mqtt.ConnectionStatus)
	client := suite.createClient(
		mqtt.WithPassword("bad"),
		mqtt.WithOnConnectionStatusHandler(func(client mqtt.Client, status mqtt.ConnectionStatus) {
			statusChan <- status
		}),
	)

	clientContext, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	clientReturnChan := make(chan error)
	go func() {
		clientReturnChan <- client.Run(clientContext)
	}()

	iteration := 0
	for {
		select {
		case status := <-statusChan:
			switch iteration {
			case 0:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 1:
				assert.Equal(suite.T(), mqtt.Disconnected, status.Code)
				assert.ErrorIs(suite.T(), status.Err, packets.ErrorRefusedNotAuthorised)
				return
			}
			iteration++
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
			return
		}
	}
}

func (suite *IntegrationTestSuite) TestConnectAndDisconnect() {
	statusChan := make(chan mqtt.ConnectionStatus)
	client := suite.createClient(mqtt.WithOnConnectionStatusHandler(func(client mqtt.Client, status mqtt.ConnectionStatus) {
		statusChan <- status
	}))

	clientContext, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	clientReturnChan := make(chan error, 1)
	go func() {
		clientReturnChan <- client.Run(clientContext)
	}()

	iteration := 0
	for {
		select {
		case err := <-clientReturnChan:
			assert.NoError(suite.T(), err)
			return
		case status := <-statusChan:
			switch iteration {
			case 0:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 1:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)
				cancelFunc()
			case 2:
				assert.Equal(suite.T(), mqtt.Stopped, status.Code)
			}
			iteration++
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
			return
		}
	}
}

func (suite *IntegrationTestSuite) TestClientReconnectsAfterServerDisconnect() {
	statusChan := make(chan mqtt.ConnectionStatus)
	client := suite.createClient(mqtt.WithOnConnectionStatusHandler(func(client mqtt.Client, status mqtt.ConnectionStatus) {
		statusChan <- status
	}))

	clientContext, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	clientReturnChan := make(chan error, 1)
	go func() {
		clientReturnChan <- client.Run(clientContext)
	}()

	iteration := 0
	for {
		select {
		case err := <-clientReturnChan:
			assert.NoError(suite.T(), err)
			return
		case status := <-statusChan:
			switch iteration {
			case 0:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 1:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)
				client, ok := suite.server.Clients.Get("test")
				require.True(suite.T(), ok, "could not find server client")
				_ = suite.server.DisconnectClient(client, mochiPackets.ErrServerUnavailable)
			case 2:
				assert.Equal(suite.T(), mqtt.Disconnected, status.Code)
			case 3:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 4:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)
				return
			}
			iteration++
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
			return
		}
	}
}

func (suite *IntegrationTestSuite) TestPublishQos0() {
	suite.withConnectedClient(func(client mqtt.Client) {
		packetProcessed := make(chan mqtt.Result[mochiPackets.Packet])
		suite.notifer.RegisterPacketProcessed(packetProcessed)
		defer suite.notifer.UnregisterPacketProcessed(packetProcessed)

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*4)
		defer cancelFunc()

		topic := "test/topic"
		payload := []byte("test payload")

		result := client.Publish(ctx, topic, payload, mqtt.QosLevel0)

		<-result.Done()
		assert.NoError(suite.T(), result.Error())

		select {
		case packet := <-packetProcessed:
			assert.Equal(suite.T(), topic, packet.Value().TopicName)
			assert.Equal(suite.T(), payload, packet.Value().Payload)
			assert.Equal(suite.T(), mqtt.QosLevel0, packet.Value().FixedHeader.Qos)
		case <-time.After(time.Second):
			suite.T().Fatal("timed out")
		}
	})
}

func (suite *IntegrationTestSuite) TestPublishQos1() {
	suite.withConnectedClient(func(client mqtt.Client) {
		packetProcessed := make(chan mqtt.Result[mochiPackets.Packet])
		suite.notifer.RegisterPacketProcessed(packetProcessed)
		defer suite.notifer.UnregisterPacketProcessed(packetProcessed)

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*4)
		defer cancelFunc()

		topic := "test/topic"
		payload := []byte("test payload")

		result := client.Publish(ctx, topic, payload, mqtt.QosLevel1)

		<-result.Done()
		assert.NoError(suite.T(), result.Error())

		select {
		case packet := <-packetProcessed:
			assert.Equal(suite.T(), topic, packet.Value().TopicName)
			assert.Equal(suite.T(), payload, packet.Value().Payload)
			assert.Equal(suite.T(), mqtt.QosLevel1, packet.Value().FixedHeader.Qos)
		case <-time.After(time.Second):
			suite.T().Fatal("timed out")
		}
	})
}

func (suite *IntegrationTestSuite) TestKeepalive() {
	suite.withConnectedClient(func(client mqtt.Client) {
		packetProcessed := make(chan mqtt.Result[mochiPackets.Packet])
		suite.notifer.RegisterPacketProcessed(packetProcessed)
		defer suite.notifer.UnregisterPacketProcessed(packetProcessed)

		select {
		case packet := <-packetProcessed:
			assert.Equal(suite.T(), mochiPackets.Pingreq, packet.Value().FixedHeader.Type)
		case <-time.After(time.Second):
			suite.T().Fatal("timed out")
		}
	}, mqtt.WithKeepAliveInterval(time.Millisecond*50))
}

func (suite *IntegrationTestSuite) TestSubscribeAndUnsubscribe() {
	suite.withConnectedClient(func(client mqtt.Client) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
		defer cancel()

		receivedPacket := make(chan packets.PublishPacket)
		fut := client.Subscribe(ctx, "test/subscribe", mqtt.QosLevel1, func(client mqtt.Client, packet packets.PublishPacket) error {
			receivedPacket <- packet
			return nil
		})
		<-fut.Done()
		require.NoError(suite.T(), fut.Error())

		expectedPayload := []byte("some payload")
		err := suite.server.Publish("test/subscribe", expectedPayload, false, mqtt.QosLevel1)
		require.NoError(suite.T(), err)

		select {
		case packet := <-receivedPacket:
			assert.Equal(suite.T(), "test/subscribe", packet.TopicName)
			assert.Equal(suite.T(), mqtt.QosLevel1, packet.Qos)
			assert.Equal(suite.T(), expectedPayload, packet.Payload)
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
		}

		unsubFut := client.Unsubscribe(ctx, "test/subscribe")
		<-unsubFut.Done()

		assert.NoError(suite.T(), unsubFut.Error())
	})
}

func (suite *IntegrationTestSuite) TestReconnect() {
	statusChan := make(chan mqtt.ConnectionStatus)
	client := suite.createClient(mqtt.WithOnConnectionStatusHandler(func(client mqtt.Client, status mqtt.ConnectionStatus) {
		statusChan <- status
	}))

	clientContext, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	clientReturnChan := make(chan error, 1)
	go func() {
		clientReturnChan <- client.Run(clientContext)
	}()

	iteration := 0
	for {
		select {
		case err := <-clientReturnChan:
			assert.NoError(suite.T(), err)
			return
		case status := <-statusChan:
			switch iteration {
			case 0:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 1:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)
				client.Reconnect()
			case 2:
				assert.Equal(suite.T(), mqtt.Disconnected, status.Code)
			case 3:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 4:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)

				c, ok := suite.server.Clients.Get("test")
				require.True(suite.T(), ok)
				assert.Equal(suite.T(), []byte("test"), c.Properties.Username)

				client.ReconnectWithCredentials("test2", "test2")
			case 5:
				assert.Equal(suite.T(), mqtt.Disconnected, status.Code)
			case 6:
				assert.Equal(suite.T(), mqtt.Connecting, status.Code)
			case 7:
				assert.Equal(suite.T(), mqtt.Connected, status.Code)

				c, ok := suite.server.Clients.Get("test")
				require.True(suite.T(), ok)
				assert.Equal(suite.T(), []byte("test2"), c.Properties.Username)

				return
			}
			iteration++
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
			return
		}
	}
}

func (suite *IntegrationTestSuite) createClient(opts ...mqtt.ClientOption) mqtt.Client {
	opts = append([]mqtt.ClientOption{
		mqtt.WithClientId("test"),
		mqtt.WithUsername("test"),
		mqtt.WithPassword("test"),
	}, opts...)
	client, err := mqtt.NewClient(fmt.Sprintf("tcp://localhost:%d", suite.mqttPort), opts...)
	require.NotNil(suite.T(), client)
	require.NoError(suite.T(), err)
	return client
}

type connectedClientCallback = func(client mqtt.Client)

func (suite *IntegrationTestSuite) withConnectedClient(callback connectedClientCallback, opts ...mqtt.ClientOption) {
	statusChan := make(chan mqtt.ConnectionStatus)

	opts = append([]mqtt.ClientOption{mqtt.WithOnConnectionStatusHandler(func(client mqtt.Client, status mqtt.ConnectionStatus) {
		statusChan <- status
	})}, opts...)
	client := suite.createClient(opts...)

	clientContext, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	clientReturnChan := make(chan error, 1)
	go func() {
		clientReturnChan <- client.Run(clientContext)
	}()

	for {
		select {
		case err := <-clientReturnChan:
			assert.NoError(suite.T(), err)
			return
		case status := <-statusChan:
			if status.Code == mqtt.Connected {
				callback(client)
				return
			}
		case <-time.After(time.Second * 4):
			suite.T().Fatal("timed out")
			return
		}
	}
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

type notifierResultChan = chan<- mqtt.Result[mochiPackets.Packet]

type notifier struct {
	mochi.HookBase
	channels []notifierResultChan
	mutex    sync.Mutex
}

func newNotifier() *notifier {
	return &notifier{channels: make([]notifierResultChan, 0)}
}

func (n *notifier) RegisterPacketProcessed(channel notifierResultChan) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.channels = append(n.channels, channel)
}

func (n *notifier) UnregisterPacketProcessed(channel notifierResultChan) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	channels := make([]notifierResultChan, 0)
	for _, ch := range n.channels {
		if ch != channel {
			channels = append(channels, ch)
		}
	}
	n.channels = channels
}

func (n *notifier) Provides(b byte) bool {
	return b == mochi.OnPacketProcessed
}

func (n *notifier) OnPacketProcessed(_ *mochi.Client, pk mochiPackets.Packet, err error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	for _, ch := range n.channels {
		go func(ch notifierResultChan) {
			ch <- mqtt.NewResult(pk, err)
		}(ch)
	}
}
