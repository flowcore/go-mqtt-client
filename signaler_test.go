package mqtt

import (
	"context"
	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewSignaler(t *testing.T) {
	s := newSignaler()
	assert.NotNil(t, s)
}

func Test_signaler_SendAndReceive(t *testing.T) {
	s := newSignaler()

	connectPacket := packets.NewControlPacket(packets.Connect)

	resultChan := s.OnSend(context.Background(), connectPacket)

	connackPacket := packets.NewControlPacket(packets.Connack)

	err := s.OnReceive(connackPacket)
	assert.NoError(t, err)

	assertResult(t, resultChan, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Equal(t, connackPacket, packet)
		assert.NoError(t, err)
	})
}

func Test_signaler_SendContextCanceled(t *testing.T) {
	s := newSignaler()

	connectPacket := packets.NewControlPacket(packets.Connect)

	ctx, cancel := context.WithCancel(context.Background())
	resultChan := s.OnSend(ctx, connectPacket)
	cancel()

	assertResult(t, resultChan, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Nil(t, packet)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func Test_signaler_SendError(t *testing.T) {
	s := newSignaler()

	connectPacket := packets.NewControlPacket(packets.Disconnect)

	resultChan := s.OnSend(context.Background(), connectPacket)

	assertResult(t, resultChan, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Nil(t, packet)
		assert.ErrorIs(t, err, ErrUnsupportedPacketType)
	})
}

func Test_signaler_SendReplaced(t *testing.T) {
	s := newSignaler()

	connectPacket := packets.NewControlPacket(packets.Connect)
	resultChan1 := s.OnSend(context.Background(), connectPacket)
	resultChan2 := s.OnSend(context.Background(), connectPacket)

	connackPacket := packets.NewControlPacket(packets.Connack)
	err := s.OnReceive(connackPacket)
	assert.NoError(t, err)

	assertResult(t, resultChan1, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Nil(t, packet)
		assert.ErrorIs(t, err, ErrRequestReplaced)
	})
	assertResult(t, resultChan2, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Equal(t, connackPacket, packet)
		assert.NoError(t, err)
	})
}

func Test_signaler_ReceiveNoRequest(t *testing.T) {
	s := newSignaler()

	connackPacket := packets.NewControlPacket(packets.Connack)

	err := s.OnReceive(connackPacket)

	assert.ErrorIs(t, err, ErrNoRequestForResponse)
}

func Test_signaler_ReceiveNoMapping(t *testing.T) {
	s := newSignaler()

	connectPacket := packets.NewControlPacket(packets.Connect)

	err := s.OnReceive(connectPacket)

	assert.ErrorIs(t, err, ErrUnsupportedPacketType)
}

func Test_signaler_ReceiveOutOfOrder(t *testing.T) {
	s := newSignaler()

	publish10Packet := &packets.PublishPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Publish}, MessageID: 10}
	publishAck10Packet := &packets.PubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Puback}, MessageID: 10}
	publish11Packet := &packets.PublishPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Publish}, MessageID: 11}
	publishAck11Packet := &packets.PubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Puback}, MessageID: 11}

	publish10PacketResult := s.OnSend(context.Background(), publish10Packet)
	publish11PacketResult := s.OnSend(context.Background(), publish11Packet)

	err := s.OnReceive(publishAck11Packet)
	assert.NoError(t, err)

	assertResult(t, publish11PacketResult, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Equal(t, publishAck11Packet, packet)
		assert.NoError(t, err)
	})

	err = s.OnReceive(publishAck10Packet)
	assert.NoError(t, err)

	assertResult(t, publish10PacketResult, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Equal(t, publishAck10Packet, packet)
		assert.NoError(t, err)
	})
}

func Test_signaler_Flush(t *testing.T) {
	s := newSignaler()

	publish10Packet := packets.PublishPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Publish}, MessageID: 10}
	publish10PacketResult := s.OnSend(context.Background(), &publish10Packet)

	flushErr := errors.New("flushed")
	err := s.Flush(flushErr)
	assert.NoError(t, err)

	assertResult(t, publish10PacketResult, time.Second, func(packet packets.ControlPacket, err error) {
		assert.Nil(t, packet)
		assert.ErrorIs(t, err, flushErr)
	})

	si := s.(*signalerImpl)
	assert.Empty(t, si.requests)
}

func Test_signaler_FlushWithNilError(t *testing.T) {
	s := newSignaler()
	err := s.Flush(nil)

	assert.ErrorIs(t, err, ErrFlushWithNilError)
}

func Test_signaler_getPacketIdentifier(t *testing.T) {
	tests := []struct {
		name   string
		packet packets.ControlPacket
		result string
		err    error
	}{
		{
			name:   "connack",
			packet: packets.NewControlPacket(packets.Connack),
			result: "CONNACK/0",
		},
		{
			name:   "connect",
			packet: packets.NewControlPacket(packets.Connect),
			result: "CONNECT/0",
		},
		{
			name:   "disconnect",
			packet: packets.NewControlPacket(packets.Disconnect),
			err:    ErrUnsupportedPacketType,
		},
		{
			name:   "pingreq",
			packet: packets.NewControlPacket(packets.Pingreq),
			result: "PINGREQ/0",
		},
		{
			name:   "pingresp",
			packet: packets.NewControlPacket(packets.Pingresp),
			result: "PINGRESP/0",
		},
		{
			name:   "puback",
			packet: &packets.PubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Puback}, MessageID: 1},
			result: "PUBACK/1",
		},
		{
			name:   "pubcomp",
			packet: &packets.PubcompPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Pubcomp}, MessageID: 2},
			result: "PUBCOMP/2",
		},
		{
			name:   "publish",
			packet: &packets.PublishPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Publish}, MessageID: 3},
			result: "PUBLISH/3",
		},
		{
			name:   "pubrec",
			packet: &packets.PubrecPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Pubrec}, MessageID: 4},
			result: "PUBREC/4",
		},
		{
			name:   "pubrel",
			packet: &packets.PubrelPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Pubrel}, MessageID: 5},
			result: "PUBREL/5",
		},
		{
			name:   "suback",
			packet: &packets.SubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Suback}, MessageID: 6},
			result: "SUBACK/6",
		},
		{
			name:   "subscribe",
			packet: &packets.SubscribePacket{FixedHeader: packets.FixedHeader{MessageType: packets.Subscribe}, MessageID: 7},
			result: "SUBSCRIBE/7",
		},
		{
			name:   "unsuback",
			packet: &packets.UnsubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Unsuback}, MessageID: 8},
			result: "UNSUBACK/8",
		},
		{
			name:   "unsubscribe",
			packet: &packets.UnsubscribePacket{FixedHeader: packets.FixedHeader{MessageType: packets.Unsubscribe}, MessageID: 9},
			result: "UNSUBSCRIBE/9",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := signalerImpl{requests: map[string]request{}}

			got, err := s.getPacketIdentifier(tt.packet)
			if tt.err != nil {
				assert.Equal(t, tt.result, "")
				assert.ErrorIs(t, err, tt.err)
			} else {
				assert.Equal(t, tt.result, got)
				assert.NoError(t, err)
			}
		})
	}
}

func Test_signaler_getRequestPacketIdentifierForResponse(t *testing.T) {
	tests := []struct {
		name   string
		packet packets.ControlPacket
		result string
		err    error
	}{
		{
			name:   "connack -> connect",
			packet: packets.NewControlPacket(packets.Connack),
			result: "CONNECT/0",
		},
		{
			name:   "pingresp -> pingreq",
			packet: packets.NewControlPacket(packets.Pingresp),
			result: "PINGREQ/0",
		},
		{
			name:   "puback -> publish",
			packet: &packets.PubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Puback}, MessageID: 1},
			result: "PUBLISH/1",
		},
		{
			name:   "pubcomp -> pubrel",
			packet: &packets.PubcompPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Pubcomp}, MessageID: 1},
			result: "PUBREL/1",
		},
		{
			name:   "pubrec -> publish",
			packet: &packets.PubrecPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Pubrec}, MessageID: 1},
			result: "PUBLISH/1",
		},
		{
			name:   "suback -> subscribe",
			packet: &packets.SubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Suback}, MessageID: 1},
			result: "SUBSCRIBE/1",
		},
		{
			name:   "unsuback -> unsubscribe",
			packet: &packets.UnsubackPacket{FixedHeader: packets.FixedHeader{MessageType: packets.Unsuback}, MessageID: 1},
			result: "UNSUBSCRIBE/1",
		},
		{
			name:   "no mapping",
			packet: packets.NewControlPacket(packets.Disconnect),
			err:    ErrUnsupportedPacketType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := signalerImpl{requests: map[string]request{}}

			got, err := s.getRequestPacketIdentifierForResponse(tt.packet)
			if tt.err != nil {
				assert.Equal(t, tt.result, "")
				assert.ErrorIs(t, err, tt.err)
			} else {
				assert.Equal(t, tt.result, got)
				assert.NoError(t, err)
			}
		})
	}
}

func assertResult(t *testing.T, future Future[packets.ControlPacket], duration time.Duration, cb func(packet packets.ControlPacket, err error)) {
	select {
	case <-future.Done():
		cb(future.Value(), future.Error())
	case <-time.After(time.Second):
		t.Errorf("timeout: no result within %s", duration.String())
	}
}
