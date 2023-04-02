package mqtt

import (
	"context"
	"errors"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"reflect"
	"strconv"
	"sync"
)

var ErrUnsupportedPacketType = errors.New("unsupported packet type")
var ErrNoRequestForResponse = errors.New("no request for response")
var ErrRequestReplaced = errors.New("replaced by new request")
var ErrFlushWithNilError = errors.New("signaler must be closed with a non-nil error")

type signaler interface {
	OnSend(ctx context.Context, packet packets.ControlPacket) Future[packets.ControlPacket]
	OnReceive(packet packets.ControlPacket) error
	Flush(err error) error
}

func newSignaler() signaler {
	return &signalerImpl{
		requests: make(map[string]request),
		flusher:  NewFuture[any](),
	}
}

type signalerImpl struct {
	mutex    sync.Mutex
	requests map[string]request

	flusher Future[any]
}

type request struct {
	future       Future[packets.ControlPacket]
	responseChan chan Result[packets.ControlPacket]
}

func (s *signalerImpl) OnSend(ctx context.Context, packet packets.ControlPacket) Future[packets.ControlPacket] {
	future := NewFuture[packets.ControlPacket]()

	requestId, err := s.getPacketIdentifier(packet)
	if err != nil {
		return future.Complete(nil, err)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	req := request{
		future:       future,
		responseChan: make(chan Result[packets.ControlPacket], 1),
	}
	s.putRequest(requestId, req)

	go s.listenForResponseContext(ctx, requestId, req, s.flusher)

	return future
}

func (s *signalerImpl) OnReceive(packet packets.ControlPacket) error {
	id, err := s.getRequestPacketIdentifierForResponse(packet)
	if err != nil {
		return err
	}
	return s.withRequest(id, func(request request) {
		request.responseChan <- NewResult(packet, nil)
	})
}

func (s *signalerImpl) Flush(err error) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err == nil {
		return ErrFlushWithNilError
	}

	s.flusher.Complete(nil, err)
	s.flusher = NewFuture[any]()

	return nil
}

func (s *signalerImpl) getPacketIdentifier(packet packets.ControlPacket) (string, error) {
	switch p := packet.(type) {
	case *packets.ConnackPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, 0), nil
	case *packets.ConnectPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, 0), nil
	case *packets.PingreqPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, 0), nil
	case *packets.PingrespPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, 0), nil
	case *packets.PubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.PubcompPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.PublishPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.PubrecPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.PubrelPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.SubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.SubscribePacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.UnsubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	case *packets.UnsubscribePacket:
		return s.packageIdentifierFromTypeAndMessageId(p.MessageType, p.MessageID), nil
	}
	return "", fmt.Errorf("type %s: %w", reflect.TypeOf(packet).String(), ErrUnsupportedPacketType)
}

func (s *signalerImpl) getRequestPacketIdentifierForResponse(packet packets.ControlPacket) (string, error) {
	switch p := packet.(type) {
	case *packets.ConnackPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Connect, 0), nil
	case *packets.PingrespPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Pingreq, 0), nil
	case *packets.PubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Publish, p.MessageID), nil
	case *packets.PubcompPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Pubrel, p.MessageID), nil
	case *packets.PubrecPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Publish, p.MessageID), nil
	case *packets.SubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Subscribe, p.MessageID), nil
	case *packets.UnsubackPacket:
		return s.packageIdentifierFromTypeAndMessageId(packets.Unsubscribe, p.MessageID), nil
	}
	return "", fmt.Errorf("type %s does not have a response mapping: %w", reflect.TypeOf(packet).String(), ErrUnsupportedPacketType)
}

func (s *signalerImpl) packageIdentifierFromTypeAndMessageId(t byte, id uint16) string {
	return packets.PacketNames[t] + "/" + strconv.Itoa(int(id))
}

func (s *signalerImpl) listenForResponseContext(ctx context.Context, requestId string, request request, flusher Future[any]) {
	select {
	case result := <-request.responseChan:
		request.future.Complete(result.Value(), result.Error())
	case <-ctx.Done():
		request.future.Complete(nil, ctx.Err())
		s.deleteRequest(requestId, true)
	case <-flusher.Done():
		request.future.Complete(nil, flusher.Error())
		s.deleteRequest(requestId, true)
	}
}

func (s *signalerImpl) putRequest(id string, request request) {
	existingRequest, existing := s.requests[id]
	if existing {
		existingRequest.responseChan <- NewResult[packets.ControlPacket](nil, ErrRequestReplaced)
	}

	s.requests[id] = request
}

func (s *signalerImpl) deleteRequest(id string, lock bool) {
	if lock {
		s.mutex.Lock()
		defer s.mutex.Unlock()
	}
	delete(s.requests, id)
}

func (s *signalerImpl) withRequest(id string, callback func(request request)) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	request, ok := s.requests[id]
	if !ok {
		return ErrNoRequestForResponse
	}

	callback(request)

	s.deleteRequest(id, false)

	return nil
}
