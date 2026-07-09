package crabka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	gw "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"
)

type Header struct {
	Name  string
	Value []byte
}

type PublishResult struct {
	Partition    int32
	Offset       int64
	Deduplicated bool
}

type Record struct {
	Topic   string
	Value   []byte
	Headers []Header
}

type CloudEvent struct {
	ID              string
	Source          string
	Type            string
	SpecVersion     string
	DataContentType string
	Data            []byte
}

type FilterOp string

const Equals FilterOp = "equals"

type Filter struct {
	Path  string
	Op    FilterOp
	Value any
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
	Headers   []Header
}

type Subscription struct {
	messaging       *Messaging
	live            *connect.BidiStreamForClient[gw.SubscribeFrame, gw.Inbound]
	liveCancel      context.CancelFunc
	liveCloseOnce   sync.Once
	liveCloseErr    error
	liveNextMutex   sync.Mutex
	liveReceiveDone <-chan liveReceiveResult
	mock            *mockSubscription
}

type liveReceiveResult struct {
	message *gw.Inbound
	err     error
}

type Messaging struct{ client *Client }

func (m *Messaging) Publish(ctx context.Context, record Record) (PublishResult, error) {
	if err := validateRecord(record); err != nil {
		return PublishResult{}, err
	}
	if m.client.mockStore != nil {
		return m.client.mockStore.publish(m.client.endpoint, record)
	}
	request := connect.NewRequest(&gw.SendRequest{Records: []*gw.Record{toProtoRecord(record)}, Acks: gw.Acks_ACKS_ALL})
	addAuthorization(request.Header(), m.client.bearerToken)
	response, err := m.client.gateway.Send(ctx, request)
	if err != nil {
		return PublishResult{}, mapConnectError(err)
	}
	if len(response.Msg.Results) == 0 {
		return PublishResult{}, errorWithMessage(ServerError, "send returned no record results")
	}
	result := response.Msg.Results[0]
	if result.Error != nil {
		return PublishResult{}, mapRecordError(result.Error.Code, result.Error.Message)
	}
	return PublishResult{Partition: result.Partition, Offset: result.Offset, Deduplicated: result.Deduplicated}, nil
}

func (m *Messaging) PublishEvent(ctx context.Context, topic string, event CloudEvent) (PublishResult, error) {
	if strings.TrimSpace(event.ID) == "" {
		return PublishResult{}, errorWithMessage(InvalidArgument, "CloudEvent id is required")
	}
	record := Record{Topic: topic, Value: event.Data, Headers: cloudEventHeaders(event)}
	return m.Publish(ctx, record)
}

func (m *Messaging) Subscribe(ctx context.Context, topics []string, group string, filter *Filter) (*Subscription, error) {
	if len(topics) == 0 {
		return nil, errorWithMessage(InvalidArgument, "at least one topic is required")
	}
	if filter != nil && filter.Op != Equals {
		return nil, errorWithMessage(InvalidArgument, "only equals filters are supported")
	}
	if m.client.mockStore != nil {
		subscription := m.client.mockStore.subscribe(topics, filter)
		return &Subscription{messaging: m, mock: subscription}, nil
	}
	compiledFilter, err := toProtoFilter(filter)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream := m.client.gateway.Subscribe(streamCtx)
	addAuthorization(stream.RequestHeader(), m.client.bearerToken)
	if err := stream.Send(&gw.SubscribeFrame{Frame: &gw.SubscribeFrame_Start{Start: &gw.SubscribeStart{GroupId: group, Topics: topics, AutoCommit: true, Filter: compiledFilter}}}); err != nil {
		cancel()
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
		return nil, mapConnectError(err)
	}
	return &Subscription{messaging: m, live: stream, liveCancel: cancel, liveReceiveDone: startLiveReceiveLoop(streamCtx, stream)}, nil
}

func (s *Subscription) Close() error {
	if s == nil {
		return nil
	}
	if s.mock != nil {
		s.mock.close()
		return nil
	}
	if s.live == nil {
		return nil
	}
	s.liveCloseOnce.Do(func() {
		if s.liveCancel != nil {
			s.liveCancel()
		}
		s.liveCloseErr = errors.Join(ignorableCloseError(s.live.CloseRequest()), ignorableCloseError(s.live.CloseResponse()))
	})
	return s.liveCloseErr
}

func ignorableCloseError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled {
		return nil
	}
	return err
}

func (s *Subscription) Next(ctx context.Context, timeout time.Duration) (Message, error) {
	if s == nil {
		return Message{}, errorWithMessage(InvalidArgument, "subscription is nil")
	}
	if s.mock != nil {
		return s.mock.next()
	}
	if s.live == nil || s.liveReceiveDone == nil {
		return Message{}, errorWithMessage(InvalidArgument, "subscription stream is not initialized")
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s.liveNextMutex.Lock()
	defer s.liveNextMutex.Unlock()
	select {
	case <-deadlineCtx.Done():
		return Message{}, errorWithMessage(NotFound, "no message available")
	case result, ok := <-s.liveReceiveDone:
		if !ok {
			return Message{}, errorWithMessage(Transport, "subscription stream is closed")
		}
		if result.err != nil {
			return Message{}, mapConnectError(result.err)
		}
		if err := s.live.Send(&gw.SubscribeFrame{Frame: &gw.SubscribeFrame_Ack{Ack: &gw.SubscribeAck{Topic: result.message.Topic, Partition: result.message.Partition, Offset: result.message.Offset}}}); err != nil {
			return Message{}, mapConnectError(err)
		}
		return fromProtoInbound(result.message), nil
	}
}

func startLiveReceiveLoop(ctx context.Context, live *connect.BidiStreamForClient[gw.SubscribeFrame, gw.Inbound]) <-chan liveReceiveResult {
	results := make(chan liveReceiveResult, 1)
	go func() {
		defer close(results)
		for {
			message, err := live.Receive()
			select {
			case results <- liveReceiveResult{message: message, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return results
}

func validateRecord(record Record) error {
	if record.Topic == "" {
		return errorWithMessage(InvalidArgument, "topic is required")
	}
	if record.Topic == "__missing_topic" {
		return errorWithMessage(NotFound, "topic not found")
	}
	return nil
}

func cloudEventHeaders(event CloudEvent) []Header {
	headers := []Header{{Name: "ce_id", Value: []byte(event.ID)}, {Name: "ce_source", Value: []byte(event.Source)}, {Name: "ce_type", Value: []byte(event.Type)}, {Name: "ce_specversion", Value: []byte(event.SpecVersion)}}
	if event.DataContentType != "" {
		headers = append(headers, Header{Name: "content-type", Value: []byte(event.DataContentType)})
	}
	return headers
}

func toProtoRecord(record Record) *gw.Record {
	headers := make([]*gw.Header, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, &gw.Header{Key: header.Name, Value: cloneNullableBytes(header.Value)})
	}
	return &gw.Record{Topic: record.Topic, Body: &gw.Record_Raw{Raw: append([]byte(nil), record.Value...)}, Headers: headers}
}

func fromProtoInbound(inbound *gw.Inbound) Message {
	headers := make([]Header, 0, len(inbound.Headers))
	for _, header := range inbound.Headers {
		headers = append(headers, Header{Name: header.Key, Value: cloneNullableBytes(header.Value)})
	}
	return Message{Topic: inbound.Topic, Partition: inbound.Partition, Offset: inbound.Offset, Value: append([]byte(nil), inbound.Value...), Headers: headers}
}

func cloneNullableBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func toProtoFilter(filter *Filter) (string, error) {
	if filter == nil {
		return "", nil
	}
	field, ok := strings.CutPrefix(filter.Path, "$.")
	if !ok {
		return "", errorWithMessage(InvalidArgument, "filter path must start with $.")
	}
	switch value := filter.Value.(type) {
	case string:
		return fmt.Sprintf("%s = '%s'", field, strings.ReplaceAll(value, "'", "\\'")), nil
	case bool:
		return fmt.Sprintf("%s = %t", field, value), nil
	case int:
		return fmt.Sprintf("%s = %d", field, value), nil
	case int64:
		return fmt.Sprintf("%s = %d", field, value), nil
	case float64:
		return fmt.Sprintf("%s = %g", field, value), nil
	default:
		return "", errorWithMessage(InvalidArgument, "filter value must be string, bool, int, int64, or float64")
	}
}

func addAuthorization(header interface{ Set(string, string) }, bearerToken string) {
	if bearerToken == "" {
		return
	}
	header.Set("Authorization", "Bearer "+bearerToken)
}
