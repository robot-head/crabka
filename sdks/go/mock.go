package crabka

import (
	"encoding/json"
	"strings"
)

type mockMessage struct {
	record        Record
	partition     int32
	offset        int64
	queueState    mockQueueMessageState
	deliveryCount int32
}

type mockQueueMessageState string

const (
	mockQueueMessageAvailable mockQueueMessageState = "available"
	mockQueueMessageAcquired  mockQueueMessageState = "acquired"
	mockQueueMessageAccepted  mockQueueMessageState = "accepted"
	mockQueueMessageRejected  mockQueueMessageState = "rejected"
)

type mockStore struct {
	messages           []mockMessage
	nextQueueSessionID int64
}

type mockSubscription struct {
	store     *mockStore
	topics    []string
	filter    *Filter
	nextIndex int
	closed    bool
}

func newMockStore() *mockStore {
	return &mockStore{nextQueueSessionID: 1}
}

func (s *mockStore) publish(endpoint string, record Record) (PublishResult, error) {
	if strings.HasPrefix(endpoint, "unreachable://") {
		return PublishResult{}, errorWithMessage(Transport, "endpoint unreachable")
	}
	offset := int64(0)
	for _, message := range s.messages {
		if message.record.Topic == record.Topic {
			offset++
		}
	}
	stored := mockMessage{record: cloneRecord(record), partition: 0, offset: offset, queueState: mockQueueMessageAvailable}
	s.messages = append(s.messages, stored)
	return PublishResult{Partition: 0, Offset: offset, Deduplicated: false}, nil
}

func (s *mockStore) subscribe(topics []string, filter *Filter) *mockSubscription {
	return &mockSubscription{store: s, topics: append([]string(nil), topics...), filter: cloneFilter(filter)}
}

func (s *mockSubscription) next() (Message, error) {
	if s.closed {
		return Message{}, errorWithMessage(Transport, "subscription is closed")
	}
	for s.nextIndex < len(s.store.messages) {
		message := s.store.messages[s.nextIndex]
		s.nextIndex++
		if !containsTopic(s.topics, message.record.Topic) {
			continue
		}
		if !mockFilterMatches(s.filter, message.record.Value) {
			continue
		}
		return Message{Topic: message.record.Topic, Partition: message.partition, Offset: message.offset, Value: append([]byte(nil), message.record.Value...), Headers: cloneHeaders(message.record.Headers)}, nil
	}
	return Message{}, errorWithMessage(NotFound, "no message available")
}

func (s *mockSubscription) close() {
	s.closed = true
}

func containsTopic(topics []string, topic string) bool {
	for _, candidate := range topics {
		if candidate == topic {
			return true
		}
	}
	return false
}

func mockFilterMatches(filter *Filter, value []byte) bool {
	if filter == nil {
		return true
	}
	if filter.Op != Equals {
		return false
	}
	field, ok := strings.CutPrefix(filter.Path, "$.")
	if !ok {
		return false
	}
	var decoded map[string]any
	if err := json.Unmarshal(value, &decoded); err != nil {
		return false
	}
	return decoded[field] == filter.Value
}

func cloneRecord(record Record) Record {
	return Record{Topic: record.Topic, Value: append([]byte(nil), record.Value...), Headers: cloneHeaders(record.Headers)}
}

func cloneHeaders(headers []Header) []Header {
	cloned := make([]Header, 0, len(headers))
	for _, header := range headers {
		cloned = append(cloned, Header{Name: header.Name, Value: append([]byte(nil), header.Value...)})
	}
	return cloned
}

func cloneFilter(filter *Filter) *Filter {
	if filter == nil {
		return nil
	}
	cloned := *filter
	return &cloned
}
