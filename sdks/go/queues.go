package crabka

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	gw "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"
)

const (
	defaultQueueLockDurationMS int64 = 30_000
	queueMessageIDParts              = 3
	queueMessageNotAcquired          = "queue message is not acquired"
)

type QueueAckType string

const (
	QueueAckAccept  QueueAckType = "accept"
	QueueAckRelease QueueAckType = "release"
	QueueAckReject  QueueAckType = "reject"
)

type QueueAckEntry struct {
	MessageID string
	AckType   QueueAckType
}

type QueueRenewEntry struct {
	MessageID string
}

type QueueOperationError struct {
	Kind    ErrorKind
	Message string
}

type QueueResult struct {
	MessageID string
	Error     *QueueOperationError
}

type QueueMessage struct {
	MessageID     string
	Topic         string
	Partition     int32
	Offset        int64
	Value         []byte
	Headers       []Header
	DeliveryCount int32
}

type QueueAcquireResult struct {
	SessionID string
	Messages  []QueueMessage
}

type QueueBatchResult struct {
	Results []QueueResult
}

type Queues struct{ client *Client }

type parsedQueueMessageID struct {
	topic     string
	partition int32
	offset    int64
}

func (q *Queues) Acquire(ctx context.Context, topic string, group string, max uint32, lockDurationMS int64) (QueueAcquireResult, error) {
	if err := validateQueueAcquire(group, lockDurationMS); err != nil {
		return QueueAcquireResult{}, err
	}
	if strings.HasPrefix(q.client.endpoint, "unreachable://") {
		return QueueAcquireResult{}, errorWithMessage(Transport, "endpoint unreachable")
	}
	if q.client.mockStore != nil {
		return q.client.mockStore.acquireQueueMessages(topic, max), nil
	}
	request := connect.NewRequest(&gw.QueueAcquireRequest{GroupId: group, Topics: []string{topic}, MaxMessages: max, WaitMs: 0, LockDurationMs: uint64(defaultQueueLockDurationMS)})
	addAuthorization(request.Header(), q.client.bearerToken)
	response, err := q.client.gateway.QueueAcquire(ctx, request)
	if err != nil {
		return QueueAcquireResult{}, mapConnectError(err)
	}
	return fromProtoQueueAcquire(response.Msg), nil
}

func (q *Queues) Acknowledge(ctx context.Context, sessionID string, entries []QueueAckEntry) (QueueBatchResult, error) {
	if err := validateQueueSessionID(sessionID); err != nil {
		return QueueBatchResult{}, err
	}
	if q.client.mockStore != nil {
		return q.client.mockStore.acknowledgeQueueMessages(entries), nil
	}
	protoEntries, err := toProtoQueueAckEntries(entries)
	if err != nil {
		return QueueBatchResult{}, err
	}
	request := connect.NewRequest(&gw.QueueAcknowledgeRequest{SessionId: sessionID, Entries: protoEntries})
	addAuthorization(request.Header(), q.client.bearerToken)
	response, err := q.client.gateway.QueueAcknowledge(ctx, request)
	if err != nil {
		return QueueBatchResult{}, mapConnectError(err)
	}
	return fromProtoQueueBatch(entries, response.Msg.Results), nil
}

func (q *Queues) Renew(ctx context.Context, sessionID string, entries []QueueRenewEntry) (QueueBatchResult, error) {
	if err := validateQueueSessionID(sessionID); err != nil {
		return QueueBatchResult{}, err
	}
	if q.client.mockStore != nil {
		return q.client.mockStore.renewQueueMessages(entries), nil
	}
	protoEntries, err := toProtoQueueRenewEntries(entries)
	if err != nil {
		return QueueBatchResult{}, err
	}
	request := connect.NewRequest(&gw.QueueRenewRequest{SessionId: sessionID, Entries: protoEntries})
	addAuthorization(request.Header(), q.client.bearerToken)
	response, err := q.client.gateway.QueueRenew(ctx, request)
	if err != nil {
		return QueueBatchResult{}, mapConnectError(err)
	}
	return fromProtoQueueBatch(renewEntriesAsAckEntries(entries), response.Msg.Results), nil
}

func (q *Queues) Ack(ctx context.Context, messageID string) error {
	result, err := q.Acknowledge(ctx, "legacy-ack", []QueueAckEntry{{MessageID: messageID, AckType: QueueAckAccept}})
	if err != nil {
		return err
	}
	if len(result.Results) == 0 || result.Results[0].Error == nil {
		return nil
	}
	return errorWithMessage(result.Results[0].Error.Kind, result.Results[0].Error.Message)
}

func validateQueueAcquire(group string, lockDurationMS int64) error {
	if group == "" {
		return errorWithMessage(InvalidArgument, "queue group is required")
	}
	if lockDurationMS != 0 && lockDurationMS != defaultQueueLockDurationMS {
		return errorWithMessage(InvalidArgument, "queue lock_duration_ms must be 30000; per-acquire lock durations are not supported")
	}
	return nil
}

func validateQueueSessionID(sessionID string) error {
	if sessionID == "" {
		return errorWithMessage(InvalidArgument, "queue session_id is required")
	}
	return nil
}

func (s *mockStore) acquireQueueMessages(topic string, max uint32) QueueAcquireResult {
	sessionID := fmt.Sprintf("queue-session-%d", s.nextQueueSessionID)
	s.nextQueueSessionID++
	messages := make([]QueueMessage, 0, max)
	for index := range s.messages {
		if uint32(len(messages)) == max {
			break
		}
		message := &s.messages[index]
		if message.record.Topic != topic || message.queueState != mockQueueMessageAvailable {
			continue
		}
		messages = append(messages, acquireMockQueueMessage(message))
	}
	return QueueAcquireResult{SessionID: sessionID, Messages: messages}
}

func acquireMockQueueMessage(message *mockMessage) QueueMessage {
	message.queueState = mockQueueMessageAcquired
	message.deliveryCount++
	return QueueMessage{MessageID: queueMessageID(message.record.Topic, message.partition, message.offset), Topic: message.record.Topic, Partition: message.partition, Offset: message.offset, Value: append([]byte(nil), message.record.Value...), Headers: cloneHeaders(message.record.Headers), DeliveryCount: message.deliveryCount}
}

func (s *mockStore) acknowledgeQueueMessages(entries []QueueAckEntry) QueueBatchResult {
	results := make([]QueueResult, 0, len(entries))
	for _, entry := range entries {
		results = append(results, s.acknowledgeQueueMessage(entry))
	}
	return QueueBatchResult{Results: results}
}

func (s *mockStore) acknowledgeQueueMessage(entry QueueAckEntry) QueueResult {
	message := s.findAcquiredQueueMessage(entry.MessageID)
	if message == nil {
		return queueEntryError(entry.MessageID)
	}
	message.queueState = mockQueueStateForAck(entry.AckType)
	return QueueResult{MessageID: entry.MessageID}
}

func (s *mockStore) renewQueueMessages(entries []QueueRenewEntry) QueueBatchResult {
	results := make([]QueueResult, 0, len(entries))
	for _, entry := range entries {
		if s.findAcquiredQueueMessage(entry.MessageID) == nil {
			results = append(results, queueEntryError(entry.MessageID))
			continue
		}
		results = append(results, QueueResult{MessageID: entry.MessageID})
	}
	return QueueBatchResult{Results: results}
}

func (s *mockStore) findAcquiredQueueMessage(messageID string) *mockMessage {
	for index := range s.messages {
		message := &s.messages[index]
		if message.queueState == mockQueueMessageAcquired && queueMessageID(message.record.Topic, message.partition, message.offset) == messageID {
			return message
		}
	}
	return nil
}

func mockQueueStateForAck(ackType QueueAckType) mockQueueMessageState {
	if ackType == QueueAckRelease {
		return mockQueueMessageAvailable
	}
	if ackType == QueueAckReject {
		return mockQueueMessageRejected
	}
	return mockQueueMessageAccepted
}

func queueEntryError(messageID string) QueueResult {
	return QueueResult{MessageID: messageID, Error: &QueueOperationError{Kind: InvalidArgument, Message: queueMessageNotAcquired}}
}

func toProtoQueueAckEntries(entries []QueueAckEntry) ([]*gw.QueueAckEntry, error) {
	protoEntries := make([]*gw.QueueAckEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry, err := toProtoQueueAckEntry(entry)
		if err != nil {
			return nil, err
		}
		protoEntries = append(protoEntries, protoEntry)
	}
	return protoEntries, nil
}

func toProtoQueueRenewEntries(entries []QueueRenewEntry) ([]*gw.QueueAckEntry, error) {
	protoEntries := make([]*gw.QueueAckEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry, err := toProtoQueueAckEntry(QueueAckEntry{MessageID: entry.MessageID, AckType: QueueAckAccept})
		if err != nil {
			return nil, err
		}
		protoEntries = append(protoEntries, protoEntry)
	}
	return protoEntries, nil
}

func toProtoQueueAckEntry(entry QueueAckEntry) (*gw.QueueAckEntry, error) {
	messageID, err := parseQueueMessageID(entry.MessageID)
	if err != nil {
		return nil, err
	}
	return &gw.QueueAckEntry{Topic: messageID.topic, Partition: messageID.partition, Offset: messageID.offset, Type: toProtoQueueAckType(entry.AckType)}, nil
}

func toProtoQueueAckType(ackType QueueAckType) gw.QueueAckType {
	if ackType == QueueAckRelease {
		return gw.QueueAckType_RELEASE
	}
	if ackType == QueueAckReject {
		return gw.QueueAckType_REJECT
	}
	return gw.QueueAckType_ACCEPT
}

func parseQueueMessageID(messageID string) (parsedQueueMessageID, error) {
	parts := strings.Split(messageID, ":")
	if len(parts) != queueMessageIDParts || parts[0] == "" {
		return parsedQueueMessageID{}, invalidQueueMessageIDError()
	}
	partition, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return parsedQueueMessageID{}, invalidQueueMessageIDError()
	}
	offset, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return parsedQueueMessageID{}, invalidQueueMessageIDError()
	}
	return parsedQueueMessageID{topic: parts[0], partition: int32(partition), offset: offset}, nil
}

func invalidQueueMessageIDError() error {
	return errorWithMessage(InvalidArgument, "queue message_id must be <topic>:<partition>:<offset>")
}

func fromProtoQueueAcquire(response *gw.QueueAcquireResponse) QueueAcquireResult {
	messages := make([]QueueMessage, 0, len(response.Messages))
	for _, message := range response.Messages {
		messages = append(messages, fromProtoQueuedMessage(message))
	}
	return QueueAcquireResult{SessionID: response.SessionId, Messages: messages}
}

func fromProtoQueuedMessage(message *gw.QueuedMessage) QueueMessage {
	return QueueMessage{MessageID: queueMessageID(message.Topic, message.Partition, message.Offset), Topic: message.Topic, Partition: message.Partition, Offset: message.Offset, Value: append([]byte(nil), message.Value...), Headers: fromProtoHeaders(message.Headers), DeliveryCount: message.DeliveryCount}
}

func fromProtoHeaders(headers []*gw.Header) []Header {
	converted := make([]Header, 0, len(headers))
	for _, header := range headers {
		converted = append(converted, Header{Name: header.Key, Value: cloneNullableBytes(header.Value)})
	}
	return converted
}

func fromProtoQueueBatch(entries []QueueAckEntry, results []*gw.QueueAckResult) QueueBatchResult {
	converted := make([]QueueResult, 0, len(results))
	for index, result := range results {
		messageID := ""
		if index < len(entries) {
			messageID = entries[index].MessageID
		}
		converted = append(converted, fromProtoQueueResult(messageID, result))
	}
	return QueueBatchResult{Results: converted}
}

func fromProtoQueueResult(messageID string, result *gw.QueueAckResult) QueueResult {
	if result == nil || result.Error == nil {
		return QueueResult{MessageID: messageID}
	}
	return queueEntryError(messageID)
}

func renewEntriesAsAckEntries(entries []QueueRenewEntry) []QueueAckEntry {
	converted := make([]QueueAckEntry, 0, len(entries))
	for _, entry := range entries {
		converted = append(converted, QueueAckEntry{MessageID: entry.MessageID, AckType: QueueAckAccept})
	}
	return converted
}

func queueMessageID(topic string, partition int32, offset int64) string {
	return fmt.Sprintf("%s:%d:%d", topic, partition, offset)
}
