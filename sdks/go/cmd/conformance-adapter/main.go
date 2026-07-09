package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	crabka "github.com/robot-head/crabka/sdks/go"
)

const (
	contractMajor                   = 1
	contractMinor                   = 1
	gatewayQueueNotAcquiredMessage  = "record is not acquired by this session"
	contractQueueNotAcquiredMessage = "queue message is not acquired"
)

type command struct {
	Cmd            string       `json:"cmd"`
	Endpoint       string       `json:"endpoint"`
	Bearer         *string      `json:"bearer"`
	Topic          string       `json:"topic"`
	ValueB64       string       `json:"value_b64"`
	Headers        []header     `json:"headers"`
	Event          cloudEvent   `json:"event"`
	Topics         []string     `json:"topics"`
	Group          string       `json:"group"`
	Filter         *filter      `json:"filter"`
	TimeoutMS      uint64       `json:"timeout_ms"`
	Max            uint32       `json:"max"`
	LockDurationMS uint64       `json:"lock_duration_ms"`
	MessageID      string       `json:"message_id"`
	SessionID      string       `json:"session_id"`
	Entries        []queueEntry `json:"entries"`
	Name           string       `json:"name"`
	Username       string       `json:"username"`
	Password       string       `json:"password"`
	Key            string       `json:"key"`
}

type header struct {
	Name     string  `json:"name"`
	ValueB64 *string `json:"value_b64"`
}

type cloudEvent struct {
	ID              string  `json:"id"`
	Source          string  `json:"source"`
	Type            string  `json:"type"`
	SpecVersion     string  `json:"specversion"`
	DataContentType *string `json:"datacontenttype"`
	DataB64         string  `json:"data_b64"`
}

type filter struct {
	Path  string `json:"path"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

type queueEntry struct {
	MessageID string `json:"message_id"`
	AckType   string `json:"ack_type"`
}

type adapter struct {
	client              *crabka.Client
	subscription        *crabka.Subscription
	nextQueueSessionID  int64
	queueSessionAliases map[string]string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	adapter := &adapter{client: crabka.New("mock://gateway", nil), nextQueueSessionID: 1, queueSessionAliases: map[string]string{}}
	defer adapter.closeSubscription()
	scanner := bufio.NewScanner(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for scanner.Scan() {
		var command command
		if err := json.Unmarshal(scanner.Bytes(), &command); err != nil {
			return err
		}
		if err := encoder.Encode(adapter.handle(context.Background(), command)); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (a *adapter) handle(ctx context.Context, cmd command) any {
	switch cmd.Cmd {
	case "hello":
		return map[string]any{"hello": map[string]any{"contract_major": contractMajor, "contract_minor": contractMinor, "language": "go"}}
	case "configure":
		options := []crabka.Option{}
		if cmd.Bearer != nil {
			options = append(options, crabka.WithBearerToken(*cmd.Bearer))
		}
		a.closeSubscription()
		a.client = crabka.New(cmd.Endpoint, nil, options...)
		return ok(map[string]any{"bearer_configured": cmd.Bearer != nil})
	case "publish":
		value, err := base64.StdEncoding.DecodeString(cmd.ValueB64)
		if err != nil {
			return sdkError(crabka.InvalidArgument, "", "", err.Error())
		}
		result, err := a.client.Messaging().Publish(ctx, crabka.Record{Topic: cmd.Topic, Value: value, Headers: decodeHeaders(cmd.Headers)})
		return publishResponse(result, err)
	case "publish_event":
		value, err := base64.StdEncoding.DecodeString(cmd.Event.DataB64)
		if err != nil {
			return sdkError(crabka.InvalidArgument, "", "", err.Error())
		}
		event := crabka.CloudEvent{ID: cmd.Event.ID, Source: cmd.Event.Source, Type: cmd.Event.Type, SpecVersion: cmd.Event.SpecVersion, Data: value}
		if cmd.Event.DataContentType != nil {
			event.DataContentType = *cmd.Event.DataContentType
		}
		result, err := a.client.Messaging().PublishEvent(ctx, cmd.Topic, event)
		return publishResponse(result, err)
	case "subscribe":
		subscription, err := a.client.Messaging().Subscribe(ctx, cmd.Topics, cmd.Group, toSDKFilter(cmd.Filter))
		if err != nil {
			return errorResponse(err)
		}
		a.replaceSubscription(subscription)
		return ok(map[string]any{})
	case "next_message":
		if a.subscription == nil {
			return sdkError(crabka.InvalidArgument, "", "", "subscribe before next_message")
		}
		message, err := a.subscription.Next(ctx, time.Duration(cmd.TimeoutMS)*time.Millisecond)
		if err != nil {
			return errorResponse(err)
		}
		return map[string]any{"message": map[string]any{"topic": message.Topic, "partition": message.Partition, "offset": message.Offset, "value_b64": base64.StdEncoding.EncodeToString(message.Value), "headers": encodeHeaders(message.Headers)}}
	case "queue_acquire":
		result, err := a.client.Queues().Acquire(ctx, cmd.Topic, cmd.Group, cmd.Max, int64(cmd.LockDurationMS))
		return queueAcquireResponse(a.rememberQueueSession(result), err)
	case "queue_ack":
		return errorResponse(a.client.Queues().Ack(ctx, cmd.MessageID))
	case "queue_acknowledge":
		result, err := a.client.Queues().Acknowledge(ctx, a.actualQueueSessionID(cmd.SessionID), toQueueAckEntries(cmd.Entries))
		return queueBatchResponse(result, err)
	case "queue_renew":
		result, err := a.client.Queues().Renew(ctx, a.actualQueueSessionID(cmd.SessionID), toQueueRenewEntries(cmd.Entries))
		return queueBatchResponse(result, err)
	case "db_connect":
		return errorResponse(a.client.Database().Connect(ctx, cmd.Name))
	case "auth_sign_in":
		return errorResponse(a.client.Auth().SignIn(ctx, cmd.Username, cmd.Password))
	case "blob_put":
		return errorResponse(a.client.Blob().Put(ctx, cmd.Key, nil))
	case "blob_get":
		_, err := a.client.Blob().Get(ctx, cmd.Key)
		return errorResponse(err)
	default:
		return sdkError(crabka.InvalidArgument, "", "", "unknown command")
	}
}

func (a *adapter) rememberQueueSession(result crabka.QueueAcquireResult) crabka.QueueAcquireResult {
	if result.SessionID == "" {
		return result
	}
	publicSessionID := fmt.Sprintf("queue-session-%d", a.nextQueueSessionID)
	a.nextQueueSessionID++
	a.queueSessionAliases[publicSessionID] = result.SessionID
	result.SessionID = publicSessionID
	return result
}

func (a *adapter) actualQueueSessionID(publicSessionID string) string {
	actualSessionID, ok := a.queueSessionAliases[publicSessionID]
	if !ok {
		return publicSessionID
	}
	return actualSessionID
}

func (a *adapter) replaceSubscription(subscription *crabka.Subscription) {
	a.closeSubscription()
	a.subscription = subscription
}

func (a *adapter) closeSubscription() {
	if a.subscription == nil {
		return
	}
	_ = a.subscription.Close()
	a.subscription = nil
}

func publishResponse(result crabka.PublishResult, err error) any {
	if err != nil {
		return errorResponse(err)
	}
	return ok(map[string]any{"partition": result.Partition, "offset": result.Offset, "deduplicated": result.Deduplicated})
}

func queueAcquireResponse(result crabka.QueueAcquireResult, err error) any {
	if err != nil {
		return errorResponse(err)
	}
	messages := make([]map[string]any, 0, len(result.Messages))
	for _, message := range result.Messages {
		messages = append(messages, map[string]any{"message_id": message.MessageID, "topic": message.Topic, "partition": message.Partition, "offset": message.Offset, "value_b64": base64.StdEncoding.EncodeToString(message.Value), "headers": encodeHeaders(message.Headers), "delivery_count": message.DeliveryCount})
	}
	return ok(map[string]any{"session_id": result.SessionID, "messages": messages})
}

func queueBatchResponse(result crabka.QueueBatchResult, err error) any {
	if err != nil {
		return errorResponse(err)
	}
	encodedResults := make([]map[string]any, 0, len(result.Results))
	for _, queueResult := range result.Results {
		encodedResults = append(encodedResults, encodeQueueResult(queueResult))
	}
	return ok(map[string]any{"results": encodedResults})
}

func encodeQueueResult(result crabka.QueueResult) map[string]any {
	if result.Error == nil {
		return map[string]any{"message_id": result.MessageID, "error": nil}
	}
	return map[string]any{"message_id": result.MessageID, "error": map[string]any{"kind": string(result.Error.Kind), "message": contractQueueErrorMessage(result.Error.Message)}}
}

func contractQueueErrorMessage(message string) string {
	if message == gatewayQueueNotAcquiredMessage {
		return contractQueueNotAcquiredMessage
	}
	return message
}

func ok(value any) any { return map[string]any{"ok": value} }

func errorResponse(err error) any {
	if err == nil {
		return ok(map[string]any{})
	}
	if sdkErr, matches := err.(*crabka.SDKError); matches {
		return sdkError(sdkErr.Kind, sdkErr.Module, sdkErr.GatedOn, sdkErr.Message)
	}
	return sdkError(crabka.ServerError, "", "", err.Error())
}

func sdkError(kind crabka.ErrorKind, module string, gatedOn string, message string) any {
	body := map[string]any{"kind": string(kind)}
	if module != "" {
		body["module"] = module
	}
	if gatedOn != "" {
		body["gated_on"] = gatedOn
	}
	if message != "" {
		body["message"] = message
	}
	return map[string]any{"error": body}
}

func decodeHeaders(headers []header) []crabka.Header {
	decoded := make([]crabka.Header, 0, len(headers))
	for _, header := range headers {
		var value []byte
		if header.ValueB64 != nil {
			decodedValue, err := base64.StdEncoding.DecodeString(*header.ValueB64)
			if err == nil {
				value = decodedValue
			}
		}
		decoded = append(decoded, crabka.Header{Name: header.Name, Value: value})
	}
	return decoded
}

func encodeHeaders(headers []crabka.Header) []header {
	encoded := make([]header, 0, len(headers))
	for _, sdkHeader := range headers {
		var valueB64 *string
		if sdkHeader.Value != nil {
			encodedValue := base64.StdEncoding.EncodeToString(sdkHeader.Value)
			valueB64 = &encodedValue
		}
		encoded = append(encoded, header{Name: sdkHeader.Name, ValueB64: valueB64})
	}
	return encoded
}

func toSDKFilter(in *filter) *crabka.Filter {
	if in == nil {
		return nil
	}
	return &crabka.Filter{Path: in.Path, Op: crabka.Equals, Value: in.Value}
}

func toQueueAckEntries(entries []queueEntry) []crabka.QueueAckEntry {
	converted := make([]crabka.QueueAckEntry, 0, len(entries))
	for _, entry := range entries {
		ackType := crabka.QueueAckAccept
		if entry.AckType != "" {
			ackType = crabka.QueueAckType(entry.AckType)
		}
		converted = append(converted, crabka.QueueAckEntry{MessageID: entry.MessageID, AckType: ackType})
	}
	return converted
}

func toQueueRenewEntries(entries []queueEntry) []crabka.QueueRenewEntry {
	converted := make([]crabka.QueueRenewEntry, 0, len(entries))
	for _, entry := range entries {
		converted = append(converted, crabka.QueueRenewEntry{MessageID: entry.MessageID})
	}
	return converted
}
