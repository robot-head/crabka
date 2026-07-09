package crabka

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	gw "github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1"
	"github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1/gatewayv1connect"
)

func TestCloudEventBinaryHeaders(t *testing.T) {
	client := New("mock://gateway", nil)
	_, err := client.Messaging().PublishEvent(context.Background(), "events", CloudEvent{ID: "evt-1", Source: "/orders", Type: "order.created", SpecVersion: "1.0", DataContentType: "application/json", Data: []byte(`{"n":7}`)})
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
	subscription, err := client.Messaging().Subscribe(context.Background(), []string{"events"}, "reader", nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	message, err := subscription.Next(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	expected := []Header{{Name: "ce_id", Value: []byte("evt-1")}, {Name: "ce_source", Value: []byte("/orders")}, {Name: "ce_type", Value: []byte("order.created")}, {Name: "ce_specversion", Value: []byte("1.0")}, {Name: "content-type", Value: []byte("application/json")}}
	if !headersEqual(message.Headers, expected) {
		t.Fatalf("headers = %#v, want %#v", message.Headers, expected)
	}
	for _, header := range message.Headers {
		if header.Name == "ce_datacontenttype" {
			t.Fatal("CloudEvents binary mode must not emit ce_datacontenttype")
		}
	}
}

func TestTaxonomyAndStubs(t *testing.T) {
	client := New("mock://gateway", nil, WithBearerToken("dev-token"))
	if client.Auth().BearerToken() != "dev-token" {
		t.Fatal("bearer token was not configured")
	}
	assertSDKError(t, client.Database().Connect(context.Background(), "orders"), Unimplemented, "database", "chapter-f-control-plane")
	assertSDKError(t, client.Auth().SignIn(context.Background(), "u", "p"), Unauthenticated, "", "")
	assertSDKError(t, client.Blob().Put(context.Background(), "avatar.png", []byte("image")), Unimplemented, "blob", "chapter-b-blob-api")
	_, err := New("unreachable://gateway", nil).Messaging().Publish(context.Background(), Record{Topic: "t", Value: []byte("x")})
	assertSDKError(t, err, Transport, "", "")
}

func TestMockQueueAcquireAcknowledgeAndRenew(t *testing.T) {
	client := New("mock://gateway", nil)
	if _, err := client.Messaging().Publish(context.Background(), Record{Topic: "queue", Value: []byte("first"), Headers: []Header{{Name: "kind", Value: []byte("queue")}}}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if _, err := client.Messaging().Publish(context.Background(), Record{Topic: "queue", Value: []byte("second")}); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	acquired, err := client.Queues().Acquire(context.Background(), "queue", "workers", 1, 30_000)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	expectedMessage := QueueMessage{MessageID: "queue:0:0", Topic: "queue", Partition: 0, Offset: 0, Value: []byte("first"), Headers: []Header{{Name: "kind", Value: []byte("queue")}}, DeliveryCount: 1}
	if acquired.SessionID != "queue-session-1" || len(acquired.Messages) != 1 || !queueMessagesEqual(acquired.Messages[0], expectedMessage) {
		t.Fatalf("acquired = %#v, want session queue-session-1 and %#v", acquired, expectedMessage)
	}

	renewed, err := client.Queues().Renew(context.Background(), acquired.SessionID, []QueueRenewEntry{{MessageID: "queue:0:0"}, {MessageID: "missing:0:0"}})
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	assertQueueResults(t, renewed.Results, []QueueResult{{MessageID: "queue:0:0"}, queueEntryError("missing:0:0")})

	acknowledged, err := client.Queues().Acknowledge(context.Background(), acquired.SessionID, []QueueAckEntry{{MessageID: "queue:0:0", AckType: QueueAckRelease}})
	if err != nil {
		t.Fatalf("acknowledge release: %v", err)
	}
	assertQueueResults(t, acknowledged.Results, []QueueResult{{MessageID: "queue:0:0"}})

	redelivered, err := client.Queues().Acquire(context.Background(), "queue", "workers", 1, 30_000)
	if err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	if redelivered.SessionID != "queue-session-2" || len(redelivered.Messages) != 1 || redelivered.Messages[0].MessageID != "queue:0:0" || redelivered.Messages[0].DeliveryCount != 2 {
		t.Fatalf("redelivered = %#v", redelivered)
	}
}

func TestQueueValidationErrors(t *testing.T) {
	client := New("mock://gateway", nil)
	_, err := client.Queues().Acquire(context.Background(), "queue", "", 1, 30_000)
	assertSDKError(t, err, InvalidArgument, "", "")
	_, err = client.Queues().Acquire(context.Background(), "queue", "workers", 1, 1_000)
	assertSDKError(t, err, InvalidArgument, "", "")
	_, err = client.Queues().Acknowledge(context.Background(), "", nil)
	assertSDKError(t, err, InvalidArgument, "", "")
}

func TestMockPublishSubscribeFilter(t *testing.T) {
	client := New("mock://gateway", nil)
	if _, err := client.Messaging().Publish(context.Background(), Record{Topic: "filtered", Value: []byte(`{"kind":"skip"}`)}); err != nil {
		t.Fatalf("publish skip: %v", err)
	}
	result, err := client.Messaging().Publish(context.Background(), Record{Topic: "filtered", Value: []byte(`{"kind":"keep"}`)})
	if err != nil {
		t.Fatalf("publish keep: %v", err)
	}
	if result.Offset != 1 || result.Partition != 0 || result.Deduplicated {
		t.Fatalf("result = %#v", result)
	}
	subscription, err := client.Messaging().Subscribe(context.Background(), []string{"filtered"}, "reader", &Filter{Path: "$.kind", Op: Equals, Value: "keep"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	message, err := subscription.Next(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if message.Offset != 1 || string(message.Value) != `{"kind":"keep"}` {
		t.Fatalf("message = %#v", message)
	}
}

func TestPublishHeadersPreserveOrderDuplicatesAndNulls(t *testing.T) {
	record := Record{Topic: "headers", Value: []byte("hi"), Headers: []Header{{Name: "x", Value: []byte("first")}, {Name: "x", Value: nil}, {Name: "y", Value: []byte("last")}}}
	protoRecord := toProtoRecord(record)
	if len(protoRecord.Headers) != 3 {
		t.Fatalf("headers length = %d, want 3", len(protoRecord.Headers))
	}
	if protoRecord.Headers[0].Key != "x" || string(protoRecord.Headers[0].Value) != "first" {
		t.Fatalf("first header = %#v", protoRecord.Headers[0])
	}
	if protoRecord.Headers[1].Key != "x" || protoRecord.Headers[1].Value != nil {
		t.Fatalf("second header = %#v", protoRecord.Headers[1])
	}
	if protoRecord.Headers[2].Key != "y" || string(protoRecord.Headers[2].Value) != "last" {
		t.Fatalf("third header = %#v", protoRecord.Headers[2])
	}
}

func TestNewPreservesCallerSuppliedHTTPClient(t *testing.T) {
	client := New("http://gateway", http.DefaultClient)
	if client.httpClient != http.DefaultClient {
		t.Fatal("New replaced caller-supplied http.DefaultClient")
	}
}

func TestNewSelectsPlaintextHTTP2ClientForNilHTTPClient(t *testing.T) {
	client := New("http://gateway", nil)
	if client.httpClient == nil {
		t.Fatal("New returned nil HTTP client")
	}
	if client.httpClient == http.DefaultClient {
		t.Fatal("New should use plaintext HTTP/2 transport for nil client and http endpoint")
	}
}

func TestLiveSubscriptionCloseCancelsStreamContext(t *testing.T) {
	gateway := &subscriptionCloseGateway{started: make(chan struct{}), canceled: make(chan struct{})}
	_, handler := gatewayv1connect.NewGatewayHandler(gateway)
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	client := New(server.URL, server.Client())
	subscription, err := client.Messaging().Subscribe(context.Background(), []string{"live"}, "reader", nil)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-gateway.started:
	case <-time.After(time.Second):
		t.Fatal("subscribe stream did not start")
	}
	if err := subscription.Close(); err != nil {
		t.Fatalf("close subscription: %v", err)
	}
	select {
	case <-gateway.canceled:
	case <-time.After(time.Second):
		t.Fatal("close did not cancel subscribe stream")
	}
}

type subscriptionCloseGateway struct {
	gatewayv1connect.UnimplementedGatewayHandler
	started  chan struct{}
	canceled chan struct{}
}

func (g *subscriptionCloseGateway) Subscribe(ctx context.Context, stream *connect.BidiStream[gw.SubscribeFrame, gw.Inbound]) error {
	close(g.started)
	<-ctx.Done()
	close(g.canceled)
	return ctx.Err()
}

func assertSDKError(t *testing.T, err error, kind ErrorKind, module string, gatedOn string) {
	t.Helper()
	var sdkErr *SDKError
	if !errors.As(err, &sdkErr) {
		t.Fatalf("error = %v, want SDKError", err)
	}
	if sdkErr.Kind != kind || sdkErr.Module != module || sdkErr.GatedOn != gatedOn {
		t.Fatalf("SDKError = %#v", sdkErr)
	}
}

func headersEqual(left []Header, right []Header) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name || string(left[index].Value) != string(right[index].Value) || (left[index].Value == nil) != (right[index].Value == nil) {
			return false
		}
	}
	return true
}

func queueMessagesEqual(left QueueMessage, right QueueMessage) bool {
	return left.MessageID == right.MessageID && left.Topic == right.Topic && left.Partition == right.Partition && left.Offset == right.Offset && string(left.Value) == string(right.Value) && headersEqual(left.Headers, right.Headers) && left.DeliveryCount == right.DeliveryCount
}

func assertQueueResults(t *testing.T, actual []QueueResult, expected []QueueResult) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("queue results length = %d, want %d", len(actual), len(expected))
	}
	for index := range actual {
		if actual[index].MessageID != expected[index].MessageID || !queueOperationErrorsEqual(actual[index].Error, expected[index].Error) {
			t.Fatalf("queue results = %#v, want %#v", actual, expected)
		}
	}
}

func queueOperationErrorsEqual(left *QueueOperationError, right *QueueOperationError) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Kind == right.Kind && left.Message == right.Message
}
