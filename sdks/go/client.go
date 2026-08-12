package crabka

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/robot-head/crabka/sdks/go/gen/crabka/gateway/v1/gatewayv1connect"
	"golang.org/x/net/http2"
)

type Option func(*Client)

func WithBearerToken(token string) Option {
	return func(client *Client) {
		client.bearerToken = token
	}
}

type Client struct {
	endpoint    string
	httpClient  *http.Client
	bearerToken string
	gateway     gatewayv1connect.GatewayClient
	mockStore   *mockStore
}

func New(endpoint string, httpClient *http.Client, opts ...Option) *Client {
	if httpClient == nil {
		httpClient = defaultHTTPClientForEndpoint(endpoint)
	}
	client := &Client{endpoint: endpoint, httpClient: httpClient}
	for _, opt := range opts {
		opt(client)
	}
	if strings.HasPrefix(endpoint, "mock://") || strings.HasPrefix(endpoint, "unreachable://") {
		client.mockStore = newMockStore()
		return client
	}
	client.gateway = gatewayv1connect.NewGatewayClient(httpClient, endpoint)
	return client
}

func defaultHTTPClientForEndpoint(endpoint string) *http.Client {
	if !isPlaintextHTTPEndpoint(endpoint) {
		return http.DefaultClient
	}
	return &http.Client{Transport: plaintextHTTP2Transport()}
}

func isPlaintextHTTPEndpoint(endpoint string) bool {
	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	return parsedEndpoint.Scheme == "http"
}

func plaintextHTTP2Transport() http.RoundTripper {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network string, address string, _ *tls.Config) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
	}
}

func (c *Client) BearerToken() string { return c.bearerToken }

func (c *Client) Messaging() *Messaging { return &Messaging{client: c} }
func (c *Client) Queues() *Queues       { return &Queues{client: c} }
func (c *Client) Database() *Database   { return &Database{client: c} }
func (c *Client) Auth() *Auth           { return &Auth{client: c} }
func (c *Client) Blob() *Blob           { return &Blob{client: c} }
