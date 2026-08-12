//go:build integration

package crabka

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	integrationOptInEnv           = "CRABKA_GO_INTEGRATION"
	integrationGatewayEndpointEnv = "CRABKA_GATEWAY_ENDPOINT"
)

func TestComposeGatewaySmoke(t *testing.T) {
	if os.Getenv(integrationOptInEnv) != "1" {
		t.Skipf("set %s=1 and %s to run the live compose gateway smoke", integrationOptInEnv, integrationGatewayEndpointEnv)
	}

	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv(integrationGatewayEndpointEnv)), "/")
	if endpoint == "" {
		t.Fatalf("%s=1 requires %s, for example http://127.0.0.1:9500", integrationOptInEnv, integrationGatewayEndpointEnv)
	}

	client := New(endpoint, nil)
	if client.gateway == nil {
		t.Fatalf("%s must name a live gateway endpoint, got %q", integrationGatewayEndpointEnv, endpoint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := checkGatewayHealthOverSDKTransport(ctx, endpoint); err != nil {
		t.Fatalf("gateway h2c health smoke through SDK transport failed: %v", err)
	}
}

func checkGatewayHealthOverSDKTransport(ctx context.Context, endpoint string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/healthz", nil)
	if err != nil {
		return fmt.Errorf("build health request: %w", err)
	}

	response, err := defaultHTTPClientForEndpoint(endpoint).Do(request)
	if err != nil {
		return fmt.Errorf("GET %s: %w", request.URL, err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %s, want 200 OK", request.URL, response.Status)
	}
	return nil
}
