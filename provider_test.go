package asyncapi

import (
	"context"
	"testing"

	openbindings "github.com/openbindings/openbindings-go"
)

func TestGetContextInfo_HTTPServer(t *testing.T) {
	spec := `{
  "asyncapi": "3.0.0",
  "info": { "title": "Chat Service", "version": "1.0.0" },
  "servers": {
    "production": {
      "host": "ws.example.com",
      "protocol": "wss",
      "pathname": "/v1"
    }
  },
  "channels": {}
}`

	p := New()
	result, err := p.GetContextInfo(context.Background(), openbindings.ExecuteSource{
		Location: "chat.json",
		Content:  spec,
	}, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Key != "wss://ws.example.com" {
		t.Errorf("expected key 'wss://ws.example.com', got %q", result.Key)
	}
	if result.Required {
		t.Error("AsyncAPI context should not be required by default")
	}
	if result.Description != "Chat Service" {
		t.Errorf("expected description 'Chat Service', got %q", result.Description)
	}
}

func TestGetContextInfo_NoHTTPServer(t *testing.T) {
	spec := `{
  "asyncapi": "3.0.0",
  "info": { "title": "MQTT Service", "version": "1.0.0" },
  "servers": {
    "broker": {
      "host": "mqtt.example.com",
      "protocol": "mqtt"
    }
  },
  "channels": {}
}`

	p := New()
	result, err := p.GetContextInfo(context.Background(), openbindings.ExecuteSource{
		Location: "mqtt.json",
		Content:  spec,
	}, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for non-HTTP/WS server, got key=%q", result.Key)
	}
}

func TestGetContextInfo_NoServers(t *testing.T) {
	spec := `{
  "asyncapi": "3.0.0",
  "info": { "title": "No Servers", "version": "1.0.0" },
  "channels": {}
}`

	p := New()
	result, err := p.GetContextInfo(context.Background(), openbindings.ExecuteSource{
		Location: "noservers.json",
		Content:  spec,
	}, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil result for no servers, got key=%q", result.Key)
	}
}

func TestGetContextInfo_MultipleServers_PicksFirst(t *testing.T) {
	spec := `{
  "asyncapi": "3.0.0",
  "info": { "title": "Multi Server", "version": "1.0.0" },
  "servers": {
    "alpha": {
      "host": "alpha.example.com",
      "protocol": "https"
    },
    "beta": {
      "host": "beta.example.com",
      "protocol": "wss"
    }
  },
  "channels": {}
}`

	p := New()
	result, err := p.GetContextInfo(context.Background(), openbindings.ExecuteSource{
		Location: "multi.json",
		Content:  spec,
	}, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	// Sorted alphabetically, "alpha" comes first
	if result.Key != "https://alpha.example.com" {
		t.Errorf("expected key from first sorted server, got %q", result.Key)
	}
}

func TestGetContextInfo_DefaultDescription(t *testing.T) {
	spec := `{
  "asyncapi": "3.0.0",
  "info": { "title": "", "version": "1.0.0" },
  "servers": {
    "main": {
      "host": "example.com",
      "protocol": "ws"
    }
  },
  "channels": {}
}`

	p := New()
	result, err := p.GetContextInfo(context.Background(), openbindings.ExecuteSource{
		Location: "notitle.json",
		Content:  spec,
	}, "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Description != "AsyncAPI service" {
		t.Errorf("expected fallback description 'AsyncAPI service', got %q", result.Description)
	}
}
