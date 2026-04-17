package tenant

import (
	"context"
	"testing"

	"github.com/langzp/dmqtt/internal/plugin"
)

// mockResolver implements TenantResolver for testing
type mockResolver struct {
	mapping map[string]string
}

func (r *mockResolver) ResolveTenant(username string) string {
	return r.mapping[username]
}

func newTestInterceptor(t *testing.T, tenantYAML string, resolver TenantResolver) *TenantInterceptor {
	t.Helper()
	m := NewManager()
	if tenantYAML != "" {
		if err := m.LoadTenantsFromBytes([]byte(tenantYAML)); err != nil {
			t.Fatal(err)
		}
	}
	return NewInterceptor(m, resolver)
}

func TestInterceptor_OnConnect_QuotaCheck(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme", "user2": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 1
    max_message_rate: 100
    max_message_size: 1024
`, resolver)

	ctx := context.Background()

	// First connect succeeds
	err := interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})
	if err != nil {
		t.Fatalf("first connect: %v", err)
	}

	// Second connect should fail (quota=1)
	err = interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c2", Username: "user2"})
	if err == nil {
		t.Fatal("expected quota error")
	}
}

func TestInterceptor_OnPublish_TopicPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 65536
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	evt := &plugin.PublishEvent{ClientID: "c1", Topic: "sensors/temp", Payload: []byte("hello")}
	if err := interceptor.OnPublish(ctx, evt); err != nil {
		t.Fatalf("OnPublish: %v", err)
	}
	if evt.Topic != "acme/sensors/temp" {
		t.Errorf("expected topic=acme/sensors/temp, got %s", evt.Topic)
	}
}

func TestInterceptor_OnPublish_SysTopic_NoPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 65536
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	evt := &plugin.PublishEvent{ClientID: "c1", Topic: "$SYS/broker/uptime", Payload: []byte("123")}
	if err := interceptor.OnPublish(ctx, evt); err != nil {
		t.Fatalf("OnPublish $SYS: %v", err)
	}
	if evt.Topic != "$SYS/broker/uptime" {
		t.Errorf("$SYS topic should not be prefixed, got %s", evt.Topic)
	}
}

func TestInterceptor_OnSubscribe_TopicPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 65536
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	evt := &plugin.SubscribeEvent{ClientID: "c1", TopicFilter: "sensors/#"}
	if err := interceptor.OnSubscribe(ctx, evt); err != nil {
		t.Fatalf("OnSubscribe: %v", err)
	}
	if evt.TopicFilter != "acme/sensors/#" {
		t.Errorf("expected filter=acme/sensors/#, got %s", evt.TopicFilter)
	}
}

func TestInterceptor_OnDelivery_StripPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 65536
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	evt := &plugin.DeliveryEvent{ClientID: "c1", Topic: "acme/sensors/temp", Payload: []byte("hello")}
	if err := interceptor.OnDelivery(ctx, evt); err != nil {
		t.Fatalf("OnDelivery: %v", err)
	}
	if evt.Topic != "sensors/temp" {
		t.Errorf("expected topic=sensors/temp, got %s", evt.Topic)
	}
}

func TestInterceptor_OnUnsubscribe_TopicPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 10
    max_message_rate: 1000
    max_message_size: 65536
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	evt := &plugin.UnsubscribeEvent{ClientID: "c1", TopicFilter: "sensors/#"}
	if err := interceptor.OnUnsubscribe(ctx, evt); err != nil {
		t.Fatalf("OnUnsubscribe: %v", err)
	}
	if evt.TopicFilter != "acme/sensors/#" {
		t.Errorf("expected filter=acme/sensors/#, got %s", evt.TopicFilter)
	}
}

func TestInterceptor_OnDisconnect_ReleasesQuota(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{"user1": "acme", "user2": "acme"}}
	interceptor := newTestInterceptor(t, `
tenants:
  - tenant_id: acme
    max_connections: 1
    max_message_rate: 100
    max_message_size: 1024
`, resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "user1"})

	// Second should fail
	err := interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c2", Username: "user2"})
	if err == nil {
		t.Fatal("expected quota error")
	}

	// Disconnect first
	interceptor.OnDisconnect(&plugin.DisconnectEvent{ClientID: "c1"})

	// Now second should succeed
	err = interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c2", Username: "user2"})
	if err != nil {
		t.Fatalf("after disconnect, should allow: %v", err)
	}
}

func TestInterceptor_DefaultTenant_NoPrefix(t *testing.T) {
	resolver := &mockResolver{mapping: map[string]string{}}
	interceptor := newTestInterceptor(t, "", resolver)

	ctx := context.Background()
	interceptor.OnConnect(ctx, &plugin.ConnectEvent{ClientID: "c1", Username: "unknown"})

	evt := &plugin.PublishEvent{ClientID: "c1", Topic: "sensors/temp", Payload: []byte("hello")}
	if err := interceptor.OnPublish(ctx, evt); err != nil {
		t.Fatalf("OnPublish: %v", err)
	}
	if evt.Topic != "sensors/temp" {
		t.Errorf("default tenant should not prefix, got %s", evt.Topic)
	}
}
