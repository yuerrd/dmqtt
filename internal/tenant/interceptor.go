package tenant

import (
	"context"
	"log/slog"
	"strings"

	"github.com/yuerrd/dmqtt/internal/metrics"
	"github.com/yuerrd/dmqtt/internal/plugin"
)

// TenantResolver resolves a username to a tenant ID.
type TenantResolver interface {
	ResolveTenant(username string) string
}

// TenantInterceptor implements multi-tenant isolation via the interceptor framework.
// It must be registered FIRST in the chain so topic rewriting happens before
// other interceptors see the message.
type TenantInterceptor struct {
	manager  *TenantManager
	resolver TenantResolver
}

// NewInterceptor creates a new TenantInterceptor.
func NewInterceptor(manager *TenantManager, resolver TenantResolver) *TenantInterceptor {
	return &TenantInterceptor{
		manager:  manager,
		resolver: resolver,
	}
}

// Name returns the interceptor name for metrics and logging.
func (ti *TenantInterceptor) Name() string {
	return "tenant"
}

// Init is a no-op for TenantInterceptor.
func (ti *TenantInterceptor) Init() error {
	return nil
}

// Close is a no-op for TenantInterceptor.
func (ti *TenantInterceptor) Close() error {
	return nil
}

// OnConnect resolves the tenant for the connecting user, checks connection quota,
// and records the clientID→tenantID mapping.
func (ti *TenantInterceptor) OnConnect(ctx context.Context, evt *plugin.ConnectEvent) error {
	tenantID := ti.resolver.ResolveTenant(evt.Username)

	if err := ti.manager.TryConnect(evt.ClientID, tenantID); err != nil {
		tid := effectiveTenantID(tenantID)
		metrics.TenantConnectionRejected(tid)
		slog.Info("tenant connection rejected", "client", evt.ClientID, "tenant", tid, "error", err)
		return err
	}

	tid := effectiveTenantID(tenantID)
	metrics.TenantConnectionOpened(tid)
	slog.Debug("tenant connection accepted", "client", evt.ClientID, "tenant", tid)
	return nil
}

// OnPublish checks rate limits and message size, then prepends the tenant topic prefix.
func (ti *TenantInterceptor) OnPublish(ctx context.Context, evt *plugin.PublishEvent) error {
	if err := ti.manager.CheckPublish(evt.ClientID, len(evt.Payload)); err != nil {
		tid := effectiveTenantID(ti.manager.GetTenantID(evt.ClientID))
		if strings.Contains(err.Error(), "rate limit") {
			metrics.TenantMessageRejected(tid, "rate_limit")
		} else {
			metrics.TenantMessageRejected(tid, "message_size")
		}
		return err
	}

	prefix := ti.manager.GetTopicPrefix(evt.ClientID)
	if prefix != "" && !strings.HasPrefix(evt.Topic, "$") {
		evt.Topic = prefix + "/" + evt.Topic
	}
	return nil
}

// OnSubscribe prepends the tenant topic prefix to the subscription filter.
func (ti *TenantInterceptor) OnSubscribe(ctx context.Context, evt *plugin.SubscribeEvent) error {
	prefix := ti.manager.GetTopicPrefix(evt.ClientID)
	if prefix != "" && !strings.HasPrefix(evt.TopicFilter, "$") {
		evt.TopicFilter = prefix + "/" + evt.TopicFilter
	}
	return nil
}

// OnDelivery strips the tenant topic prefix before delivering to the client.
func (ti *TenantInterceptor) OnDelivery(ctx context.Context, evt *plugin.DeliveryEvent) error {
	prefix := ti.manager.GetTopicPrefix(evt.ClientID)
	if prefix != "" {
		pfx := prefix + "/"
		if strings.HasPrefix(evt.Topic, pfx) {
			evt.Topic = strings.TrimPrefix(evt.Topic, pfx)
		}
	}
	return nil
}

// OnUnsubscribe prepends the tenant topic prefix to the unsubscribe filter.
func (ti *TenantInterceptor) OnUnsubscribe(ctx context.Context, evt *plugin.UnsubscribeEvent) error {
	prefix := ti.manager.GetTopicPrefix(evt.ClientID)
	if prefix != "" && !strings.HasPrefix(evt.TopicFilter, "$") {
		evt.TopicFilter = prefix + "/" + evt.TopicFilter
	}
	return nil
}

// OnDisconnect releases the connection count and cleans up the client mapping.
func (ti *TenantInterceptor) OnDisconnect(evt *plugin.DisconnectEvent) {
	tid := effectiveTenantID(ti.manager.GetTenantID(evt.ClientID))
	ti.manager.OnDisconnect(evt.ClientID)
	metrics.TenantConnectionClosed(tid)
	slog.Debug("tenant connection closed", "client", evt.ClientID, "tenant", tid)
}
