package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	g.Write(&m)
	return m.GetGauge().GetValue()
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	c.Write(&m)
	return m.GetCounter().GetValue()
}

func TestConnectionOpenedClosed(t *testing.T) {
	// Reset
	connectionsActive.Set(0)

	ConnectionOpened()
	ConnectionOpened()
	if v := gaugeValue(connectionsActive); v != 2 {
		t.Errorf("connections_active = %v, want 2", v)
	}

	ConnectionClosed()
	if v := gaugeValue(connectionsActive); v != 1 {
		t.Errorf("connections_active = %v, want 1", v)
	}

	if v := counterValue(connectionsTotal); v < 2 {
		t.Errorf("connections_total = %v, want >= 2", v)
	}
	if v := counterValue(disconnectionsTotal); v < 1 {
		t.Errorf("disconnections_total = %v, want >= 1", v)
	}
}

func TestMessagePublished(t *testing.T) {
	MessagePublished(0)
	MessagePublished(1)
	MessagePublished(2)

	var m dto.Metric
	messagesPublished.WithLabelValues("1").Write(&m)
	if v := m.GetCounter().GetValue(); v < 1 {
		t.Errorf("messages_published{qos=1} = %v, want >= 1", v)
	}
}

func TestMessageDelivered(t *testing.T) {
	MessageDelivered(0)
	MessageDelivered(1)

	var m dto.Metric
	messagesDelivered.WithLabelValues("0").Write(&m)
	if v := m.GetCounter().GetValue(); v < 1 {
		t.Errorf("messages_delivered{qos=0} = %v, want >= 1", v)
	}
}

func TestMessageForwarded(t *testing.T) {
	before := counterValue(messagesForwarded)
	MessageForwarded()
	after := counterValue(messagesForwarded)
	if after-before != 1 {
		t.Errorf("messages_forwarded increment = %v, want 1", after-before)
	}
}

type mockProvider struct {
	subs    int
	retain  int
	cluster int
}

func (m *mockProvider) ActiveSubscriptions() int  { return m.subs }
func (m *mockProvider) RetainedMessageCount() int { return m.retain }
func (m *mockProvider) ClusterNodeCount() int     { return m.cluster }

func TestCollect(t *testing.T) {
	p := &mockProvider{subs: 42, retain: 5, cluster: 3}
	collect(p)

	if v := gaugeValue(subscriptionsActive); v != 42 {
		t.Errorf("subscriptions_active = %v, want 42", v)
	}
	if v := gaugeValue(retainedMessages); v != 5 {
		t.Errorf("retained_messages = %v, want 5", v)
	}
	if v := gaugeValue(clusterNodes); v != 3 {
		t.Errorf("cluster_nodes = %v, want 3", v)
	}
	if v := gaugeValue(goroutines); v <= 0 {
		t.Errorf("goroutines = %v, want > 0", v)
	}
}

func TestAuthAttemptMetric(t *testing.T) {
	AuthAttempt("success")
	AuthAttempt("failure")
	AuthAttempt("failure")

	m := &dto.Metric{}
	authAttempts.WithLabelValues("success").Write(m)
	if m.Counter.GetValue() < 1 {
		t.Error("expected success counter >= 1")
	}
	authAttempts.WithLabelValues("failure").Write(m)
	if m.Counter.GetValue() < 2 {
		t.Error("expected failure counter >= 2")
	}
}

func TestACLDenialMetric(t *testing.T) {
	ACLDenial("publish")
	ACLDenial("subscribe")

	m := &dto.Metric{}
	aclDenials.WithLabelValues("publish").Write(m)
	if m.Counter.GetValue() < 1 {
		t.Error("expected publish denial counter >= 1")
	}
	aclDenials.WithLabelValues("subscribe").Write(m)
	if m.Counter.GetValue() < 1 {
		t.Error("expected subscribe denial counter >= 1")
	}
}

func TestRateLimitRejected(t *testing.T) {
	RateLimitRejected("client", "rate_exceeded")
	RateLimitRejected("global", "ingress_exceeded")
}

func TestClientBlacklisted(t *testing.T) {
	ClientBlacklisted()
}

func TestSetBackpressureLevel(t *testing.T) {
	SetBackpressureLevel(3)
}

func TestSetCircuitBreakerState(t *testing.T) {
	SetCircuitBreakerState("shard-1", 1)
}

func TestOfflineMessageEvicted(t *testing.T) {
	OfflineMessageEvicted("low")
}

func TestMigrationTotal(t *testing.T) {
	MigrationTotal("completed")
	// No panic = pass
}

func TestMigrationDevicesTotal(t *testing.T) {
	MigrationDevicesTotal(5)
	// No panic = pass
}

func TestMigrationDuration(t *testing.T) {
	MigrationDuration(2.5)
	// No panic = pass
}

func TestSetMigrationActive(t *testing.T) {
	SetMigrationActive(3)
	// No panic = pass
}

func TestMessageLatency(t *testing.T) {
	MessageLatency(0, 5*time.Millisecond)
	MessageLatency(1, 10*time.Millisecond)
	MessageLatency(2, 100*time.Millisecond)
}

func TestTopicMatchDuration(t *testing.T) {
	TopicMatchDuration(50 * time.Microsecond)
	TopicMatchDuration(1 * time.Millisecond)
}

func TestStorageWriteLatency(t *testing.T) {
	StorageWriteLatency("set", 100*time.Microsecond)
	StorageWriteLatency("delete", 200*time.Microsecond)
}

func TestStorageReadLatency(t *testing.T) {
	StorageReadLatency("get", 50*time.Microsecond)
	StorageReadLatency("scan", 5*time.Millisecond)
}

func TestCollectSystemMetrics(t *testing.T) {
	collectSystemMetrics()
}
