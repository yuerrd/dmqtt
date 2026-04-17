package metrics

import (
	"fmt"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	connectionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_connections_active",
		Help: "Currently connected MQTT clients.",
	})
	connectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_connections_total",
		Help: "Total MQTT connections accepted since start.",
	})
	disconnectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_disconnections_total",
		Help: "Total MQTT disconnections since start.",
	})
	messagesPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_messages_published_total",
		Help: "Total messages published by clients.",
	}, []string{"qos"})
	messagesDelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_messages_delivered_total",
		Help: "Total messages delivered to subscribers.",
	}, []string{"qos"})
	messagesForwarded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "mqtt_message_forwards_total",
		Help: "Total message forward operations to remote cluster nodes.",
	})
	subscriptionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_subscriptions_active",
		Help: "Current active MQTT subscriptions.",
	})
	retainedMessages = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_retained_messages",
		Help: "Current retained message count.",
	})
	goroutines = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_goroutines",
		Help: "Current number of goroutines.",
	})
	clusterNodes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_cluster_nodes",
		Help: "Number of nodes in the cluster (0 if standalone).",
	})
	authAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_auth_attempts_total",
		Help: "Total authentication attempts.",
	}, []string{"result"})
	aclDenials = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mqtt_acl_denials_total",
		Help: "Total ACL authorization denials.",
	}, []string{"action"})
	rateLimitRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_rate_limit_rejected_total",
		Help: "Total rate limit rejections.",
	}, []string{"level", "reason"})
	clientBlacklisted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_client_blacklisted_total",
		Help: "Total clients added to blacklist.",
	})
	backpressureLevel = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dmqtt_backpressure_level",
		Help: "Current backpressure level (0=none, 4=critical).",
	})
	circuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dmqtt_circuit_breaker_state",
		Help: "Circuit breaker state (0=closed, 1=open, 2=half-open).",
	}, []string{"shard"})
	offlineEvicted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_offline_messages_evicted_total",
		Help: "Total offline messages evicted by priority.",
	}, []string{"priority"})
	migrationTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_migration_total",
		Help: "Total migration tasks by status.",
	}, []string{"status"})
	migrationDevicesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_migration_devices_total",
		Help: "Total devices migrated.",
	})
	migrationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "dmqtt_migration_duration_seconds",
		Help:    "Migration task duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 10),
	})
	migrationActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dmqtt_migration_active",
		Help: "Currently active migrations.",
	})
)

func init() {
	prometheus.MustRegister(
		connectionsActive,
		connectionsTotal,
		disconnectionsTotal,
		messagesPublished,
		messagesDelivered,
		messagesForwarded,
		subscriptionsActive,
		retainedMessages,
		goroutines,
		clusterNodes,
		authAttempts,
		aclDenials,
		rateLimitRejected,
		clientBlacklisted,
		backpressureLevel,
		circuitBreakerState,
		offlineEvicted,
		migrationTotal,
		migrationDevicesTotal,
		migrationDuration,
		migrationActive,
	)
}

func ConnectionOpened() {
	connectionsActive.Inc()
	connectionsTotal.Inc()
}

func ConnectionClosed() {
	connectionsActive.Dec()
	disconnectionsTotal.Inc()
}

func MessagePublished(qos byte) {
	messagesPublished.WithLabelValues(fmt.Sprintf("%d", qos)).Inc()
}

func MessageDelivered(qos byte) {
	messagesDelivered.WithLabelValues(fmt.Sprintf("%d", qos)).Inc()
}

func MessageForwarded() {
	messagesForwarded.Inc()
}

func AuthAttempt(result string) {
	authAttempts.WithLabelValues(result).Inc()
}

func ACLDenial(action string) {
	aclDenials.WithLabelValues(action).Inc()
}

func RateLimitRejected(level, reason string) {
	rateLimitRejected.WithLabelValues(level, reason).Inc()
}

func ClientBlacklisted() {
	clientBlacklisted.Inc()
}

func SetBackpressureLevel(level int) {
	backpressureLevel.Set(float64(level))
}

func SetCircuitBreakerState(shard string, state int) {
	circuitBreakerState.WithLabelValues(shard).Set(float64(state))
}

func OfflineMessageEvicted(priority string) {
	offlineEvicted.WithLabelValues(priority).Inc()
}

func SetSubscriptions(n int) {
	subscriptionsActive.Set(float64(n))
}

func SetRetainedMessages(n int) {
	retainedMessages.Set(float64(n))
}

func SetClusterNodes(n int) {
	clusterNodes.Set(float64(n))
}

func SetGoroutines(n int) {
	goroutines.Set(float64(n))
}

// StatsProvider is called periodically to update gauge metrics.
type StatsProvider interface {
	ActiveSubscriptions() int
	RetainedMessageCount() int
	ClusterNodeCount() int
}

// StartCollector runs a background goroutine that polls the provider
// every 10 seconds and updates gauge metrics. It stops when done is closed.
func StartCollector(provider StatsProvider, done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		// Initial collection
		collect(provider)

		for {
			select {
			case <-ticker.C:
				collect(provider)
			case <-done:
				return
			}
		}
	}()
}

func collect(provider StatsProvider) {
	SetGoroutines(runtime.NumGoroutine())
	SetSubscriptions(provider.ActiveSubscriptions())
	SetRetainedMessages(provider.RetainedMessageCount())
	SetClusterNodes(provider.ClusterNodeCount())
}

func MigrationTotal(status string) {
	migrationTotal.WithLabelValues(status).Inc()
}

func MigrationDevicesTotal(n int) {
	migrationDevicesTotal.Add(float64(n))
}

func MigrationDuration(seconds float64) {
	migrationDuration.Observe(seconds)
}

func SetMigrationActive(n int) {
	migrationActive.Set(float64(n))
}
