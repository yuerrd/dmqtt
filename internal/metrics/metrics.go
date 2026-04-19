package metrics

import (
	"fmt"
	"runtime"
	"sync"
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
	interceptorDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dmqtt_interceptor_duration_seconds",
		Help:    "Interceptor execution duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"name", "hook"})
	interceptorErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_interceptor_errors_total",
		Help: "Total interceptor errors.",
	}, []string{"name", "hook"})
	interceptorSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_interceptor_skipped_total",
		Help: "Total interceptor calls skipped due to circuit breaker.",
	}, []string{"name"})
	auditEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_audit_entries_total",
		Help: "Total audit entries recorded.",
	}, []string{"action"})
	auditDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_audit_dropped_total",
		Help: "Total audit entries dropped.",
	})
	ruleEvaluations = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_rule_evaluations_total",
		Help: "Total rule evaluations performed.",
	})
	ruleMatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_rule_matches_total",
		Help: "Total rules that matched (CEL filter returned true).",
	})
	ruleActions = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_rule_actions_total",
		Help: "Total rule actions dispatched.",
	})
	ruleActionErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_rule_action_errors_total",
		Help: "Total rule action execution errors.",
	})
	ruleActionsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dmqtt_rule_actions_dropped_total",
		Help: "Total rule actions dropped (worker pool full).",
	})
	tenantConnectionsActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dmqtt_tenant_connections_active",
		Help: "Current active connections per tenant.",
	}, []string{"tenant_id"})
	tenantConnectionsRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_tenant_connections_rejected_total",
		Help: "Total connections rejected per tenant (quota exhausted).",
	}, []string{"tenant_id"})
	tenantMessagesRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dmqtt_tenant_messages_rejected_total",
		Help: "Total messages rejected per tenant.",
	}, []string{"tenant_id", "reason"})
	messageLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mqtt_message_latency_seconds",
		Help:    "End-to-end message publish-to-deliver latency.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"qos"})
	topicMatchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "mqtt_topic_match_duration_seconds",
		Help:    "Time spent matching a topic against the subscription index.",
		Buckets: []float64{0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01},
	})
	storageWriteLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mqtt_storage_write_latency_seconds",
		Help:    "Pebble storage write operation latency.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	}, []string{"op"})
	storageReadLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mqtt_storage_read_latency_seconds",
		Help:    "Pebble storage read operation latency.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
	}, []string{"op"})
	cpuUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_cpu_usage",
		Help: "Process CPU usage (user+system time delta per collection interval).",
	})
	memorySysBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_memory_usage_bytes",
		Help: "Total bytes of memory obtained from the OS (runtime.MemStats.Sys).",
	})
	memoryAllocBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "mqtt_memory_alloc_bytes",
		Help: "Bytes of allocated heap objects (runtime.MemStats.Alloc).",
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
		interceptorDuration,
		interceptorErrors,
		interceptorSkipped,
		auditEntries,
		auditDropped,
		ruleEvaluations,
		ruleMatches,
		ruleActions,
		ruleActionErrors,
		ruleActionsDropped,
		tenantConnectionsActive,
		tenantConnectionsRejected,
		tenantMessagesRejected,
		messageLatency,
		topicMatchDuration,
		storageWriteLatency,
		storageReadLatency,
		cpuUsage,
		memorySysBytes,
		memoryAllocBytes,
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
	collectSystemMetrics()
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

func InterceptorDuration(name, hook string, seconds float64) {
	interceptorDuration.WithLabelValues(name, hook).Observe(seconds)
}

func InterceptorError(name, hook string) {
	interceptorErrors.WithLabelValues(name, hook).Inc()
}

func InterceptorSkipped(name string) {
	interceptorSkipped.WithLabelValues(name).Inc()
}

func AuditEntry(action string) {
	auditEntries.WithLabelValues(action).Inc()
}

func AuditDropped() {
	auditDropped.Inc()
}

func RuleEvaluation()    { ruleEvaluations.Inc() }
func RuleMatch()         { ruleMatches.Inc() }
func RuleAction()        { ruleActions.Inc() }
func RuleActionError()   { ruleActionErrors.Inc() }
func RuleActionDropped() { ruleActionsDropped.Inc() }

var tenantRegistry = struct {
	mu       sync.Mutex
	tenants  map[string]struct{}
	maxCount int
}{
	tenants:  make(map[string]struct{}),
	maxCount: 1000,
}

func isTenantRegistered(tenantID string) bool {
	tenantRegistry.mu.Lock()
	defer tenantRegistry.mu.Unlock()
	if _, ok := tenantRegistry.tenants[tenantID]; ok {
		return true
	}
	if len(tenantRegistry.tenants) >= tenantRegistry.maxCount {
		return false
	}
	tenantRegistry.tenants[tenantID] = struct{}{}
	return true
}

func TenantConnectionOpened(tenantID string) {
	if !isTenantRegistered(tenantID) {
		tenantID = "__overflow__"
	}
	tenantConnectionsActive.WithLabelValues(tenantID).Inc()
}

func TenantConnectionClosed(tenantID string) {
	if !isTenantRegistered(tenantID) {
		tenantID = "__overflow__"
	}
	tenantConnectionsActive.WithLabelValues(tenantID).Dec()
}

func TenantConnectionRejected(tenantID string) {
	if !isTenantRegistered(tenantID) {
		tenantID = "__overflow__"
	}
	tenantConnectionsRejected.WithLabelValues(tenantID).Inc()
}

func TenantMessageRejected(tenantID, reason string) {
	if !isTenantRegistered(tenantID) {
		tenantID = "__overflow__"
	}
	tenantMessagesRejected.WithLabelValues(tenantID, reason).Inc()
}

func MessageLatency(qos byte, d time.Duration) {
	messageLatency.WithLabelValues(fmt.Sprintf("%d", qos)).Observe(d.Seconds())
}

func TopicMatchDuration(d time.Duration) {
	topicMatchDuration.Observe(d.Seconds())
}

// StorageWriteLatency records a storage write operation latency.
// Valid op values: "set", "delete".
func StorageWriteLatency(op string, d time.Duration) {
	storageWriteLatency.WithLabelValues(op).Observe(d.Seconds())
}

// StorageReadLatency records a storage read operation latency.
// Valid op values: "get", "scan".
func StorageReadLatency(op string, d time.Duration) {
	storageReadLatency.WithLabelValues(op).Observe(d.Seconds())
}

var cpuState struct {
	mu          sync.Mutex
	lastCPUTime float64
	lastCollect time.Time
}

func collectSystemMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	memorySysBytes.Set(float64(memStats.Sys))
	memoryAllocBytes.Set(float64(memStats.Alloc))

	totalCPU, ok := cpuTimeSec()
	if !ok {
		return
	}

	cpuState.mu.Lock()
	defer cpuState.mu.Unlock()

	now := time.Now()
	if !cpuState.lastCollect.IsZero() {
		elapsed := now.Sub(cpuState.lastCollect).Seconds()
		if elapsed > 0 {
			cpuUsage.Set((totalCPU - cpuState.lastCPUTime) / elapsed)
		}
	}
	cpuState.lastCPUTime = totalCPU
	cpuState.lastCollect = now
}
