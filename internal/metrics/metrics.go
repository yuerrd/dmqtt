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
