package plugin

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type recordingInterceptor struct {
	name      string
	connected []string
	published []string
}

func (r *recordingInterceptor) Name() string { return r.name }
func (r *recordingInterceptor) Init() error  { return nil }
func (r *recordingInterceptor) Close() error { return nil }

func (r *recordingInterceptor) OnConnect(ctx context.Context, evt *ConnectEvent) error {
	r.connected = append(r.connected, evt.ClientID)
	return nil
}

func (r *recordingInterceptor) OnPublish(ctx context.Context, evt *PublishEvent) error {
	r.published = append(r.published, evt.Topic)
	return nil
}

type rejectInterceptor struct {
	name string
}

func (r *rejectInterceptor) Name() string { return r.name }
func (r *rejectInterceptor) Init() error  { return nil }
func (r *rejectInterceptor) Close() error { return nil }
func (r *rejectInterceptor) OnPublish(ctx context.Context, evt *PublishEvent) error {
	return errors.New("rejected by " + r.name)
}

type disconnectRecorder struct {
	count atomic.Int32
}

func (d *disconnectRecorder) Name() string { return "disconn-recorder" }
func (d *disconnectRecorder) Init() error  { return nil }
func (d *disconnectRecorder) Close() error { return nil }
func (d *disconnectRecorder) OnDisconnect(evt *DisconnectEvent) {
	d.count.Add(1)
}

func TestChain_OnPublish_MultipleInterceptors(t *testing.T) {
	chain := NewInterceptorChain()
	r1 := &recordingInterceptor{name: "r1"}
	r2 := &recordingInterceptor{name: "r2"}
	chain.Register(r1)
	chain.Register(r2)

	evt := &PublishEvent{ClientID: "c1", Topic: "t/1"}
	err := chain.OnPublish(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r1.published) != 1 || r1.published[0] != "t/1" {
		t.Fatalf("r1 not called correctly")
	}
	if len(r2.published) != 1 || r2.published[0] != "t/1" {
		t.Fatalf("r2 not called correctly")
	}
}

func TestChain_OnPublish_RejectStopsChain(t *testing.T) {
	chain := NewInterceptorChain()
	reject := &rejectInterceptor{name: "blocker"}
	r2 := &recordingInterceptor{name: "r2"}
	chain.Register(reject)
	chain.Register(r2)

	evt := &PublishEvent{ClientID: "c1", Topic: "t/1"}
	err := chain.OnPublish(context.Background(), evt)
	if err == nil {
		t.Fatal("expected error from reject interceptor")
	}
	if len(r2.published) != 0 {
		t.Fatal("r2 should not have been called after rejection")
	}
}

func TestChain_OnDisconnect_Async(t *testing.T) {
	chain := NewInterceptorChain()
	rec := &disconnectRecorder{}
	chain.Register(rec)

	chain.OnDisconnect(&DisconnectEvent{ClientID: "c1", Reason: "clean"})

	// Wait for async execution
	time.Sleep(50 * time.Millisecond)
	if rec.count.Load() != 1 {
		t.Fatalf("expected 1 disconnect call, got %d", rec.count.Load())
	}
}

func TestChain_SkipsUnimplementedHooks(t *testing.T) {
	chain := NewInterceptorChain()
	// recordingInterceptor implements OnConnect and OnPublish, but NOT OnSubscribe
	r := &recordingInterceptor{name: "partial"}
	chain.Register(r)

	// OnSubscribe should succeed (no interceptors implement it)
	err := chain.OnSubscribe(context.Background(), &SubscribeEvent{ClientID: "c1", TopicFilter: "t/#", QoS: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestChain_InitAndClose(t *testing.T) {
	chain := NewInterceptorChain()
	r := &recordingInterceptor{name: "r1"}
	chain.Register(r)

	if err := chain.InitAll(); err != nil {
		t.Fatalf("InitAll failed: %v", err)
	}
	if err := chain.CloseAll(); err != nil {
		t.Fatalf("CloseAll failed: %v", err)
	}
}

// --- Additional interceptor types for new hook tests ---

type connectRecorder struct {
	name   string
	events []*ConnectEvent
}

func (c *connectRecorder) Name() string { return c.name }
func (c *connectRecorder) Init() error  { return nil }
func (c *connectRecorder) Close() error { return nil }
func (c *connectRecorder) OnConnect(_ context.Context, evt *ConnectEvent) error {
	c.events = append(c.events, evt)
	return nil
}

type deliveryRecorder struct {
	name   string
	topics []string
}

func (d *deliveryRecorder) Name() string { return d.name }
func (d *deliveryRecorder) Init() error  { return nil }
func (d *deliveryRecorder) Close() error { return nil }
func (d *deliveryRecorder) OnDelivery(_ context.Context, evt *DeliveryEvent) error {
	d.topics = append(d.topics, evt.Topic)
	return nil
}

type unsubRecorder struct {
	name    string
	filters []string
}

func (u *unsubRecorder) Name() string { return u.name }
func (u *unsubRecorder) Init() error  { return nil }
func (u *unsubRecorder) Close() error { return nil }
func (u *unsubRecorder) OnUnsubscribe(_ context.Context, evt *UnsubscribeEvent) error {
	u.filters = append(u.filters, evt.TopicFilter)
	return nil
}

type sessionExpiredRecorder struct {
	count atomic.Int32
}

func (s *sessionExpiredRecorder) Name() string { return "session-expired-recorder" }
func (s *sessionExpiredRecorder) Init() error  { return nil }
func (s *sessionExpiredRecorder) Close() error { return nil }
func (s *sessionExpiredRecorder) OnSessionExpired(_ *SessionExpiredEvent) {
	s.count.Add(1)
}

// --- Tests for new hooks ---

func TestChain_OnConnect(t *testing.T) {
	chain := NewInterceptorChain()
	rec := &connectRecorder{name: "connect-rec"}
	chain.Register(rec)

	evt := &ConnectEvent{ClientID: "c1", Username: "user1"}
	if err := chain.OnConnect(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.events) != 1 || rec.events[0].ClientID != "c1" {
		t.Fatalf("OnConnect not called correctly, got %v", rec.events)
	}
}

func TestChain_OnDelivery(t *testing.T) {
	chain := NewInterceptorChain()
	rec := &deliveryRecorder{name: "delivery-rec"}
	chain.Register(rec)

	evt := &DeliveryEvent{ClientID: "c1", Topic: "sensor/temp", Payload: []byte("25"), QoS: 0}
	if err := chain.OnDelivery(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.topics) != 1 || rec.topics[0] != "sensor/temp" {
		t.Fatalf("OnDelivery not called correctly, got %v", rec.topics)
	}
}

func TestChain_OnUnsubscribe(t *testing.T) {
	chain := NewInterceptorChain()
	rec := &unsubRecorder{name: "unsub-rec"}
	chain.Register(rec)

	evt := &UnsubscribeEvent{ClientID: "c1", TopicFilter: "sensor/#"}
	if err := chain.OnUnsubscribe(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec.filters) != 1 || rec.filters[0] != "sensor/#" {
		t.Fatalf("OnUnsubscribe not called correctly, got %v", rec.filters)
	}
}

func TestChain_OnSessionExpired_Async(t *testing.T) {
	chain := NewInterceptorChain()
	rec := &sessionExpiredRecorder{}
	chain.Register(rec)

	chain.OnSessionExpired(&SessionExpiredEvent{ClientID: "c1"})
	time.Sleep(50 * time.Millisecond)
	if rec.count.Load() != 1 {
		t.Fatalf("expected 1 session-expired call, got %d", rec.count.Load())
	}
}

func TestChain_WithOptions(t *testing.T) {
	chain := NewInterceptorChain()
	r := &recordingInterceptor{name: "r1"}
	chain.Register(r,
		WithTimeout(200*time.Millisecond),
		WithBreakerThreshold(0.8),
		WithBreakerResetInterval(60*time.Second),
	)

	// Just verify registration succeeds and dispatching works
	if err := chain.OnPublish(context.Background(), &PublishEvent{Topic: "t/1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(r.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(r.published))
	}
}

type initErrInterceptor struct{ name string }

func (i *initErrInterceptor) Name() string { return i.name }
func (i *initErrInterceptor) Init() error  { return errors.New("init failed: " + i.name) }
func (i *initErrInterceptor) Close() error { return errors.New("close failed: " + i.name) }

func TestChain_InitAll_Error(t *testing.T) {
	chain := NewInterceptorChain()
	chain.Register(&initErrInterceptor{name: "err-interceptor"})
	if err := chain.InitAll(); err == nil {
		t.Fatal("expected error from InitAll")
	}
}

func TestChain_CloseAll_Error(t *testing.T) {
	chain := NewInterceptorChain()
	chain.Register(&initErrInterceptor{name: "err-interceptor"})
	if err := chain.CloseAll(); err == nil {
		t.Fatal("expected error from CloseAll")
	}
}
