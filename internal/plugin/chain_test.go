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
