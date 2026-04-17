package ratelimit

import (
	"testing"
	"time"
)

func TestMaliciousDetector_NoScoreInitially(t *testing.T) {
	cfg := &DetectorConfig{Enabled: true, HighRateThreshold: 100, ScanThreshold: 10, ReconnectThreshold: 5, ScoreThreshold: 80, DecayInterval: time.Minute}
	d := NewMaliciousDetector(cfg)
	if score := d.Score("client1"); score != 0 {
		t.Fatalf("initial score should be 0, got %f", score)
	}
	if d.IsMalicious("client1") {
		t.Fatal("should not be malicious initially")
	}
}

func TestMaliciousDetector_HighRateDetection(t *testing.T) {
	cfg := &DetectorConfig{Enabled: true, HighRateThreshold: 10, ScanThreshold: 100, ReconnectThreshold: 100, ScoreThreshold: 40, DecayInterval: time.Hour}
	d := NewMaliciousDetector(cfg)
	for i := 0; i < 20; i++ {
		d.RecordMessage("client1")
	}
	d.Evaluate("client1")
	if !d.IsMalicious("client1") {
		t.Fatalf("should be malicious after high rate; score=%f", d.Score("client1"))
	}
}

func TestMaliciousDetector_TopicScanDetection(t *testing.T) {
	cfg := &DetectorConfig{Enabled: true, HighRateThreshold: 100_000, ScanThreshold: 5, ReconnectThreshold: 100, ScoreThreshold: 25, DecayInterval: time.Hour}
	d := NewMaliciousDetector(cfg)
	for i := 0; i < 10; i++ {
		d.RecordSubscribe("client1", "topic/"+string(rune('a'+i)))
	}
	d.Evaluate("client1")
	if !d.IsMalicious("client1") {
		t.Fatalf("should be malicious after topic scan; score=%f", d.Score("client1"))
	}
}

func TestMaliciousDetector_ReconnectDetection(t *testing.T) {
	cfg := &DetectorConfig{Enabled: true, HighRateThreshold: 100_000, ScanThreshold: 100, ReconnectThreshold: 3, ScoreThreshold: 15, DecayInterval: time.Hour}
	d := NewMaliciousDetector(cfg)
	for i := 0; i < 5; i++ {
		d.RecordReconnect("client1")
	}
	d.Evaluate("client1")
	if !d.IsMalicious("client1") {
		t.Fatalf("should be malicious after reconnect loop; score=%f", d.Score("client1"))
	}
}

func TestMaliciousDetector_ScoreDecay(t *testing.T) {
	cfg := &DetectorConfig{Enabled: true, HighRateThreshold: 10, ScanThreshold: 100, ReconnectThreshold: 100, ScoreThreshold: 80, DecayInterval: 50 * time.Millisecond}
	d := NewMaliciousDetector(cfg)
	for i := 0; i < 20; i++ {
		d.RecordMessage("client1")
	}
	d.Evaluate("client1")
	scoreBefore := d.Score("client1")
	time.Sleep(100 * time.Millisecond)
	d.Decay()
	scoreAfter := d.Score("client1")
	if scoreAfter >= scoreBefore {
		t.Fatalf("score should decay: before=%f after=%f", scoreBefore, scoreAfter)
	}
}

func TestMaliciousDetector_DisabledNeverMalicious(t *testing.T) {
	cfg := &DetectorConfig{Enabled: false, HighRateThreshold: 1, ScoreThreshold: 0, DecayInterval: time.Minute}
	d := NewMaliciousDetector(cfg)
	for i := 0; i < 100; i++ {
		d.RecordMessage("client1")
	}
	d.Evaluate("client1")
	if d.IsMalicious("client1") {
		t.Fatal("should never be malicious when disabled")
	}
}
