package broker

import "testing"

func TestSubscriptionIndex_AddAndMatch(t *testing.T) {
	idx := NewSubscriptionIndex()

	idx.Add("client-1", "sensor/+/temp", 1)
	idx.Add("client-2", "sensor/room1/temp", 0)
	idx.Add("client-3", "#", 0)

	matches := idx.Match("sensor/room1/temp")

	if len(matches) != 3 {
		t.Fatalf("Match count = %d, want 3. Got: %v", len(matches), matchClientIDs(matches))
	}

	clientIDs := matchClientIDs(matches)
	for _, expected := range []string{"client-1", "client-2", "client-3"} {
		if !contains(clientIDs, expected) {
			t.Errorf("Expected %q in matches, got %v", expected, clientIDs)
		}
	}
}

func TestSubscriptionIndex_Remove(t *testing.T) {
	idx := NewSubscriptionIndex()

	idx.Add("client-1", "sensor/temp", 0)
	idx.Add("client-2", "sensor/temp", 1)

	idx.Remove("client-1", "sensor/temp")

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("Match count = %d, want 1", len(matches))
	}
	if matches[0].ClientID != "client-2" {
		t.Errorf("Remaining match = %q, want %q", matches[0].ClientID, "client-2")
	}
}

func TestSubscriptionIndex_RemoveAll(t *testing.T) {
	idx := NewSubscriptionIndex()

	idx.Add("client-1", "a/b", 0)
	idx.Add("client-1", "c/d", 1)
	idx.Add("client-2", "a/b", 0)

	idx.RemoveAll("client-1")

	matches := idx.Match("a/b")
	if len(matches) != 1 {
		t.Fatalf("Match count = %d, want 1", len(matches))
	}
	if matches[0].ClientID != "client-2" {
		t.Errorf("Remaining = %q, want %q", matches[0].ClientID, "client-2")
	}

	matches = idx.Match("c/d")
	if len(matches) != 0 {
		t.Errorf("Match count for c/d = %d, want 0", len(matches))
	}
}

func TestSubscriptionIndex_QoSUpgrade(t *testing.T) {
	idx := NewSubscriptionIndex()

	idx.Add("client-1", "sensor/temp", 0)
	idx.Add("client-1", "sensor/temp", 1)

	matches := idx.Match("sensor/temp")
	if len(matches) != 1 {
		t.Fatalf("Match count = %d, want 1 (deduplicated)", len(matches))
	}
	if matches[0].QoS != 1 {
		t.Errorf("QoS = %d, want 1 (upgraded)", matches[0].QoS)
	}
}

func TestSubscriptionIndex_NoDoubleMatch(t *testing.T) {
	idx := NewSubscriptionIndex()

	idx.Add("client-1", "sensor/#", 0)
	idx.Add("client-1", "sensor/temp", 1)

	matches := idx.Match("sensor/temp")
	clientCount := 0
	for _, m := range matches {
		if m.ClientID == "client-1" {
			clientCount++
		}
	}
	if clientCount != 1 {
		t.Errorf("client-1 matched %d times, want 1", clientCount)
	}
}

func TestSubscriptionIndex_AllFilters(t *testing.T) {
	idx := NewSubscriptionIndex()
	idx.Add("client-1", "sensor/temp", 1)
	idx.Add("client-2", "sensor/temp", 0)
	idx.Add("client-1", "light/#", 2)

	filters := idx.AllFilters()
	if len(filters) != 2 {
		t.Fatalf("expected 2 unique filters, got %d", len(filters))
	}

	expected := map[string]bool{"sensor/temp": false, "light/#": false}
	for _, f := range filters {
		if _, ok := expected[f]; !ok {
			t.Errorf("unexpected filter: %s", f)
		}
		expected[f] = true
	}
	for f, found := range expected {
		if !found {
			t.Errorf("missing filter: %s", f)
		}
	}
}

func matchClientIDs(matches []SubscriptionMatch) []string {
	ids := make([]string, len(matches))
	for i, m := range matches {
		ids[i] = m.ClientID
	}
	return ids
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
