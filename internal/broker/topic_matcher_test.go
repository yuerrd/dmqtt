package broker

import "testing"

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		filter string
		topic  string
		match  bool
	}{
		{"sensor/temp", "sensor/temp", true},
		{"sensor/temp", "sensor/humidity", false},
		{"sensor/+/temp", "sensor/1/temp", true},
		{"sensor/+/temp", "sensor/abc/temp", true},
		{"sensor/+/temp", "sensor/1/2/temp", false},
		{"+/temp", "sensor/temp", true},
		{"+/+", "a/b", true},
		{"+/+", "a/b/c", false},
		{"sensor/#", "sensor", true},
		{"sensor/#", "sensor/temp", true},
		{"sensor/#", "sensor/1/temp", true},
		{"#", "anything", true},
		{"#", "a/b/c/d", true},
		{"", "", true},
		{"/", "/", true},
		{"/+", "/finance", true},
		{"+/+/+", "a/b/c", true},
		{"a/b/c/#", "a/b/c", true},
		{"a/b/c/#", "a/b/c/d/e", true},
		{"#", "$SYS/info", false},
		{"+/info", "$SYS/info", false},
		{"$SYS/#", "$SYS/info", true},
		{"$SYS/+", "$SYS/info", true},
	}

	for _, tt := range tests {
		t.Run(tt.filter+"→"+tt.topic, func(t *testing.T) {
			result := TopicMatch(tt.filter, tt.topic)
			if result != tt.match {
				t.Errorf("TopicMatch(%q, %q) = %v, want %v", tt.filter, tt.topic, result, tt.match)
			}
		})
	}
}

func TestTopicValid(t *testing.T) {
	tests := []struct {
		topic string
		valid bool
	}{
		{"sensor/temp", true},
		{"", false},
		{"sensor/+/temp", false},
		{"sensor/#", false},
		{"a/b/c", true},
		{"/", true},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			result := TopicValid(tt.topic)
			if result != tt.valid {
				t.Errorf("TopicValid(%q) = %v, want %v", tt.topic, result, tt.valid)
			}
		})
	}
}

func TestTopicFilterValid(t *testing.T) {
	tests := []struct {
		filter string
		valid  bool
	}{
		{"sensor/temp", true},
		{"sensor/+/temp", true},
		{"sensor/#", true},
		{"#", true},
		{"+", true},
		{"", false},
		{"sensor/#/more", false},
		{"sensor/+extra", false},
	}

	for _, tt := range tests {
		t.Run(tt.filter, func(t *testing.T) {
			result := TopicFilterValid(tt.filter)
			if result != tt.valid {
				t.Errorf("TopicFilterValid(%q) = %v, want %v", tt.filter, result, tt.valid)
			}
		})
	}
}
