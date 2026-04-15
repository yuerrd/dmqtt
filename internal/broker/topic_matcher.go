package broker

import "strings"

// TopicMatch checks if a topic name matches a subscription filter.
// Follows MQTT 3.1.1 §4.7 Topic matching rules.
func TopicMatch(filter, topic string) bool {
	// $SYS topics: wildcard filters starting with + or # must not match
	if len(topic) > 0 && topic[0] == '$' {
		if len(filter) > 0 && (filter[0] == '+' || filter[0] == '#') {
			return false
		}
	}

	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	for i := 0; i < len(filterParts); i++ {
		if filterParts[i] == "#" {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if filterParts[i] == "+" {
			continue
		}
		if filterParts[i] != topicParts[i] {
			return false
		}
	}

	return len(filterParts) == len(topicParts)
}

// TopicValid checks if a topic name is valid for PUBLISH.
func TopicValid(topic string) bool {
	if topic == "" {
		return false
	}
	return !strings.Contains(topic, "+") && !strings.Contains(topic, "#")
}

// TopicFilterValid checks if a subscription topic filter is syntactically valid.
func TopicFilterValid(filter string) bool {
	if filter == "" {
		return false
	}
	parts := strings.Split(filter, "/")
	for i, part := range parts {
		if strings.Contains(part, "#") {
			if part != "#" || i != len(parts)-1 {
				return false
			}
		}
		if strings.Contains(part, "+") {
			if part != "+" {
				return false
			}
		}
	}
	return true
}
