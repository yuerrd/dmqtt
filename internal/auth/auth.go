package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Authenticator interface for user authentication
type Authenticator interface {
	Authenticate(username string, password []byte) bool
}

// Authorizer interface for authorization checks
type Authorizer interface {
	Authorize(username string, topic string, action string) bool
}

// ACLRule represents an access control rule
type ACLRule struct {
	Topic  string `json:"topic"`
	Access string `json:"access"`
}

// User represents a user with credentials and ACL rules
type User struct {
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	TenantID     string    `json:"tenant_id"`
	ACL          []ACLRule `json:"acl"`
}

// credentialsFile represents the JSON structure of the credentials file
type credentialsFile struct {
	Users []User `json:"users"`
}

// CredentialStore implements both Authenticator and Authorizer interfaces
type CredentialStore struct {
	users map[string]*User
}

// LoadCredentials loads user credentials from a JSON file
func LoadCredentials(path string) (*CredentialStore, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read credentials file: %v", err)
	}

	var creds credentialsFile
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("failed to parse credentials file: %v", err)
	}

	store := &CredentialStore{
		users: make(map[string]*User),
	}

	// Validate and load users
	for i, user := range creds.Users {
		// Validate username
		if user.Username == "" {
			return nil, fmt.Errorf("username cannot be empty")
		}

		// Check for duplicate usernames
		if _, exists := store.users[user.Username]; exists {
			return nil, fmt.Errorf("duplicate username: %s", user.Username)
		}

		// Validate password hash
		if user.PasswordHash == "" {
			return nil, fmt.Errorf("password_hash cannot be empty for user: %s", user.Username)
		}

		// Validate ACL rules
		for _, rule := range user.ACL {
			if rule.Access != "publish" && rule.Access != "subscribe" {
				return nil, fmt.Errorf("invalid access type '%s' for user %s: must be 'publish' or 'subscribe'", rule.Access, user.Username)
			}
		}

		// Store user (use a pointer to the slice element)
		userCopy := creds.Users[i]
		store.users[user.Username] = &userCopy
	}

	return store, nil
}

// Authenticate implements the Authenticator interface
func (cs *CredentialStore) Authenticate(username string, password []byte) bool {
	user, exists := cs.users[username]
	if !exists {
		// Run bcrypt anyway to prevent timing attacks
		bcrypt.CompareHashAndPassword([]byte("$2a$10$dummy.hash.to.prevent.timing.attacks"), password)
		return false
	}

	err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), password)
	return err == nil
}

// Authorize implements the Authorizer interface
func (cs *CredentialStore) Authorize(username string, topic string, action string) bool {
	user, exists := cs.users[username]
	if !exists {
		return false
	}

	// Check each ACL rule
	for _, rule := range user.ACL {
		if rule.Access == action && topicMatch(rule.Topic, topic) {
			return true
		}
	}

	// Default deny
	return false
}

// NoopAuth implements both Authenticator and Authorizer interfaces with permissive behavior
type NoopAuth struct{}

// Authenticate always returns true for NoopAuth
func (n *NoopAuth) Authenticate(username string, password []byte) bool {
	return true
}

// Authorize always returns true for NoopAuth
func (n *NoopAuth) Authorize(username string, topic string, action string) bool {
	return true
}

// TenantResolver resolves a username to a tenant ID.
type TenantResolver interface {
	ResolveTenant(username string) string
}

// ResolveTenant returns the tenant ID for the given username.
func (cs *CredentialStore) ResolveTenant(username string) string {
	user, exists := cs.users[username]
	if !exists {
		return ""
	}
	return user.TenantID
}

// ResolveTenant always returns empty string (default tenant).
func (n *NoopAuth) ResolveTenant(username string) string {
	return ""
}

// topicMatch checks if a topic name matches a subscription filter.
// This is a private copy of the TopicMatch function from internal/broker/topic_matcher.go
// to avoid circular dependencies.
func topicMatch(filter, topic string) bool {
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