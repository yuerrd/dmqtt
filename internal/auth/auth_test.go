package auth

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// Test helper functions
func hashPassword(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}

func writeAuthFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	err := os.WriteFile(path, []byte(content), 0644)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadCredentials_Valid(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "publish"},
					{"topic": "commands/sensor1/#", "access": "subscribe"}
				]
			},
			{
				"username": "sensor2",
				"password_hash": "` + hashPassword(t, "password456") + `",
				"acl": [
					{"topic": "devices/sensor2/+", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	if len(store.users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(store.users))
	}
	
	// Check first user
	user1, exists := store.users["sensor1"]
	if !exists {
		t.Fatal("sensor1 user not found")
	}
	if user1.Username != "sensor1" {
		t.Errorf("Expected username sensor1, got %s", user1.Username)
	}
	if len(user1.ACL) != 2 {
		t.Errorf("Expected 2 ACL rules, got %d", len(user1.ACL))
	}
	
	// Check second user
	user2, exists := store.users["sensor2"]
	if !exists {
		t.Fatal("sensor2 user not found")
	}
	if len(user2.ACL) != 1 {
		t.Errorf("Expected 1 ACL rule, got %d", len(user2.ACL))
	}
}

func TestLoadCredentials_DuplicateUsers(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": []
			},
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "different") + `",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	_, err := LoadCredentials(path)
	if err == nil {
		t.Fatal("Expected error for duplicate users")
	}
	if err.Error() != "duplicate username: sensor1" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestLoadCredentials_EmptyUsername(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	_, err := LoadCredentials(path)
	if err == nil {
		t.Fatal("Expected error for empty username")
	}
	if err.Error() != "username cannot be empty" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestLoadCredentials_EmptyPasswordHash(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	_, err := LoadCredentials(path)
	if err == nil {
		t.Fatal("Expected error for empty password hash")
	}
	if err.Error() != "password_hash cannot be empty for user: sensor1" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestLoadCredentials_InvalidAccess(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "invalid"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	_, err := LoadCredentials(path)
	if err == nil {
		t.Fatal("Expected error for invalid access")
	}
	if err.Error() != "invalid access type 'invalid' for user sensor1: must be 'publish' or 'subscribe'" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestLoadCredentials_FileNotFound(t *testing.T) {
	_, err := LoadCredentials("/nonexistent/path/auth.json")
	if err == nil {
		t.Fatal("Expected error for nonexistent file")
	}
}

func TestLoadCredentials_InvalidJSON(t *testing.T) {
	path := writeAuthFile(t, `{"users": [invalid json`)
	_, err := LoadCredentials(path)
	if err == nil {
		t.Fatal("Expected error for invalid JSON")
	}
}

func TestAuthenticate_Success(t *testing.T) {
	password := "secret123"
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, password) + `",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	if !store.Authenticate("sensor1", []byte(password)) {
		t.Error("Authentication should have succeeded")
	}
}

func TestAuthenticate_WrongPassword(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	if store.Authenticate("sensor1", []byte("wrong_password")) {
		t.Error("Authentication should have failed for wrong password")
	}
}

func TestAuthenticate_UnknownUser(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": []
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	if store.Authenticate("unknown_user", []byte("any_password")) {
		t.Error("Authentication should have failed for unknown user")
	}
}

func TestAuthorize_ExactTopic(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/temperature", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	if !store.Authorize("sensor1", "devices/sensor1/temperature", "publish") {
		t.Error("Authorization should have succeeded for exact topic match")
	}
	
	if store.Authorize("sensor1", "devices/sensor1/temperature", "subscribe") {
		t.Error("Authorization should have failed for wrong action")
	}
	
	if store.Authorize("sensor1", "devices/sensor1/humidity", "publish") {
		t.Error("Authorization should have failed for different topic")
	}
}

func TestAuthorize_WildcardPlus(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/+", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	// Should match single level
	if !store.Authorize("sensor1", "devices/sensor1/temperature", "publish") {
		t.Error("Authorization should have succeeded for + wildcard match")
	}
	
	if !store.Authorize("sensor1", "devices/sensor1/humidity", "publish") {
		t.Error("Authorization should have succeeded for + wildcard match")
	}
	
	// Should not match multiple levels
	if store.Authorize("sensor1", "devices/sensor1/sub/temperature", "publish") {
		t.Error("Authorization should have failed for + wildcard with multiple levels")
	}
}

func TestAuthorize_WildcardHash(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	// Should match any sub-topic
	if !store.Authorize("sensor1", "devices/sensor1/temperature", "publish") {
		t.Error("Authorization should have succeeded for # wildcard match")
	}
	
	if !store.Authorize("sensor1", "devices/sensor1/sub/temperature", "publish") {
		t.Error("Authorization should have succeeded for # wildcard match with multiple levels")
	}
	
	if !store.Authorize("sensor1", "devices/sensor1", "publish") {
		t.Error("Authorization should have succeeded for # wildcard match at exact level")
	}
	
	// Should not match different prefix
	if store.Authorize("sensor1", "devices/sensor2/temperature", "publish") {
		t.Error("Authorization should have failed for # wildcard with different prefix")
	}
}

func TestAuthorize_Denied(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	// Should deny access to topics not in ACL
	if store.Authorize("sensor1", "commands/sensor1/start", "publish") {
		t.Error("Authorization should have failed for topic not in ACL")
	}
}

func TestAuthorize_WrongAction(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	// Should deny wrong action type
	if store.Authorize("sensor1", "devices/sensor1/temperature", "subscribe") {
		t.Error("Authorization should have failed for wrong action type")
	}
}

func TestAuthorize_UnknownUser(t *testing.T) {
	content := `{
		"users": [
			{
				"username": "sensor1",
				"password_hash": "` + hashPassword(t, "secret123") + `",
				"acl": [
					{"topic": "devices/sensor1/#", "access": "publish"}
				]
			}
		]
	}`
	
	path := writeAuthFile(t, content)
	store, err := LoadCredentials(path)
	if err != nil {
		t.Fatalf("LoadCredentials failed: %v", err)
	}
	
	// Should deny unknown users
	if store.Authorize("unknown_user", "devices/sensor1/temperature", "publish") {
		t.Error("Authorization should have failed for unknown user")
	}
}

func TestNoopAuth(t *testing.T) {
	noop := &NoopAuth{}
	
	// Should always return true for authentication
	if !noop.Authenticate("any_user", []byte("any_password")) {
		t.Error("NoopAuth should always authenticate successfully")
	}
	
	// Should always return true for authorization
	if !noop.Authorize("any_user", "any/topic", "any_action") {
		t.Error("NoopAuth should always authorize successfully")
	}
}