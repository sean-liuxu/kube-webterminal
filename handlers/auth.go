package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// AuthHandler provides authentication and authorization endpoints.
type AuthHandler struct {
	// In production, this would be backed by a real auth system.
	// For now, we support token-based auth and rate limiting.
}

// NewAuthHandler creates a new auth handler.
func NewAuthHandler() *AuthHandler {
	return &AuthHandler{}
}

// AuthRequest represents an auth request from the frontend.
type AuthRequest struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Token     string `json:"token"`
	UserID    string `json:"user_id"`
}

// AuthResponse represents the response after authentication.
type AuthResponse struct {
	Success bool   `json:"success"`
	UserID  string `json:"user_id,omitempty"`
	Token   string `json:"token,omitempty"`
	Message string `json:"message,omitempty"`
}

// Rate limiter for terminal creation requests.
type rateLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

var (
	rateLimiters   = make(map[string]*rateLimiterEntry)
	rateLimiterMu  sync.Mutex
	rateLimitRate  = rate.Limit(2)  // 2 requests per second
	rateLimitBurst = 3              // burst of 3
)

// getRateLimiter returns a rate limiter for the given key.
func getRateLimiter(key string) *rate.Limiter {
	rateLimiterMu.Lock()
	defer rateLimiterMu.Unlock()

	entry, ok := rateLimiters[key]
	if !ok || time.Since(entry.lastSeen) > 5*time.Minute {
		rateLimiters[key] = &rateLimiterEntry{
			limiter:  rate.NewLimiter(rateLimitRate, rateLimitBurst),
			lastSeen: time.Now(),
		}
		entry = rateLimiters[key]
	} else {
		entry.lastSeen = time.Now()
	}
	return entry.limiter
}

// cleanupRateLimiters periodically removes old entries.
func init() {
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rateLimiterMu.Lock()
			for key, entry := range rateLimiters {
				if time.Since(entry.lastSeen) > 15*time.Minute {
					delete(rateLimiters, key)
				}
			}
			rateLimiterMu.Unlock()
		}
	}()
}

// HandleAuth processes authentication requests.
func (ah *AuthHandler) HandleAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, AuthResponse{
			Success: false,
			Message: "仅支持 POST 请求",
		})
		return
	}

	var req AuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, AuthResponse{
			Success: false,
			Message: "请求解析失败: " + err.Error(),
		})
		return
	}

	// In production, validate against your auth system (LDAP, OIDC, etc.).
	// For now, accept any non-empty username or token.
	if req.Username == "" && req.Token == "" {
		writeJSON(w, http.StatusUnauthorized, AuthResponse{
			Success: false,
			Message: "用户名或Token不能为空",
		})
		return
	}

	userID := req.Username
	if userID == "" {
		userID = req.UserID
	}
	if userID == "" {
		userID = "anonymous"
	}

	// Generate a session token (in production, use JWT or similar).
	sessionToken := generateToken(userID)

	log.Printf("[INFO] User authenticated: %s", userID)

	writeJSON(w, http.StatusOK, AuthResponse{
		Success: true,
		UserID:  userID,
		Token:   sessionToken,
		Message: "认证成功",
	})
}

// HandleStatus returns server and session status.
func (ah *AuthHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "running",
		"session": GetSessionInfo(),
	})
}

// CheckRateLimit checks if the request should be rate limited.
func CheckRateLimit(key string) bool {
	limiter := getRateLimiter(key)
	return limiter.Allow()
}

// ValidateRequest validates terminal connection parameters.
func ValidateRequest(namespace, pod, container string) (string, bool) {
	if namespace == "" {
		return "namespace 不能为空", false
	}
	if pod == "" {
		return "pod 名称不能为空", false
	}

	// Sanitize inputs to prevent injection.
	namespace = SanitizeParam(namespace)
	pod = SanitizeParam(pod)
	if container != "" {
		container = SanitizeParam(container)
	}

	if namespace == "" || pod == "" {
		return "参数包含非法字符", false
	}

	return "", true
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// generateToken creates a simple token. In production, use JWT.
func generateToken(userID string) string {
	return fmt.Sprintf("tok-%s-%d", userID, time.Now().UnixNano())
}
