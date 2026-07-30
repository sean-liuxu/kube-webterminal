package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MaxSessionsPerPod is the hard limit of concurrent terminal sessions per pod.
const MaxSessionsPerPod = 3

// IdleTimeout is the duration after which an idle terminal session is closed.
const IdleTimeout = 15 * time.Minute

// TerminalSession represents a single active terminal WebSocket connection.
type TerminalSession struct {
	ID          string
	UserID      string
	Namespace   string
	PodName     string
	Container   string
	Conn        *websocket.Conn
	CreatedAt   time.Time
	LastActive  time.Time
	mu          sync.Mutex
	closed      bool
	CloseCh     chan struct{}
	doneCh      chan struct{}
}

// IsExpired returns true if the session has been idle beyond IdleTimeout.
func (ts *TerminalSession) IsExpired() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return time.Since(ts.LastActive) > IdleTimeout
}

// Touch updates the LastActive timestamp.
func (ts *TerminalSession) Touch() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.LastActive = time.Now()
}

// Close marks the session as closed and signals done channels.
func (ts *TerminalSession) Close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.closed {
		return
	}
	ts.closed = true
	close(ts.CloseCh)
}

// IsClosed returns whether the session has been closed.
func (ts *TerminalSession) IsClosed() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.closed
}

// SessionManager manages all active terminal sessions with per-pod limits.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*TerminalSession         // sessionID -> session
	userSess map[string]map[string]bool          // userID -> set of sessionIDs
	podSess  map[string]map[string]bool          // "ns/pod" -> set of sessionIDs
}

// NewSessionManager creates a new session manager and starts the idle cleanup goroutine.
func NewSessionManager() *SessionManager {
	sm := &SessionManager{
		sessions: make(map[string]*TerminalSession),
		userSess: make(map[string]map[string]bool),
		podSess:  make(map[string]map[string]bool),
	}
	go sm.idleCleanupLoop()
	return sm
}

// idleCleanupLoop periodically checks for expired sessions and cleans them up.
func (sm *SessionManager) idleCleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		sm.cleanupExpiredSessions()
	}
}

// cleanupExpiredSessions removes all sessions that have exceeded the idle timeout.
func (sm *SessionManager) cleanupExpiredSessions() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for id, sess := range sm.sessions {
		if sess.IsExpired() {
			sess.Close()
			if sess.Conn != nil {
				sess.Conn.Close()
			}
			sm.removeSessionLocked(id, sess.UserID)
		}
	}
}

// CountForPod returns the number of active sessions for a given ns/pod.
func (sm *SessionManager) CountForPod(ns, pod string) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	key := ns + "/" + pod
	if m, ok := sm.podSess[key]; ok {
		return len(m)
	}
	return 0
}

// CountForUser returns the number of active sessions for a given user.
func (sm *SessionManager) CountForUser(userID string) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if m, ok := sm.userSess[userID]; ok {
		return len(m)
	}
	return 0
}

// AddSession registers a new session. Returns an error if the user has reached the limit.
func (sm *SessionManager) AddSession(sess *TerminalSession) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	podKey := sess.Namespace + "/" + sess.PodName
	if m, ok := sm.podSess[podKey]; ok && len(m) >= MaxSessionsPerPod {
		return fmt.Errorf("Pod %s 已达到最大终端会话数上限 (%d)，请关闭其他终端后重试", podKey, MaxSessionsPerPod)
	}

	sess.CloseCh = make(chan struct{})
	sess.doneCh = make(chan struct{})
	sess.LastActive = time.Now()
	sess.CreatedAt = time.Now()

	sm.sessions[sess.ID] = sess
	if sm.userSess[sess.UserID] == nil {
		sm.userSess[sess.UserID] = make(map[string]bool)
	}
	sm.userSess[sess.UserID][sess.ID] = true

	podKey = sess.Namespace + "/" + sess.PodName
	if sm.podSess[podKey] == nil {
		sm.podSess[podKey] = make(map[string]bool)
	}
	sm.podSess[podKey][sess.ID] = true

	return nil
}

// RemoveSession removes a session by ID.
func (sm *SessionManager) RemoveSession(sessionID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sess, ok := sm.sessions[sessionID]
	if !ok {
		return
	}
	sess.Close()
	sm.removeSessionLocked(sessionID, sess.UserID)
}

// removeSessionLocked removes a session (caller must hold sm.mu write lock).
func (sm *SessionManager) removeSessionLocked(sessionID, userID string) {
	delete(sm.sessions, sessionID)
	if m, ok := sm.userSess[userID]; ok {
		delete(m, sessionID)
		if len(m) == 0 {
			delete(sm.userSess, userID)
		}
	}
	// Clean up podSess
	for pk, pm := range sm.podSess {
		if pm[sessionID] {
			delete(pm, sessionID)
			if len(pm) == 0 {
				delete(sm.podSess, pk)
			}
			break
		}
	}
}

// GetSession retrieves a session by ID.
func (sm *SessionManager) GetSession(sessionID string) *TerminalSession {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.sessions[sessionID]
}

// RemoveAllUserSessions removes all sessions belonging to a user.
func (sm *SessionManager) RemoveAllUserSessions(userID string) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sessIDs, ok := sm.userSess[userID]
	if !ok {
		return 0
	}

	count := len(sessIDs)
	for sid := range sessIDs {
		if sess, exists := sm.sessions[sid]; exists {
			sess.Close()
			if sess.Conn != nil {
				sess.Conn.Close()
			}
		}
		delete(sm.sessions, sid)
	}
	delete(sm.userSess, userID)
	return count
}

// ActiveSessionCount returns the total number of active sessions.
func (sm *SessionManager) ActiveSessionCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}
