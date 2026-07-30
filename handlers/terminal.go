package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"

	"github.com/kube-webterminal/k8s"
	"github.com/kube-webterminal/session"
)

// Global session manager instance.
var SessionMgr = session.NewSessionManager()

// K8sClientProvider is a function that returns a K8s client for a given user.
type K8sClientProvider func(impersonateUser string) (*k8s.Client, error)

// TerminalHandler handles WebSocket terminal connections.
type TerminalHandler struct {
	ClientProvider K8sClientProvider
	upgrader       websocket.Upgrader
}

// NewTerminalHandler creates a new terminal handler.
func NewTerminalHandler(provider K8sClientProvider) *TerminalHandler {
	return &TerminalHandler{
		ClientProvider: provider,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // Production: verify Origin header
			},
		},
	}
}

// WSMessage represents the JSON message structure between frontend and backend.
type WSMessage struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Command   string `json:"command,omitempty"`
	Cols      int    `json:"cols,omitempty"`
	Rows      int    `json:"rows,omitempty"`
	Data      string `json:"data,omitempty"`
	Message   string `json:"message,omitempty"`
}

// TerminalSizePayload is used for resize messages.
type terminalSizePayload struct {
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

// ServeHTTP handles the WebSocket upgrade and terminal session lifecycle.
func (th *TerminalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := th.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ERROR] WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	// Set initial read deadline for the init message.
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	// Wait for initialization message.
	_, rawMsg, err := conn.ReadMessage()
	if err != nil {
		log.Printf("[ERROR] Failed to read init message: %v", err)
		th.sendError(conn, "初始化消息读取失败: "+err.Error())
		return
	}

	var initMsg WSMessage
	if err := json.Unmarshal(rawMsg, &initMsg); err != nil {
		log.Printf("[ERROR] Failed to parse init message: %v", err)
		th.sendError(conn, "初始化消息解析失败: "+err.Error())
		return
	}

	if initMsg.Type != "init" {
		th.sendError(conn, "首条消息必须为 init 类型")
		return
	}

	// Validate required fields.
	if initMsg.Namespace == "" || initMsg.Pod == "" {
		th.sendError(conn, "namespace 和 pod 参数不能为空")
		return
	}

	// Extract or generate user ID.
	userID := initMsg.UserID
	if userID == "" {
		userID = r.RemoteAddr
	}

	// Default container.
	container := initMsg.Container
	if container == "" {
		// The K8s exec API will use the default container if empty.
	}

	// Default command.
	command := initMsg.Command
	if command == "" {
		command = "/bin/bash"
	}

	// Check session limit for this pod.
	if SessionMgr.CountForPod(initMsg.Namespace, initMsg.Pod) >= session.MaxSessionsPerPod {
		th.sendError(conn, fmt.Sprintf("Pod %s/%s 已达到最大终端会话数上限 (%d)，请关闭其他终端后重试", initMsg.Namespace, initMsg.Pod, session.MaxSessionsPerPod))
		return
	}

	// Verify pod exists before creating session.
	k8sClient, err := th.ClientProvider(userID)
	if err == nil {
		_, err = k8sClient.Clientset.CoreV1().Pods(initMsg.Namespace).Get(context.Background(), initMsg.Pod, metav1.GetOptions{})
		if err != nil {
			th.sendError(conn, fmt.Sprintf("Pod %s/%s 不存在或无法访问: %v", initMsg.Namespace, initMsg.Pod, err))
			return
		}
	}

	// Generate session ID.
	sessionID := fmt.Sprintf("%s-%s-%d", userID, initMsg.Pod, time.Now().UnixNano())

	// Create session entry.
	ts := &session.TerminalSession{
		ID:        sessionID,
		UserID:    userID,
		Namespace: initMsg.Namespace,
		PodName:   initMsg.Pod,
		Container: initMsg.Container,
		Conn:      conn,
	}

	if err := SessionMgr.AddSession(ts); err != nil {
		th.sendError(conn, "创建会话失败: "+err.Error())
		return
	}

	// Send init success.
	th.sendJSON(conn, WSMessage{
		Type:      "ready",
		SessionID: sessionID,
		Message:   "终端会话已建立",
	})

	log.Printf("[INFO] Terminal session started: user=%s namespace=%s pod=%s session=%s total=%d",
		userID, initMsg.Namespace, initMsg.Pod, sessionID, SessionMgr.ActiveSessionCount())

	// Start the terminal exec session.
	th.runTerminal(conn, ts, command)

	// Cleanup on exit.
	SessionMgr.RemoveSession(sessionID)
	log.Printf("[INFO] Terminal session cleaned: user=%s session=%s remaining=%d",
		userID, sessionID, SessionMgr.ActiveSessionCount())
}

// runTerminal establishes the K8s exec connection and bridges data between WebSocket and exec.
func (th *TerminalHandler) runTerminal(conn *websocket.Conn, ts *session.TerminalSession, command string) {
	k8sClient, err := th.ClientProvider(ts.UserID)
	if err != nil {
		th.sendError(conn, "K8s客户端创建失败: "+err.Error())
		return
	}

	// Create pipes for stdin/stdout.
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TerminalSizeQueue is shared: exec goroutine uses it, WS reader feeds resize.
	tsq := k8s.NewTerminalSizeQueue()

	// Start K8s exec in a goroutine.
	execErrCh := make(chan error, 1)
	go func() {
		execErrCh <- k8sClient.ExecShell(ctx, &k8s.ExecOptions{
			Namespace:         ts.Namespace,
			PodName:           ts.PodName,
			Container:         ts.Container,
			Command:           []string{command},
			Stdin:             stdinReader,
			Stdout:            stdoutWriter,
			Stderr:            stdoutWriter, // Merge stderr into stdout for terminal display
			TTY:               true,
			TerminalSizeQueue: tsq,
		})
		stdoutWriter.Close()
	}()


	// Track goroutines for orderly cleanup.
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: WebSocket -> stdin
	go func() {
		defer wg.Done()
		defer stdinWriter.Close()

		for {
			select {
			case <-ts.CloseCh:
				return
			case <-ctx.Done():
				return
			default:
			}

			conn.SetReadDeadline(time.Now().Add(session.IdleTimeout))
			msgType, rawMsg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
					log.Printf("[WARN] WebSocket read error for session %s: %v", ts.ID, err)
				}
				cancel()
				return
			}

			// Ignore control messages.
			if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
				continue
			}

			var msg WSMessage
			if err := json.Unmarshal(rawMsg, &msg); err != nil {
				// Treat as raw stdin data.
				if _, writeErr := stdinWriter.Write(rawMsg); writeErr != nil {
					cancel()
					return
				}
				ts.Touch()
				continue
			}

			switch msg.Type {
			case "stdin":
				if _, writeErr := stdinWriter.Write([]byte(msg.Data)); writeErr != nil {
					cancel()
					return
				}
				ts.Touch()
			case "resize":
				// Forward resize to the K8s exec PTY via TerminalSizeQueue.
				tsq.SetSize(uint16(msg.Cols), uint16(msg.Rows))
				ts.Touch()
			case "ping":
				th.sendJSON(conn, WSMessage{Type: "pong"})
				ts.Touch()
			case "close":
				log.Printf("[INFO] Client requested close for session %s", ts.ID)
				cancel()
				return
			default:
				// Unknown message type, ignore.
			}
		}
	}()

	// Goroutine 2: stdout -> WebSocket
	go func() {
		defer wg.Done()

		buf := make([]byte, 4096)
		for {
			select {
			case <-ts.CloseCh:
				return
			case <-ctx.Done():
				return
			default:
			}

			n, readErr := stdoutReader.Read(buf)
			if n > 0 {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if writeErr := conn.WriteMessage(websocket.BinaryMessage, buf[:n]); writeErr != nil {
					log.Printf("[WARN] WebSocket write error for session %s: %v", ts.ID, writeErr)
					cancel()
					return
				}
				ts.Touch()
			}
			if readErr != nil {
				if readErr != io.EOF {
					log.Printf("[WARN] stdout read error for session %s: %v", ts.ID, readErr)
				}
				cancel()
				return
			}
		}
	}()

	// Start heartbeat goroutine.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if ts.IsExpired() {
					log.Printf("[INFO] Session %s expired (idle timeout), closing", ts.ID)
					th.sendJSON(conn, WSMessage{
						Type:    "timeout",
						Message: "会话因超时已断开",
					})
					cancel()
					return
				}
				// Send ping to client.
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second)); err != nil {
					log.Printf("[WARN] Ping failed for session %s: %v", ts.ID, err)
					cancel()
					return
				}
			case <-ts.CloseCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for both bridge goroutines to complete.
	wg.Wait()

	// Wait for exec to finish (or timeout).
	select {
	case execErr := <-execErrCh:
		if execErr != nil {
			log.Printf("[INFO] Exec session %s ended with error: %v", ts.ID, execErr)
		}
	case <-time.After(3 * time.Second):
		log.Printf("[INFO] Exec session %s cleanup timeout", ts.ID)
	}
}

// sendJSON sends a JSON message to the WebSocket client.
func (th *TerminalHandler) sendJSON(conn *websocket.Conn, msg WSMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal message: %v", err)
		return
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("[WARN] WebSocket write error: %v", err)
	}
	// Clear deadline so subsequent stdout writes are not affected.
	conn.SetWriteDeadline(time.Time{})
}

// sendError sends an error message to the WebSocket client and closes the connection.
func (th *TerminalHandler) sendError(conn *websocket.Conn, message string) {
	th.sendJSON(conn, WSMessage{
		Type:    "error",
		Message: message,
	})
	conn.Close()
}

// GetSessionInfo returns information about active sessions (for admin/debug).
func GetSessionInfo() map[string]interface{} {
	return map[string]interface{}{
		"total_sessions":     SessionMgr.ActiveSessionCount(),
		"max_per_user":       session.MaxSessionsPerPod,
		"idle_timeout_min":   int(session.IdleTimeout.Minutes()),
	}
}

// SanitizeParam sanitizes user input to prevent injection.
func SanitizeParam(s string) string {
	// Remove any characters that aren't alphanumeric, dash, dot, or underscore.
	result := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			return r
		}
		return -1
	}, s)
	return result
}

// RemoteCommand defines the terminal resize protocol.
var _ remotecommand.TerminalSizeQueue = (*k8s.TerminalSizeQueue)(nil)
