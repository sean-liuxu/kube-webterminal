package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kube-webterminal/handlers"
	"github.com/kube-webterminal/k8s"
)

var (
	port        = flag.Int("port", 8080, "HTTP server listen port")
	kubeconfig  = flag.String("kubeconfig", "", "Path to kubeconfig file (empty for in-cluster)")
	tlsCert     = flag.String("tls-cert", "", "TLS certificate file path for WSS")
	tlsKey      = flag.String("tls-key", "", "TLS private key file path for WSS")
	staticDir   = flag.String("static-dir", "", "Static files directory (empty = use frontend embedded in binary)")
	defaultUser = flag.String("default-user", "", "Default impersonation user (empty = no impersonation)")
	enableDebug = flag.Bool("debug", false, "Enable debug endpoints")
)

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("========================================")
	log.Println("  K8s Pod WebTerminal (WebSSH) Gateway")
	log.Println("========================================")
	log.Printf("[INIT] Port: %d", *port)
	if *staticDir != "" {
		log.Printf("[INIT] Static dir: %s (disk mode)", *staticDir)
	} else {
		log.Printf("[INIT] Static dir: (embedded frontend)")
	}
	if *tlsCert != "" {
		log.Printf("[INIT] TLS: enabled (WSS mode)")
	} else {
		log.Println("[INIT] TLS: disabled (WS mode — NOT recommended for production)")
	}
	log.Printf("[INIT] Max sessions per user: %d", 3)
	log.Printf("[INIT] Session idle timeout: %d minutes", 15)

	// Create K8s client provider.
	clientProvider := func(impersonateUser string) (*k8s.Client, error) {
		user := impersonateUser
		if user == "" {
			user = *defaultUser
		}
		return k8s.NewClient(*kubeconfig, user)
	}

	// Verify K8s connectivity at startup.
	testClient, err := clientProvider("")
	if err != nil {
		log.Printf("[WARN] Could not create K8s client: %v", err)
		log.Println("[WARN] Server will start but terminal connections may fail")
	} else {
		ver, err := testClient.Clientset.Discovery().ServerVersion()
		if err != nil {
			log.Printf("[WARN] Could not connect to K8s API: %v", err)
		} else {
			log.Printf("[INIT] K8s API version: %s", ver.GitVersion)
		}
	}

	// Setup routes.
	mux := http.NewServeMux()

	// WebSocket terminal endpoint.
	terminalHandler := handlers.NewTerminalHandler(clientProvider)
	mux.Handle("/ws/terminal", terminalHandler)

	// Auth endpoints.
	authHandler := handlers.NewAuthHandler()
	mux.HandleFunc("/api/auth", authHandler.HandleAuth)
	mux.HandleFunc("/api/status", authHandler.HandleStatus)

	// File handlers.
	uploadHandler := handlers.NewUploadHandler(clientProvider)
	mux.Handle("/api/upload", uploadHandler)
	mux.HandleFunc("/api/upload/progress", uploadHandler.ProgressHandler)
	mux.HandleFunc("/api/files", uploadHandler.FileListHandler)
	mux.HandleFunc("/api/download", uploadHandler.DownloadHandler)

	// Debug endpoints.
	if *enableDebug {
		mux.HandleFunc("/api/debug/sessions", handleDebugSessions)
		mux.HandleFunc("/api/debug/healthz", handleHealthz)
	}

	// Health check.
	mux.HandleFunc("/healthz", handleHealthz)

	// Serve static files (embedded by default, disk override via --static-dir).
	mux.Handle("/", staticHandler(*staticDir))

	// Graceful shutdown.
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      withLogging(mux),
		ReadTimeout:  60 * time.Minute, // Allow large file uploads (5GB)
		WriteTimeout: 0,               // WebSocket connections disable write timeout
		IdleTimeout:  120 * time.Second,
	}

	// Handle shutdown signals.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("[INIT] Server starting on port %d", *port)
		var err error
		if *tlsCert != "" && *tlsKey != "" {
			tlsConfig := &tls.Config{
				MinVersion: tls.VersionTLS12,
			}
			srv.TLSConfig = tlsConfig
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Server failed: %v", err)
		}
	}()

	<-stop
	log.Println("[INFO] Shutting down server...")

	// Clean up all sessions.
	sessionCount := handlers.SessionMgr.ActiveSessionCount()
	if sessionCount > 0 {
		log.Printf("[INFO] Cleaning up %d active sessions...", sessionCount)
	}
}

// --- Debug / Utility Handlers ---

func handleDebugSessions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	info := handlers.GetSessionInfo()
	respJSON(w, http.StatusOK, info)
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	respJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func respJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// withLogging is an HTTP middleware that logs requests.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		if r.URL.Path != "/healthz" {
			log.Printf("[HTTP] %s %s %s %v", r.Method, r.URL.Path, r.RemoteAddr, duration)
		}
	})
}
