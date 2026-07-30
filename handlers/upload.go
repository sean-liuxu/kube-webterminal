package handlers

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kube-webterminal/k8s"
)

const (
	// MaxUploadSize 5GB default upload limit.
	MaxUploadSize = 5 * 1024 * 1024 * 1024
)

// UploadHandler handles file/folder uploads to pods via K8s exec + tar pipe.
type UploadHandler struct {
	ClientProvider K8sClientProvider
	mu             sync.Mutex
	activeUploads  map[string]*UploadProgress
}

// UploadProgress tracks upload progress for polling.
type UploadProgress struct {
	ID        string    `json:"id"`
	Pod       string    `json:"pod"`
	Namespace string    `json:"namespace"`
	Path      string    `json:"path"`
	FileName  string    `json:"file_name"`
	Total     int64     `json:"total"`
	Written   int64     `json:"written"`
	Done      bool      `json:"done"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

// NewUploadHandler creates a new upload handler.
func NewUploadHandler(provider K8sClientProvider) *UploadHandler {
	return &UploadHandler{
		ClientProvider: provider,
		activeUploads:  make(map[string]*UploadProgress),
	}
}

// ServeHTTP handles file upload requests.
// POST /api/upload
// Form fields: namespace, pod, container, path (destination dir), file (multipart)
func (uh *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "仅支持 POST"})
		return
	}

	// Limit upload size (5GB)
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	namespace := r.FormValue("namespace")
	pod := r.FormValue("pod")
	container := r.FormValue("container")
	destPath := r.FormValue("path")

	if namespace == "" || pod == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace 和 pod 不能为空"})
		return
	}
	if destPath == "" {
		destPath = "/tmp"
	}

	// Parse multipart form (max 32MB in memory, rest on disk)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "解析上传文件失败: " + err.Error()})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "读取文件失败: " + err.Error()})
		return
	}
	// Don't defer file.Close() — the goroutine needs it open.
	// Prevent Go's HTTP server from cleaning up the multipart temp files
	// before the upload goroutine finishes reading them.
	form := r.MultipartForm
	r.MultipartForm = nil // detach from request, prevents auto-cleanup

	// Check file size (5GB hard limit)
	if header.Size > MaxUploadSize {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"error": fmt.Sprintf("文件大小 %d 超过上限 (%d bytes)", header.Size, MaxUploadSize),
		})
		return
	}

	// Create K8s client
	k8sClient, err := uh.ClientProvider("")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "K8s 客户端创建失败: " + err.Error()})
		return
	}

	// Generate upload ID for progress tracking
	uploadID := fmt.Sprintf("up-%s-%s-%d", pod, header.Filename, time.Now().UnixNano())

	progress := &UploadProgress{
		ID:        uploadID,
		Pod:       pod,
		Namespace: namespace,
		Path:      destPath,
		FileName:  header.Filename,
		Total:     header.Size,
		StartedAt: time.Now(),
	}

	uh.mu.Lock()
	uh.activeUploads[uploadID] = progress
	uh.mu.Unlock()

	// Run upload in background (keep temp files alive until done)
	go func() {
		defer file.Close()
		if form != nil {
			defer form.RemoveAll()
		}
		defer func() {
			progress.Done = true
			time.AfterFunc(5*time.Minute, func() {
				uh.mu.Lock()
				delete(uh.activeUploads, uploadID)
				uh.mu.Unlock()
			})
		}()

		log.Printf("[INFO] Upload %s starting: %s (%d bytes) -> %s/%s:%s", uploadID, header.Filename, header.Size, namespace, pod, destPath)
		err := uh.uploadToPod(k8sClient, file, header, namespace, pod, container, destPath, progress)
		if err != nil {
			progress.Error = err.Error()
			log.Printf("[ERROR] Upload %s FAILED: %v", uploadID, err)
		} else {
			progress.Written = progress.Total
			log.Printf("[INFO] Upload %s DONE: %s -> %s/%s:%s (%d bytes)", uploadID, header.Filename, namespace, pod, destPath, progress.Written)
		}
	}()

	// Return upload ID for progress polling
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"upload_id": uploadID,
		"file_name": header.Filename,
		"size":      header.Size,
		"path":      destPath,
	})
}

// uploadToPod streams a file into a pod via K8s exec + tar pipe.
func (uh *UploadHandler) uploadToPod(client *k8s.Client, file io.Reader, header *multipart.FileHeader, namespace, pod, container, destPath string, progress *UploadProgress) error {
	// Only tar — works in any image with GNU/busybox tar (no sh/mkdir needed).
	// Destination path is embedded in the tar header so tar creates parent dirs itself.
	tarCmd := []string{"tar", "xmf", "-"}

	// We need to wrap the uploaded file(s) in a tar archive and pipe through exec.
	// Create a pipe: we write tar to the pipe, K8s exec reads from it.
	pr, pw := io.Pipe()

	// Goroutine 1: Create tar archive from uploaded file and write to pipe
	go func() {
		defer pw.Close()
		tw := tar.NewWriter(pw)
		defer tw.Close()

		// Build full tar path: destPath + / + base filename (tar auto-creates parent dirs).
		// Only use the base name — destPath already includes subdirectory from frontend.
		_, baseName := filepath.Split(header.Filename)
		tarName := strings.TrimRight(destPath, "/") + "/" + baseName
		hdr := &tar.Header{
			Name: tarName,
			Size: header.Size,
			Mode: 0644,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			pw.CloseWithError(fmt.Errorf("tar write header: %w", err))
			return
		}

		// Track progress while copying
		written, err := io.Copy(tw, io.TeeReader(file, &writeTracker{progress: progress}))
		if err != nil {
			pw.CloseWithError(fmt.Errorf("tar write data: %w", err))
			return
		}
		_ = written
	}()

	// Goroutine 2: K8s exec reads from pipe
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	return client.CopyToPod(ctx, namespace, pod, container, tarCmd, pr)
}

// ProgressHandler returns upload progress for polling.
// GET /api/upload/progress?id=<upload_id>
func (uh *UploadHandler) ProgressHandler(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("id")
	if uploadID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "缺少 upload_id"})
		return
	}

	uh.mu.Lock()
	progress, ok := uh.activeUploads[uploadID]
	uh.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"done":  true,
			"error": "upload not found or expired",
		})
		return
	}

	writeJSON(w, http.StatusOK, progress)
}

// writeTracker implements io.Writer to track upload progress.
type writeTracker struct {
	progress *UploadProgress
}

func (wt *writeTracker) Write(p []byte) (int, error) {
	n := len(p)
	wt.progress.Written += int64(n)
	return n, nil
}

// FileListHandler lists files in a container directory via exec.
// GET /api/files?namespace=ns&pod=pod&container=c&path=/tmp
func (uh *UploadHandler) FileListHandler(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	pod := r.URL.Query().Get("pod")
	container := r.URL.Query().Get("container")
	path := r.URL.Query().Get("path")
	if path == "" { path = "/tmp" }
	if namespace == "" || pod == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace 和 pod 不能为空"})
		return
	}
	k8sClient, err := uh.ClientProvider("")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "K8s 客户端创建失败: " + err.Error()})
		return
	}
	cmd := []string{"ls", "-la", "--color=never", "--time-style=long-iso", path}
	stdout, stderr, err := k8sClient.ExecSimple(namespace, pod, container, cmd)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("执行失败: %v, stderr: %s", err, stderr)})
		return
	}
	type fileInfo struct {
		Name       string `json:"name"`
		Size       string `json:"size"`
		IsDir      bool   `json:"is_dir"`
		ModifyTime string `json:"modify_time"`
	}
	var files []fileInfo
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "total ") { continue }
		parts := strings.Fields(line)
		if len(parts) < 8 { continue }
		name := parts[7] // long-iso: 8th field is filename
		isDir := strings.HasPrefix(parts[0], "d")
		name = strings.TrimRight(name, "*/=>@|")
		if name == "." || name == ".." { continue }
		fsize := parts[4]
		if isDir { fsize = "-" }
		mtime := parts[5] + " " + parts[6]; files = append(files, fileInfo{Name: name, Size: fsize, IsDir: isDir, ModifyTime: mtime})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"path": path, "files": files})
}

// DownloadHandler streams a file from a container to the HTTP response.
// GET /api/download?namespace=ns&pod=pod&container=c&path=/tmp/file.txt
func (uh *UploadHandler) DownloadHandler(w http.ResponseWriter, r *http.Request) {
	namespace := r.URL.Query().Get("namespace")
	pod := r.URL.Query().Get("pod")
	container := r.URL.Query().Get("container")
	path := r.URL.Query().Get("path")
	if path == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path 不能为空"})
		return
	}
	if namespace == "" || pod == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "namespace 和 pod 不能为空"})
		return
	}

	k8sClient, err := uh.ClientProvider("")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "K8s 客户端创建失败: " + err.Error()})
		return
	}

	// Use tar to read file from container: tar cf - -C / filepath | tar xf - (locally)
	// Simpler: use cat and capture stdout
	cmd := []string{"cat", path}
	stdout, stderr, err := k8sClient.ExecSimple(namespace, pod, container, cmd)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("执行失败: %v, stderr: %s", err, stderr),
		})
		return
	}

	// Let browser download via <a download> — no Content-Disposition to avoid save dialog
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(stdout)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(stdout))
}
