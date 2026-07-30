package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	utilExec "k8s.io/client-go/util/exec"
)

// Client wraps the Kubernetes clientset and REST config.
type Client struct {
	Clientset  *kubernetes.Clientset
	RestConfig *rest.Config
}

// NewClient creates a new K8s client. If kubeconfigPath is empty, in-cluster config is used.
func NewClient(kubeconfigPath, impersonateUser string) (*Client, error) {
	var cfg *rest.Config
	var err error

	if kubeconfigPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build k8s config: %w", err)
	}

	// Apply impersonation if specified.
	if impersonateUser != "" {
		cfg.Impersonate = rest.ImpersonationConfig{
			UserName: impersonateUser,
		}
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %w", err)
	}

	return &Client{Clientset: clientset, RestConfig: cfg}, nil
}

// ExecOptions holds parameters for a pod exec session.
type ExecOptions struct {
	Namespace         string
	PodName           string
	Container         string
	Command           []string
	Stdin             io.Reader
	Stdout            io.Writer
	Stderr            io.Writer
	TTY               bool
	TerminalSizeQueue *TerminalSizeQueue
}

// TerminalSizeQueue wraps remotecommand.TerminalSizeQueue.
type TerminalSizeQueue struct {
	resizeCh chan remotecommand.TerminalSize
}

// NewTerminalSizeQueue creates a new size queue.
func NewTerminalSizeQueue() *TerminalSizeQueue {
	return &TerminalSizeQueue{
		resizeCh: make(chan remotecommand.TerminalSize, 16),
	}
}

// SetSize sends a new terminal size to the queue.
func (tsq *TerminalSizeQueue) SetSize(cols, rows uint16) {
	select {
	case tsq.resizeCh <- remotecommand.TerminalSize{Width: cols, Height: rows}:
	default:
		// Drop resize if channel is full — kubelet only needs the latest.
	}
}

// Next implements remotecommand.TerminalSizeQueue.
func (tsq *TerminalSizeQueue) Next() *remotecommand.TerminalSize {
	select {
	case s := <-tsq.resizeCh:
		return &s
	default:
		return nil
	}
}

// shellWrapper sets TERM + LS_COLORS via env. For bash, silently injects color
// functions via export -f so they survive exec into the interactive shell.
// Zero file writes, zero PTY echo, zero flash.
func shellWrapper(shellCmd []string) []string {
	shell := "/bin/bash"
	if len(shellCmd) > 0 {
		shell = shellCmd[0]
	}

	lsc := "di=01;34:ln=01;36:ex=01;32:so=01;35:pi=01;35:bd=01;33:cd=01;33:" +
		"or=01;31:mi=01;31:*.tar=01;33:*.tgz=01;33:*.zip=01;33:*.gz=01;33:" +
		"*.bz2=01;33:*.xz=01;33:*.7z=01;33:*.rar=01;33:*.deb=01;33:*.rpm=01;33:" +
		"*.jpg=01;35:*.png=01;35:*.gif=01;35:*.svg=01;35:*.webp=01;35:" +
		"*.mp3=01;36:*.wav=01;36:*.flac=01;36:*.ogg=01;36:" +
		"*.mp4=01;35:*.mkv=01;35:*.avi=01;35:*.mov=01;35:" +
		"*.pdf=01;33:*.txt=00;37:*.md=00;37:*.log=00;37"

	// For bash: use export -f to inject color functions that survive exec.
	// The -c commands run silently (no PTY echo), then exec bash starts interactive.
	if strings.Contains(shell, "bash") {
		return []string{
			"env",
			"TERM=xterm-256color",
			"LS_COLORS=" + lsc,
			shell,
			"-c",
			"ls(){ command ls --color=auto \"$@\";};export -f ls;" +
				"grep(){ command grep --color=auto \"$@\";};export -f grep;" +
				"ll(){ command ls -alF --color=auto \"$@\";};export -f ll;" +
				"exec " + shell,
		}
	}

	// For /bin/sh (dash) etc: just set env, no injection.
	return append([]string{"env", "TERM=xterm-256color", "LS_COLORS=" + lsc}, shellCmd...)
}

// ExecShell opens an exec session with the specified command.
// It first tries the preferred command, then falls back to /bin/sh on failure.
// All commands are wrapped with env to set UTF-8 locale for CJK support.
func (c *Client) ExecShell(ctx context.Context, opts *ExecOptions) error {
	commandsToTry := [][]string{
		shellWrapper(opts.Command),
		shellWrapper([]string{"/bin/sh"}),
	}

	var lastErr error
	for i, cmd := range commandsToTry {
		if i > 0 {
			// For fallback, check if the command exists via 'which'.
			// If 'which' itself is missing (distroless), still try execWithOptions below.
			shell := cmd[len(cmd)-1]; _ = c.checkCommandExists(ctx, opts.Namespace, opts.PodName, opts.Container, shell)
		}
		opts.Command = cmd
		err := c.execWithOptions(ctx, opts)
		if err == nil {
			return nil
		}
		if isCommandNotFoundError(err) {
			lastErr = err
			continue
		}
		return err
	}
	return fmt.Errorf("all shell attempts failed, last error: %w", lastErr)
}

// checkCommandExists checks if a command exists in the container via 'which'.
func (c *Client) checkCommandExists(ctx context.Context, namespace, podName, container, command string) error {
	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: container,
			Command:   []string{"which", command},
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, parameterCodec())

	// req.URL() returns *url.URL in client-go rest.Request.
	executor, err := remotecommand.NewSPDYExecutor(c.RestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor for check: %w", err)
	}

	var stdout, stderr strings.Builder
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return fmt.Errorf("command %q not found: %w", command, err)
	}
	if strings.TrimSpace(stdout.String()) == "" && strings.TrimSpace(stderr.String()) != "" {
		return fmt.Errorf("command %q not found", command)
	}
	return nil
}

// execWithOptions is the core exec implementation using SPDY executor.
func (c *Client) execWithOptions(ctx context.Context, opts *ExecOptions) error {
	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(opts.PodName).
		Namespace(opts.Namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: opts.Container,
			Command:   opts.Command,
			Stdin:     opts.Stdin != nil,
			Stdout:    opts.Stdout != nil,
			Stderr:    opts.Stderr != nil,
			TTY:       opts.TTY,
		}, parameterCodec())

	executor, err := remotecommand.NewSPDYExecutor(c.RestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	streamOpts := remotecommand.StreamOptions{
		Stdin:  opts.Stdin,
		Stdout: opts.Stdout,
		Stderr: opts.Stderr,
		Tty:    opts.TTY,
	}
	if opts.TerminalSizeQueue != nil {
		streamOpts.TerminalSizeQueue = opts.TerminalSizeQueue
	}

	return executor.StreamWithContext(ctx, streamOpts)
}

// CopyToPod copies a tar stream from stdin into a pod via exec.
// cmd should be something like ["tar", "xmf", "-", "-C", "/dest"].
func (c *Client) CopyToPod(ctx context.Context, namespace, pod, container string, cmd []string, stdin io.Reader) error {
	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     true,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, parameterCodec())

	executor, err := remotecommand.NewSPDYExecutor(c.RestConfig, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("failed to create executor: %w", err)
	}

	return executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: io.Discard,
		Stderr: io.Discard,
		Tty:    false,
	})
}

// parameterCodec returns a runtime.ParameterCodec using the core API scheme.
// Uses runtime.NewParameterCodec which is the standard way to get a
// ParameterCodec from a scheme — works across all client-go versions.
func parameterCodec() runtime.ParameterCodec {
	return runtime.NewParameterCodec(kubernetesscheme.Scheme)
}

// isCommandNotFoundError checks if the error indicates the command was not found.
func isCommandNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	patterns := []string{
		"command not found",
		"no such file or directory",
		"executable file not found",
		"exit code 127",
		"exit code 126",
	}
	for _, p := range patterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// IsExecErrorCode checks for specific exec error codes that are safe to retry.
func IsExecErrorCode(err error) bool {
	if err == nil {
		return false
	}
	if exitErr, ok := err.(utilExec.CodeExitError); ok {
		code := exitErr.ExitStatus()
		return code == 126 || code == 127
	}
	return false
}

// ExecSimple runs a simple command in a pod and returns stdout/stderr as strings.
func (c *Client) ExecSimple(namespace, pod, container string, cmd []string) (stdout, stderr string, err error) {
	req := c.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&v1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, parameterCodec())

	executor, err := remotecommand.NewSPDYExecutor(c.RestConfig, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("create executor: %w", err)
	}

	var outBuf, errBuf bytes.Buffer
	err = executor.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &outBuf,
		Stderr: &errBuf,
		Tty:    false,
	})
	return outBuf.String(), errBuf.String(), err
}
