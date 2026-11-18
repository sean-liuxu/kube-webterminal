package main

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
)

// staticFS embeds the entire frontend (index.html, xterm assets, etc.) into
// the binary. The build must run from the project root so the static/ dir is
// visible to the go:embed directive.
//
//go:embed all:static
var staticFS embed.FS

// staticHandler serves the frontend. If diskDir is non-empty, files are read
// from disk (handy for local dev without rebuilds); otherwise everything is
// served from the embedded FS and the binary is fully self-contained.
func staticHandler(diskDir string) http.Handler {
	if diskDir != "" {
		log.Printf("[INIT] Static: from disk (%s)", diskDir)
		return http.FileServer(http.Dir(diskDir))
	}

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		// Should never happen: the embed path is fixed at build time.
		log.Fatalf("[FATAL] Embedded static FS broken: %v", err)
	}
	log.Printf("[INIT] Static: embedded (%d bytes total)", fsSize(staticFS))
	return http.FileServer(http.FS(sub))
}

// fsSize sums the size of all regular files in the FS (for the startup log).
func fsSize(fsys fs.FS) int64 {
	var total int64
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if err != nil {
		return 0
	}
	return total
}
