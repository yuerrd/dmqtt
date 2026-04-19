package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed admin
var adminFS embed.FS

func adminHandler() http.Handler {
	sub, _ := fs.Sub(adminFS, "admin")
	fileServer := http.FileServer(http.FS(sub))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/admin/")
		if path == "" {
			path = "index.html"
		}

		// Try to open the file; if it doesn't exist, serve index.html (SPA routing)
		if _, err := sub.Open(path); err != nil {
			r.URL.Path = "/admin/"
		}
		http.StripPrefix("/admin/", fileServer).ServeHTTP(w, r)
	})
}
