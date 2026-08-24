package web

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"packetreplay/internal/capture"
	"packetreplay/internal/dedup"
	"packetreplay/internal/filter"
	"packetreplay/internal/index"
	"packetreplay/internal/replay"
	"packetreplay/internal/store"
)

// Server 聚合各服务并暴露监控页面与 HTTP API。
type Server struct {
	capture *capture.Service
	replay  *replay.ReplayService
	store   *store.Store
	index   *index.Index
	dedup   *dedup.Dedup
	filter  *filter.Filter
	webDir  string

	captureMu     sync.Mutex
	captureCancel context.CancelFunc
	captureRun    bool
}

// NewServer 构造 HTTP 服务。
func NewServer(
	captureSvc *capture.Service,
	replaySvc *replay.ReplayService,
	store *store.Store,
	idx *index.Index,
	dedup *dedup.Dedup,
	f *filter.Filter,
	webDir string,
) *Server {
	return &Server{
		capture: captureSvc,
		replay:  replaySvc,
		store:   store,
		index:   idx,
		dedup:   dedup,
		filter:  f,
		webDir:  webDir,
	}
}

// Handler 返回注册全部路由的 HTTP Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/web/monitor.html", s.handleMonitor)
	mux.HandleFunc("/api/status", s.handleStatus)
	mux.HandleFunc("/api/streams", s.handleStreams)
	mux.HandleFunc("/api/search", s.handleSearch)
	mux.HandleFunc("/api/replay", s.handleReplay)
	mux.HandleFunc("/api/replay/all", s.handleReplayAll)
	mux.HandleFunc("/api/replay/resume", s.handleReplayResume)
	mux.HandleFunc("/api/replay/stream", s.handleReplayStream)
	mux.HandleFunc("/api/filter", s.handleFilter)
	mux.HandleFunc("/api/index/snapshot", s.handleIndexSnapshot)
	mux.HandleFunc("/api/capture/start", s.handleCaptureStart)
	mux.HandleFunc("/api/capture/stop", s.handleCaptureStop)
	return logRequests(mux)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/web/monitor.html", http.StatusFound)
}

func (s *Server) handleMonitor(w http.ResponseWriter, r *http.Request) {
	path := filepath.Join(s.webDir, "monitor.html")
	raw, err := os.ReadFile(path)
	if err != nil {
		http.Error(w, "monitor page not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}
