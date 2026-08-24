package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/stamp"
	"packetreplay/internal/store"
)

// statusPayload 是监控页面需要的聚合状态。
type statusPayload struct {
	Capture      captureStats `json:"capture"`
	Store        storeStats   `json:"store"`
	Index        indexStats   `json:"index"`
	Dedup        dedupStats   `json:"dedup"`
	Replay       replayStats  `json:"replay"`
	Tasks        []taskStats  `json:"tasks"`
	Filters      []string     `json:"filters"`
	Segments     []string     `json:"segments"`
	Span         string       `json:"capture_span"`
	DedupExpired int          `json:"dedup_expired"`
}

type captureStats struct {
	Received  int    `json:"received"`
	Stored    int    `json:"stored"`
	Failed    int    `json:"failed"`
	Deduped   int    `json:"deduped"`
	Rebuilt   int    `json:"rebuilt"`
	LastError string `json:"last_error,omitempty"`
}

type storeStats struct {
	Packets    int    `json:"packets"`
	Streams    int    `json:"streams"`
	Chunks     int    `json:"chunks"`
	Generation uint64 `json:"generation"`
}

type indexStats struct {
	Version   uint64 `json:"version"`
	Entries   int    `json:"entries"`
	Snapshots int    `json:"snapshots"`
}

type dedupStats struct {
	Keys   int    `json:"keys"`
	Window string `json:"window"`
}

type replayStats struct {
	Tasks   int `json:"tasks"`
	Packets int `json:"packets"`
	Planned int `json:"planned_chunks"`
}

type taskStats struct {
	ID      string `json:"id"`
	State   string `json:"state"`
	Packets int    `json:"packets"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	capStats := s.capture.Stats()
	stStats := s.store.Stats()
	idxStats := s.index
	tasks := make([]taskStats, 0)
	for _, task := range s.replay.Tasks() {
		tasks = append(tasks, taskStats{ID: task.ID, State: string(task.State), Packets: task.Packets})
	}
	filters := make([]string, 0)
	for _, rule := range s.filter.Rules() {
		filters = append(filters, rule.Describe())
	}
	segments := make([]string, 0)
	for _, segment := range s.store.Segments() {
		segments = append(segments, store.DescribeSegment(segment))
	}
	writeJSON(w, statusPayload{
		Capture: captureStats{
			Received:  capStats.Received,
			Stored:    capStats.Stored,
			Failed:    capStats.Failed,
			Deduped:   capStats.Deduped,
			Rebuilt:   capStats.Rebuilt,
			LastError: capStats.LastError,
		},
		Store: storeStats{
			Packets:    stStats.Packets,
			Streams:    stStats.Streams,
			Chunks:     stStats.Chunks,
			Generation: stStats.Generation,
		},
		Index: indexStats{
			Version:   idxStats.CurrentVersion(),
			Entries:   idxStats.Size(),
			Snapshots: len(idxStats.History()),
		},
		Dedup: dedupStats{
			Keys:   s.dedup.Len(),
			Window: s.dedup.WindowDuration().String(),
		},
		Replay: replayStats{
			Tasks:   s.replay.Stats().Tasks,
			Packets: s.replay.Stats().Packets,
			Planned: s.replay.PlannedChunks(),
		},
		Tasks:        tasks,
		Filters:      filters,
		Segments:     segments,
		Span:         stamp.MeasureWindow(s.store.Ordered()).Span().String(),
		DedupExpired: s.dedup.ExpiredCount(time.Now()),
	})
}

func (s *Server) handleStreams(w http.ResponseWriter, r *http.Request) {
	type streamRow struct {
		Key     string `json:"key"`
		Packets int    `json:"packets"`
	}
	rows := make([]streamRow, 0)
	for _, key := range s.store.ListStreams() {
		rows = append(rows, streamRow{Key: key.String(), Packets: len(s.store.GetStream(key))})
	}
	writeJSON(w, rows)
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	text := strings.TrimSpace(r.URL.Query().Get("q"))
	query := index.Query{Text: text}
	packets := s.index.SearchPackets(query)
	writeJSON(w, packetsSummary(packets))
}

func (s *Server) handleReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	service := s.replay
	emitted, err := service.Replay(r.Context(), s.store.Ordered())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"emitted": emitted, "outputs": packetsSummary(service.Outputs())})
}

func (s *Server) handleReplayAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	emitted, err := s.replay.RunTask(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"emitted": emitted, "outputs": packetsSummary(s.replay.Outputs())})
}

func (s *Server) handleReplayResume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	emitted, err := s.replay.Resume(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"emitted": emitted, "outputs": packetsSummary(s.replay.Outputs())})
}

func (s *Server) handleReplayStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key, err := model.ParseStreamKey(r.URL.Query().Get("key"))
	if err != nil {
		http.Error(w, "invalid stream key: "+err.Error(), http.StatusBadRequest)
		return
	}
	emitted, err := s.replay.ReplayStream(r.Context(), key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"emitted": emitted, "outputs": packetsSummary(s.replay.Outputs())})
}

func (s *Server) handleFilter(w http.ResponseWriter, r *http.Request) {
	packets := s.filter.Apply(s.store.Ordered())
	writeJSON(w, map[string]any{"matched": len(packets), "outputs": packetsSummary(packets)})
}

func (s *Server) handleIndexSnapshot(w http.ResponseWriter, r *http.Request) {
	var version uint64
	if _, err := fmt.Sscanf(r.URL.Query().Get("version"), "%d", &version); err != nil {
		http.Error(w, "missing version", http.StatusBadRequest)
		return
	}
	ids := s.index.PacketsAt(version)
	writeJSON(w, map[string]any{"version": version, "count": len(ids), "ids": ids})
}

func (s *Server) handleCaptureStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	if s.captureRun {
		writeJSON(w, map[string]string{"status": "already_running"})
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.captureCancel = cancel
	s.captureRun = true
	go func() {
		_ = s.capture.Run(ctx)
		s.captureMu.Lock()
		s.captureRun = false
		s.captureMu.Unlock()
	}()
	writeJSON(w, map[string]string{"status": "started"})
}

func (s *Server) handleCaptureStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.captureMu.Lock()
	cancel := s.captureCancel
	s.captureMu.Unlock()
	if cancel != nil {
		cancel()
	}
	writeJSON(w, map[string]string{"status": "stopping"})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(value)
}
