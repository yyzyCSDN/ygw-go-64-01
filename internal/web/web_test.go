package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"packetreplay/internal/capture"
	"packetreplay/internal/dedup"
	"packetreplay/internal/filter"
	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/replay"
	"packetreplay/internal/store"
)

func TestStatusEndpoint(t *testing.T) {
	st := store.NewStore(8)
	deduper := dedup.New(time.Minute)
	st.SetKeyReleaser(deduper)
	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })
	key, _ := model.ParseStreamKey("10.0.0.1:1000-10.0.0.2:80/tcp")
	_ = st.Put(model.NewDatagram(key, 1, time.Now(), []byte("x")))
	captureSvc := capture.NewService(capture.NewLiveSource(key, time.Now(), 0, 1, []byte("x")), st, deduper)
	replaySvc := replay.NewReplayService(st, idx, replay.NewScheduler(4), replay.NewFileCursorStore(t.TempDir()+"/cursor.json"), func(model.Packet) error { return nil })
	f := filter.New()
	server := NewServer(captureSvc, replaySvc, st, idx, deduper, f, ".")

	request := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status code: %d", recorder.Code)
	}
	var payload statusPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if payload.Store.Packets != 1 {
		t.Fatalf("expected 1 packet, got %d", payload.Store.Packets)
	}
}
