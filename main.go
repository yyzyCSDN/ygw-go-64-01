package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"packetreplay/internal/capture"
	"packetreplay/internal/config"
	"packetreplay/internal/dedup"
	"packetreplay/internal/filter"
	"packetreplay/internal/index"
	"packetreplay/internal/model"
	"packetreplay/internal/reassemble"
	"packetreplay/internal/replay"
	"packetreplay/internal/store"
	"packetreplay/internal/web"
)

func main() {
	configPath := flag.String("config", "", "配置文件路径，缺省使用默认配置")
	demo := flag.Bool("demo", false, "启动时生成演示抓包数据并自动采集")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	cfg.WithEnv(os.Getenv)
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}
	dataDir, err := config.EnsureDataDir(cfg)
	if err != nil {
		log.Fatalf("prepare data dir: %v", err)
	}

	if *demo {
		if err := ensureDemoCapture(cfg); err != nil {
			log.Fatalf("prepare demo capture: %v", err)
		}
	}

	st := store.NewStore(cfg.ChunkSize)
	deduper := dedup.New(cfg.DedupWindow)
	st.SetKeyReleaser(deduper)

	idx := index.New()
	idx.SetLookup(func(id string) (model.Packet, bool) { return st.Get(id) })

	var source capture.Source
	if len(cfg.PcapFiles) > 0 {
		source = capture.NewFileSource(cfg.PcapFiles)
	} else {
		samplePath := config.SamplePcapPath(dataDir)
		if fileExists(samplePath) {
			source = capture.NewFileSource([]string{samplePath})
		} else {
			source = capture.NewLiveSource(
				model.StreamKey{SrcIP: "10.0.0.10", SrcPort: 5000, DstIP: "10.0.0.20", DstPort: 8080, Proto: model.ProtoTCP},
				time.Now().UTC().Add(-time.Hour),
				500*time.Millisecond,
				1000,
				[]byte("demo-traffic-payload"),
			)
		}
	}
	captureSvc := capture.NewService(source, st, deduper)
	assembler := reassemble.NewAssembler(cfg.ReassemblyLimit)
	captureSvc.SetAssembler(reassemble.NewService(assembler, st))

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			deduper.PurgeExpired(time.Now())
		}
	}()

	cursorStore := replay.NewFileCursorStore(config.CursorPath(dataDir))
	replaySvc := replay.NewReplayService(
		st,
		idx,
		replay.NewScheduler(cfg.ReplayChunkSize),
		cursorStore,
		func(model.Packet) error { return nil },
	)

	f := filter.New()
	f.AddRule(mustRule("dstport in 80 8080"))

	webDir := "web"
	if _, err := os.Stat(webDir); err != nil {
		webDir = "."
	}
	handler := web.NewServer(captureSvc, replaySvc, st, idx, deduper, f, webDir).Handler()

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if *demo {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			if err := captureSvc.Run(ctx); err != nil {
				log.Printf("demo capture stopped: %v", err)
				return
			}
			if _, err := captureSvc.RebuildIndex(idx, st); err != nil {
				log.Printf("demo index rebuild: %v", err)
			}
		}()
	} else if len(cfg.PcapFiles) > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			if err := captureSvc.Process(ctx); err != nil {
				log.Printf("capture process: %v", err)
			}
		}()
	}

	go func() {
		log.Printf("PacketReplay listening on %s (data dir %s)", cfg.Addr, dataDir)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	log.Println("PacketReplay stopped")
}

func ensureDemoCapture(cfg config.Config) error {
	path := config.SamplePcapPath(cfg.DataDir)
	if fileExists(path) {
		return nil
	}
	lines := []string{
		"# 演示抓包：两个 TCP 流，其中一个带分片",
		"ts=2026-08-24T09:00:00.100Z key=10.0.0.1:1000-10.0.0.2:80/tcp seq=1 frag=0 off=0 total=0 hex=0102030405",
		"ts=2026-08-24T09:00:00.150Z key=10.0.0.1:1000-10.0.0.2:80/tcp seq=2 frag=0 off=0 total=0 hex=0a0b0c0d0e0f",
		"ts=2026-08-24T09:00:00.200Z key=10.0.0.3:2000-10.0.0.4:8080/tcp seq=10 frag=1 off=0 total=2 hex=11223344",
		"ts=2026-08-24T09:00:00.210Z key=10.0.0.3:2000-10.0.0.4:8080/tcp seq=10 frag=1 off=1 total=2 hex=55667788",
		"ts=2026-08-24T09:00:00.300Z key=10.0.0.5:3000-10.0.0.6:53/udp seq=7 frag=0 off=0 total=0 hex=deadbeef",
	}
	raw := ""
	for _, line := range lines {
		raw += line + "\n"
	}
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		return fmt.Errorf("write demo capture: %w", err)
	}
	return nil
}

func mustRule(text string) filter.Rule {
	rule, err := filter.ParseRule(text)
	if err != nil {
		panic(err)
	}
	return rule
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
