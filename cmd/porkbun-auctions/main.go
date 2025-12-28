package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"porkbun-parser2/internal/checkpoint"
	"porkbun-parser2/internal/output"
	"porkbun-parser2/internal/porkbun"
)

func main() {
	startURL := flag.String("start-url", "https://porkbun.com/auctions", "Starting URL for auctions crawl")
	outPath := flag.String("out", "", "Output path (default: stdout)")
	delay := flag.Duration("delay", 750*time.Millisecond, "Delay between page fetches")
	timeout := flag.Duration("timeout", 20*time.Second, "HTTP timeout")
	maxPages := flag.Int("max-pages", 0, "Max pages to crawl (0 = unlimited)")
	checkpointPath := flag.String("checkpoint", ".porkbun-auctions.checkpoint.json", "Checkpoint file path")
	resume := flag.Bool("resume", true, "Resume from checkpoint if present")
	userAgent := flag.String("user-agent", "porkbun-auctions/1.0 (+https://porkbun.com/auctions)", "HTTP User-Agent")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prevent abrupt termination when piping to tools like `head`.
	signal.Ignore(syscall.SIGPIPE)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	var (
		outW io.WriteCloser
		err  error
	)
	if *outPath == "" {
		outW = os.Stdout
	} else {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil && filepath.Dir(*outPath) != "." {
			fmt.Fprintln(os.Stderr, "failed to create output dir:", err)
			os.Exit(2)
		}
		outW, err = os.Create(*outPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to open output:", err)
			os.Exit(2)
		}
		defer outW.Close()
	}

	jsonl := output.NewJSONLWriter(outW)
	defer jsonl.Close()

	cp := checkpoint.NewFileCheckpoint(*checkpointPath)

	urlToCrawl := *startURL
	if *resume {
		if st, ok, err := cp.Load(); err != nil {
			fmt.Fprintln(os.Stderr, "failed to load checkpoint:", err)
			os.Exit(2)
		} else if ok && st.LastURL != "" {
			urlToCrawl = st.LastURL
			fmt.Fprintln(os.Stderr, "resuming from checkpoint:", urlToCrawl)
		}
	}

	client := porkbun.NewClient(porkbun.ClientConfig{
		UserAgent: *userAgent,
		Timeout:   *timeout,
	})

	crawler := porkbun.NewCrawler(client, porkbun.CrawlerConfig{
		Delay:    *delay,
		MaxPages: *maxPages,
	})

	stats, err := crawler.Crawl(ctx, urlToCrawl, func(item porkbun.AuctionItem) error {
		return jsonl.Write(item)
	}, func(state checkpoint.State) error {
		return cp.Save(state)
	})
	if err != nil && ctx.Err() == nil {
		if errors.Is(err, syscall.EPIPE) {
			// Common when piping to `head` or a consumer that exits early.
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "crawl failed:", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "done: pages=%d rows=%d last_url=%s\n", stats.Pages, stats.Rows, stats.LastURL)
}
