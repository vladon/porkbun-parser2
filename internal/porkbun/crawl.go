package porkbun

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"porkbun-parser2/internal/checkpoint"
)

type Logger interface {
	Printf(format string, v ...any)
}

type CrawlerConfig struct {
	Delay    time.Duration
	MaxPages int
	// Concurrency > 1 enables concurrent fetching using deterministic `?from=` pagination.
	// This is much faster but may trigger 429s; backoff/retries will kick in automatically.
	Concurrency int
	// ContinueOnError makes the crawler skip pages that fail after retries (instead of aborting).
	ContinueOnError bool
	// LogEvery enables periodic progress logging (to Logger) when > 0.
	LogEvery time.Duration
	Logger   Logger

	// Retry config
	MaxRetries int
}

type CrawlStats struct {
	Pages   int
	Rows    int
	Skipped int
	Errors  int
	LastURL string
}

type Crawler struct {
	client *Client
	cfg    CrawlerConfig
}

func NewCrawler(client *Client, cfg CrawlerConfig) *Crawler {
	if cfg.Delay < 0 {
		cfg.Delay = 0
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	return &Crawler{client: client, cfg: cfg}
}

func (c *Crawler) Crawl(
	ctx context.Context,
	startURL string,
	onItem func(AuctionItem) error,
	onCheckpoint func(checkpoint.State) error,
) (CrawlStats, error) {
	if c.cfg.Concurrency > 1 {
		return c.crawlConcurrent(ctx, startURL, onItem, onCheckpoint)
	}
	var stats CrawlStats

	visited := map[string]struct{}{}
	cur := startURL
	started := time.Now()
	if c.cfg.Logger != nil && c.cfg.LogEvery > 0 {
		ticker := time.NewTicker(c.cfg.LogEvery)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					el := time.Since(started).Seconds()
					rps := 0.0
					if el > 0 {
						rps = float64(stats.Rows) / el
					}
					c.cfg.Logger.Printf("progress: pages=%d rows=%d skipped=%d errors=%d rps=%.1f last_url=%s", stats.Pages, stats.Rows, stats.Skipped, stats.Errors, rps, stats.LastURL)
				}
			}
		}()
	}

	for cur != "" {
		if c.cfg.MaxPages > 0 && stats.Pages >= c.cfg.MaxPages {
			stats.LastURL = cur
			return stats, nil
		}
		if _, ok := visited[cur]; ok {
			return stats, fmt.Errorf("pagination loop detected at %s", cur)
		}
		visited[cur] = struct{}{}

		select {
		case <-ctx.Done():
			stats.LastURL = cur
			return stats, ctx.Err()
		default:
		}

		parsed, body, err := c.fetchAndParseWithRetries(ctx, cur)
		if err != nil {
			stats.Errors++
			stats.LastURL = cur
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("warn: failed page url=%s err=%v", cur, err)
			}
			if !c.cfg.ContinueOnError {
				return stats, err
			}
			stats.Skipped++
			// Try to keep moving forward using known pagination patterns.
			if next := parsed.NextURL; next != "" {
				cur = next
				continue
			}
			if next := bumpFrom(cur, 100); next != "" && next != cur {
				cur = next
				continue
			}
			return stats, nil
		}
		if len(parsed.Items) == 0 {
			stats.Errors++
			stats.LastURL = cur
			reason := classifyNoRows(body)
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("warn: empty page url=%s reason=%s", cur, reason)
			}
			if !c.cfg.ContinueOnError {
				return stats, fmt.Errorf("no auction rows found at %s reason=%s", cur, reason)
			}
			stats.Skipped++
			if next := parsed.NextURL; next != "" {
				cur = next
				continue
			}
			if next := bumpFrom(cur, 100); next != "" && next != cur {
				cur = next
				continue
			}
			return stats, nil
		}

		now := time.Now().UTC().Format(time.RFC3339Nano)
		for i := range parsed.Items {
			parsed.Items[i].PageURL = cur
			parsed.Items[i].RowIndex = i
			parsed.Items[i].ScrapedAt = now
			if err := onItem(parsed.Items[i]); err != nil {
				stats.LastURL = cur
				return stats, err
			}
		}

		stats.Pages++
		stats.Rows += len(parsed.Items)

		next := parsed.NextURL
		stats.LastURL = next
		if stats.LastURL == "" {
			stats.LastURL = cur
		}

		if onCheckpoint != nil {
			_ = onCheckpoint(checkpoint.State{
				LastURL: stats.LastURL,
				Pages:   stats.Pages,
				Rows:    stats.Rows,
				Skipped: stats.Skipped,
				Errors:  stats.Errors,
			})
		}

		if next == "" {
			return stats, nil
		}

		if c.cfg.Delay > 0 {
			t := time.NewTimer(c.cfg.Delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return stats, ctx.Err()
			case <-t.C:
			}
		}

		cur = next
	}

	return stats, nil
}

type pageTask struct {
	url  string
	from int
}

func (c *Crawler) crawlConcurrent(
	ctx context.Context,
	startURL string,
	onItem func(AuctionItem) error,
	onCheckpoint func(checkpoint.State) error,
) (CrawlStats, error) {
	var stats CrawlStats
	var pagesDone int64
	var rowsDone int64
	var skippedDone int64
	var errorsDone int64

	baseURL, startFrom, err := normalizeBaseAndFrom(startURL)
	if err != nil {
		return stats, err
	}

	// Discover total results. Sometimes the first page is temporarily "empty" due to soft-blocking;
	// in that case, probe a few deterministic offsets until we find a page with rows + total count.
	var parsed ParsedPage
	var discovered bool
	probeLimit := 20
	for i := 0; i < probeLimit; i++ {
		probeURL := startURL
		if i > 0 {
			probeURL = withFrom(baseURL, startFrom+i*100)
		}
		p, body, err := c.fetchAndParseWithRetries(ctx, probeURL)
		if err != nil {
			atomic.AddInt64(&errorsDone, 1)
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("warn: probe failed url=%s err=%v", probeURL, err)
			}
			continue
		}
		if len(p.Items) == 0 || p.TotalResults <= 0 {
			atomic.AddInt64(&errorsDone, 1)
			if c.cfg.Logger != nil {
				c.cfg.Logger.Printf("warn: probe empty url=%s reason=%s total=%d", probeURL, classifyNoRows(body), p.TotalResults)
			}
			continue
		}
		parsed = p
		discovered = true
		break
	}
	if !discovered {
		return stats, fmt.Errorf("failed to discover total results after %d probes starting at %s", probeLimit, startURL)
	}

	step := 100
	totalPages := (parsed.TotalResults + step - 1) / step
	var offsets []int
	for p := 0; p < totalPages; p++ {
		offsets = append(offsets, p*step)
	}
	sort.Ints(offsets)

	// Resume: begin at startFrom (derived from startURL or checkpoint-resume URL).
	var tasks []pageTask
	for _, off := range offsets {
		if off < startFrom {
			continue
		}
		tasks = append(tasks, pageTask{
			url:  withFrom(baseURL, off),
			from: off,
		})
		if c.cfg.MaxPages > 0 && len(tasks) >= c.cfg.MaxPages {
			break
		}
	}
	if len(tasks) == 0 {
		stats.LastURL = startURL
		return stats, nil
	}

	// Track completion so checkpoint can advance to the next contiguous "from" offset.
	done := map[int]bool{}
	nextExpected := startFrom
	var doneMu sync.Mutex
	var cbMu sync.Mutex
	started := time.Now()
	totalTasks := len(tasks)

	if c.cfg.Logger != nil && c.cfg.LogEvery > 0 {
		ticker := time.NewTicker(c.cfg.LogEvery)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p := atomic.LoadInt64(&pagesDone)
					r := atomic.LoadInt64(&rowsDone)
					sk := atomic.LoadInt64(&skippedDone)
					er := atomic.LoadInt64(&errorsDone)
					el := time.Since(started).Seconds()
					rps := 0.0
					if el > 0 {
						rps = float64(r) / el
					}
					c.cfg.Logger.Printf("progress: pages=%d/%d rows=%d skipped=%d errors=%d rps=%.1f next_from=%d", p, totalTasks, r, sk, er, rps, nextExpected)
				}
			}
		}()
	}

	taskCh := make(chan pageTask)
	errCh := make(chan error, 1)

	worker := func() {
		for t := range taskCh {
			if ctx.Err() != nil {
				return
			}

			parsed, body, err := c.fetchAndParseWithRetries(ctx, t.url)
			if err != nil {
				atomic.AddInt64(&errorsDone, 1)
				if c.cfg.Logger != nil {
					c.cfg.Logger.Printf("warn: failed page url=%s err=%v", t.url, err)
				}
				if !c.cfg.ContinueOnError {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				atomic.AddInt64(&skippedDone, 1)
				markDoneAndCheckpoint(c, &doneMu, done, &nextExpected, baseURL, 100, &cbMu, onCheckpoint, &pagesDone, &rowsDone, &skippedDone, &errorsDone, t.from)
				continue
			}
			if len(parsed.Items) == 0 {
				atomic.AddInt64(&errorsDone, 1)
				reason := classifyNoRows(body)
				if c.cfg.Logger != nil {
					c.cfg.Logger.Printf("warn: empty page url=%s reason=%s", t.url, reason)
				}
				if !c.cfg.ContinueOnError {
					select {
					case errCh <- fmt.Errorf("no auction rows found at %s reason=%s", t.url, reason):
					default:
					}
					return
				}
				atomic.AddInt64(&skippedDone, 1)
				markDoneAndCheckpoint(c, &doneMu, done, &nextExpected, baseURL, 100, &cbMu, onCheckpoint, &pagesDone, &rowsDone, &skippedDone, &errorsDone, t.from)
				continue
			}

			now := time.Now().UTC().Format(time.RFC3339Nano)
			cbMu.Lock()
			for i := range parsed.Items {
				parsed.Items[i].PageURL = t.url
				parsed.Items[i].RowIndex = i
				parsed.Items[i].ScrapedAt = now
				if err := onItem(parsed.Items[i]); err != nil {
					cbMu.Unlock()
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
			cbMu.Unlock()

			atomic.AddInt64(&pagesDone, 1)
			atomic.AddInt64(&rowsDone, int64(len(parsed.Items)))

			// Update checkpoint to next contiguous offset.
			markDoneAndCheckpoint(c, &doneMu, done, &nextExpected, baseURL, 100, &cbMu, onCheckpoint, &pagesDone, &rowsDone, &skippedDone, &errorsDone, t.from)

			if c.cfg.Delay > 0 {
				tm := time.NewTimer(c.cfg.Delay)
				select {
				case <-ctx.Done():
					tm.Stop()
					return
				case <-tm.C:
				}
			}
		}
	}

	workers := c.cfg.Concurrency
	if workers > 64 {
		workers = 64
	}
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			worker()
		}()
	}

	go func() {
		defer close(taskCh)
		for _, t := range tasks {
			select {
			case <-ctx.Done():
				return
			case taskCh <- t:
			}
		}
	}()

	wg.Wait()

	select {
	case err := <-errCh:
		stats.Pages = int(atomic.LoadInt64(&pagesDone))
		stats.Rows = int(atomic.LoadInt64(&rowsDone))
		stats.Skipped = int(atomic.LoadInt64(&skippedDone))
		stats.Errors = int(atomic.LoadInt64(&errorsDone))
		stats.LastURL = withFrom(baseURL, nextExpected)
		return stats, err
	default:
	}

	stats.Pages = int(atomic.LoadInt64(&pagesDone))
	stats.Rows = int(atomic.LoadInt64(&rowsDone))
	stats.Skipped = int(atomic.LoadInt64(&skippedDone))
	stats.Errors = int(atomic.LoadInt64(&errorsDone))
	stats.LastURL = withFrom(baseURL, nextExpected)
	return stats, nil
}

func (c *Crawler) getWithRetries(ctx context.Context, url string) ([]byte, int, error) {
	var lastErr error
	var lastStatus int

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(250*(1<<min(attempt, 6))) * time.Millisecond
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, 0, ctx.Err()
			case <-t.C:
			}
		}

		body, status, err := c.client.Get(url)
		lastStatus = status
		if err == nil {
			return body, status, nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}

		if status == 429 || status >= 500 {
			continue
		}
		break
	}

	if lastErr == nil {
		lastErr = errors.New("unknown error")
	}
	return nil, lastStatus, lastErr
}

func (c *Crawler) fetchAndParseWithRetries(ctx context.Context, pageURL string) (ParsedPage, []byte, error) {
	var lastBody []byte
	var lastErr error

	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(250*(1<<min(attempt, 6))) * time.Millisecond
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return ParsedPage{}, nil, ctx.Err()
			case <-t.C:
			}
		}

		body, _, err := c.getWithRetries(ctx, pageURL)
		if err != nil {
			lastErr = err
			continue
		}
		lastBody = body

		parsed, err := ParseAuctionsPage(pageURL, body)
		if err != nil {
			return ParsedPage{}, body, err
		}
		if len(parsed.Items) == 0 {
			lastErr = fmt.Errorf("no auction rows found (reason=%s)", classifyNoRows(body))
			// retry a few times, this can be transient soft-blocking
			continue
		}
		return parsed, body, nil
	}

	if lastErr == nil {
		lastErr = errors.New("unknown error")
	}
	return ParsedPage{}, lastBody, lastErr
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func normalizeBaseAndFrom(startURL string) (base string, from int, err error) {
	u, err := url.Parse(startURL)
	if err != nil {
		return "", 0, err
	}
	q := u.Query()
	from = 0
	if v := strings.TrimSpace(q.Get("from")); v != "" {
		from, _ = strconv.Atoi(strings.ReplaceAll(v, ",", ""))
	}
	q.Del("from")
	u.RawQuery = q.Encode()
	return u.String(), from, nil
}

func withFrom(baseURL string, from int) string {
	u, err := url.Parse(baseURL)
	if err != nil {
		return baseURL
	}
	q := u.Query()
	if from <= 0 {
		q.Del("from")
	} else {
		q.Set("from", strconv.Itoa(from))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func bumpFrom(cur string, step int) string {
	u, err := url.Parse(cur)
	if err != nil {
		return ""
	}
	q := u.Query()
	from := 0
	if v := strings.TrimSpace(q.Get("from")); v != "" {
		from, _ = strconv.Atoi(strings.ReplaceAll(v, ",", ""))
	}
	from += step
	q.Set("from", strconv.Itoa(from))
	u.RawQuery = q.Encode()
	return u.String()
}

func classifyNoRows(body []byte) string {
	s := strings.ToLower(string(body))
	switch {
	case strings.Contains(s, "captcha"):
		return "captcha"
	case strings.Contains(s, "access denied"):
		return "access_denied"
	case strings.Contains(s, "cloudflare"):
		return "cloudflare"
	case strings.Contains(s, "<title>") && strings.Contains(s, "porkbun"):
		return "html_no_table"
	default:
		return "unknown"
	}
}

func markDoneAndCheckpoint(
	c *Crawler,
	doneMu *sync.Mutex,
	done map[int]bool,
	nextExpected *int,
	baseURL string,
	step int,
	cbMu *sync.Mutex,
	onCheckpoint func(checkpoint.State) error,
	pagesDone *int64,
	rowsDone *int64,
	skippedDone *int64,
	errorsDone *int64,
	from int,
) {
	if onCheckpoint == nil {
		return
	}
	doneMu.Lock()
	done[from] = true
	for done[*nextExpected] {
		*nextExpected += step
	}
	nextURL := withFrom(baseURL, *nextExpected)
	doneMu.Unlock()

	cbMu.Lock()
	_ = onCheckpoint(checkpoint.State{
		LastURL: nextURL,
		Pages:   int(atomic.LoadInt64(pagesDone)),
		Rows:    int(atomic.LoadInt64(rowsDone)),
		Skipped: int(atomic.LoadInt64(skippedDone)),
		Errors:  int(atomic.LoadInt64(errorsDone)),
	})
	cbMu.Unlock()
}
