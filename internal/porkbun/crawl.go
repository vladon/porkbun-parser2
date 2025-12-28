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

type CrawlerConfig struct {
	Delay    time.Duration
	MaxPages int
	// Concurrency > 1 enables concurrent fetching using deterministic `?from=` pagination.
	// This is much faster but may trigger 429s; backoff/retries will kick in automatically.
	Concurrency int

	// Retry config
	MaxRetries int
}

type CrawlStats struct {
	Pages   int
	Rows    int
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

		body, _, err := c.getWithRetries(ctx, cur)
		if err != nil {
			stats.LastURL = cur
			return stats, err
		}

		parsed, err := ParseAuctionsPage(cur, body)
		if err != nil {
			stats.LastURL = cur
			return stats, err
		}
		if len(parsed.Items) == 0 {
			return stats, fmt.Errorf("no auction rows found at %s (site may have changed / JS-only)", cur)
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

	baseURL, startFrom, err := normalizeBaseAndFrom(startURL)
	if err != nil {
		return stats, err
	}

	// Fetch one page to learn total results; also validates parsing still works.
	body, _, err := c.getWithRetries(ctx, startURL)
	if err != nil {
		return stats, err
	}
	parsed, err := ParseAuctionsPage(startURL, body)
	if err != nil {
		return stats, err
	}
	if len(parsed.Items) == 0 {
		return stats, fmt.Errorf("no auction rows found at %s (site may have changed / JS-only)", startURL)
	}
	if parsed.TotalResults <= 0 {
		// Fallback: if we can't read total results, we cannot safely precompute page list.
		// In that case, revert to sequential crawl (but keep user’s delay=0 option).
		cSeq := NewCrawler(c.client, CrawlerConfig{
			Delay:      c.cfg.Delay,
			MaxPages:   c.cfg.MaxPages,
			MaxRetries: c.cfg.MaxRetries,
		})
		return cSeq.Crawl(ctx, startURL, onItem, onCheckpoint)
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

	taskCh := make(chan pageTask)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	worker := func() {
		for t := range taskCh {
			if ctx.Err() != nil {
				return
			}

			body, _, err := c.getWithRetries(ctx, t.url)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}

			parsed, err := ParseAuctionsPage(t.url, body)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			if len(parsed.Items) == 0 {
				select {
				case errCh <- fmt.Errorf("no auction rows found at %s (site may have changed / JS-only)", t.url):
				default:
				}
				cancel()
				return
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
					cancel()
					return
				}
			}
			cbMu.Unlock()

			atomic.AddInt64(&pagesDone, 1)
			atomic.AddInt64(&rowsDone, int64(len(parsed.Items)))

			// Update checkpoint to next contiguous offset.
			if onCheckpoint != nil {
				doneMu.Lock()
				done[t.from] = true
				for done[nextExpected] {
					nextExpected += step
				}
				nextURL := withFrom(baseURL, nextExpected)
				doneMu.Unlock()

				cbMu.Lock()
				_ = onCheckpoint(checkpoint.State{
					LastURL: nextURL,
					Pages:   int(atomic.LoadInt64(&pagesDone)),
					Rows:    int(atomic.LoadInt64(&rowsDone)),
				})
				cbMu.Unlock()
			}

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
		stats.LastURL = withFrom(baseURL, nextExpected)
		return stats, err
	default:
	}

	stats.Pages = int(atomic.LoadInt64(&pagesDone))
	stats.Rows = int(atomic.LoadInt64(&rowsDone))
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
