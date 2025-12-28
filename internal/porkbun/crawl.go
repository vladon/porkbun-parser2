package porkbun

import (
	"context"
	"errors"
	"fmt"
	"time"

	"porkbun-parser2/internal/checkpoint"
)

type CrawlerConfig struct {
	Delay    time.Duration
	MaxPages int

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
