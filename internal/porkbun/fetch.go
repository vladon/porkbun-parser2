package porkbun

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type ClientConfig struct {
	UserAgent string
	Timeout   time.Duration
}

type Client struct {
	hc        *http.Client
	userAgent string
}

func NewClient(cfg ClientConfig) *Client {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = "porkbun-auctions/1.0"
	}
	return &Client{
		hc: &http.Client{
			Timeout: timeout,
		},
		userAgent: ua,
	}
}

func (c *Client) Get(url string) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return nil, resp.StatusCode, fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}
