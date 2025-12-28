package porkbun

import (
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

type AuctionItem struct {
	Domain        string  `json:"domain"`
	TLD           string  `json:"tld"`
	TimeLeftRaw   string  `json:"time_left_raw"`
	StartingPrice string  `json:"starting_price"`
	CurrentBid    string  `json:"current_bid"`
	Bids          int     `json:"bids"`
	DomainAge     int     `json:"domain_age"`
	Revenue       *string `json:"revenue"`
	Visitors      *string `json:"visitors"`

	// Extra (useful) fields
	AuctionID string `json:"auction_id,omitempty"`

	// Crawl metadata
	PageURL   string `json:"page_url"`
	RowIndex  int    `json:"row_index"`
	ScrapedAt string `json:"scraped_at"`
}

type ParsedPage struct {
	Items   []AuctionItem
	NextURL string
}

func ParseAuctionsPage(pageURL string, html []byte) (ParsedPage, error) {
	var out ParsedPage

	raw := string(html)

	// Parse rows from the auctions table body.
	// The markup we saw is stable and simple:
	// <table class="table"> ... <tbody> <tr id="..."><td>...</td> ... </tr> ... </tbody>
	tbodyRe := regexp.MustCompile(`(?is)<table[^>]*class=["'][^"']*\btable\b[^"']*["'][^>]*>.*?<tbody>(.*?)</tbody>`)
	trRe := regexp.MustCompile(`(?is)<tr\b([^>]*)>(.*?)</tr>`)
	tdRe := regexp.MustCompile(`(?is)<td\b[^>]*>(.*?)</td>`)

	m := tbodyRe.FindStringSubmatch(raw)
	if len(m) >= 2 {
		tbody := m[1]
		trMatches := trRe.FindAllStringSubmatch(tbody, -1)
		for i, trm := range trMatches {
			if len(trm) < 3 {
				continue
			}
			attrs := trm[1]
			trInner := trm[2]

			tdMatches := tdRe.FindAllStringSubmatch(trInner, -1)
			if len(tdMatches) < 9 {
				continue
			}

			item := AuctionItem{
				AuctionID: strings.TrimSpace(extractAttr(attrs, "id")),
			}

			item.Domain = cleanText(stripTags(tdMatches[0][1]))
			item.TLD = cleanText(stripTags(tdMatches[1][1]))
			item.TimeLeftRaw = cleanText(stripTags(tdMatches[2][1]))
			item.StartingPrice = cleanText(stripTags(tdMatches[3][1]))
			item.CurrentBid = cleanText(stripTags(tdMatches[4][1]))
			item.Bids = parseIntSafe(cleanText(stripTags(tdMatches[5][1])))
			item.DomainAge = parseIntSafe(cleanText(stripTags(tdMatches[6][1])))

			if v := cleanText(stripTags(tdMatches[7][1])); v != "" && v != "-" {
				item.Revenue = &v
			}
			if v := cleanText(stripTags(tdMatches[8][1])); v != "" && v != "-" {
				item.Visitors = &v
			}

			item.RowIndex = i
			out.Items = append(out.Items, item)
		}
	}

	// Find next link (pagination looks like: <a href='/auctions?from=100'>next</a>)
	nextRe := regexp.MustCompile(`(?is)<a\b[^>]*href=["']([^"']+)["'][^>]*>\s*next\s*</a>`)
	if nm := nextRe.FindStringSubmatch(raw); len(nm) >= 2 {
		out.NextURL = resolveURL(pageURL, strings.TrimSpace(nm[1]))
	}

	return out, nil
}

func resolveURL(baseStr, href string) string {
	bu, err := url.Parse(baseStr)
	if err != nil {
		return href
	}
	ru, err := url.Parse(href)
	if err != nil {
		return href
	}
	return bu.ResolveReference(ru).String()
}

func cleanText(s string) string {
	s = strings.ReplaceAll(s, "\u00a0", " ")
	s = html.UnescapeString(s)
	return strings.TrimSpace(strings.Join(strings.Fields(s), " "))
}

func parseIntSafe(s string) int {
	s = strings.ReplaceAll(s, ",", "")
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

func stripTags(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	inTag := false
	for _, r := range s {
		switch r {
		case '<':
			inTag = true
		case '>':
			inTag = false
		default:
			if !inTag {
				b.WriteRune(r)
			}
		}
	}
	return b.String()
}

func extractAttr(attrs, name string) string {
	// attrs is content between "<tr" and ">" (not including either).
	// Handle id="123" or id='123'
	re := regexp.MustCompile(`(?is)\b` + regexp.QuoteMeta(name) + `\s*=\s*["']([^"']+)["']`)
	m := re.FindStringSubmatch(attrs)
	if len(m) >= 2 {
		return m[1]
	}
	return ""
}
