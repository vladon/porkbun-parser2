# Porkbun Auctions Parser (JSONL CLI)

This repository contains a **Go console utility** that crawls Porkbun Local Auctions and prints **all domains from all pages** with the full table data as **JSONL** (one JSON object per line).

Source page: [`https://porkbun.com/auctions`](https://porkbun.com/auctions)

## Build

```bash
cd /home/vladon/projects/porkbun-parser2
go build -o porkbun-auctions ./cmd/porkbun-auctions
```

## Run

Stream to stdout:

```bash
./porkbun-auctions
```

Write to a file:

```bash
./porkbun-auctions --out auctions.jsonl
```

Limit pages (useful for testing):

```bash
./porkbun-auctions --max-pages 3 --delay 0ms
```

Blazingly fast (parallel fetch; may trigger 429s, retries will back off):

```bash
./porkbun-auctions --concurrency 16 --delay 0ms --out auctions.jsonl
```

Recommended for monitoring progress (logs to stderr, JSONL stays clean):

```bash
./porkbun-auctions --concurrency 16 --delay 0ms --log-every 2s --out auctions.jsonl
```

Don’t stop on bad pages (enabled by default):

```bash
./porkbun-auctions --continue-on-error=true
```

## Resume / checkpoint

By default, the crawler writes a checkpoint file and will **resume automatically** if it exists:

- Checkpoint path: `.porkbun-auctions.checkpoint.json`
- Disable resume: `--resume=false`
- Change checkpoint path: `--checkpoint /path/to/checkpoint.json`

## Performance notes

- Porkbun shows **100 rows per page**. If the site shows ~335k results, that’s ~3,355 pages.
- Default delay is `750ms` between pages, so a full crawl is expected to take **tens of minutes** (plus network overhead).

## Output format (JSONL)

Each output line contains:

- `domain`, `tld`
- `time_left_raw`
- `starting_price`, `current_bid` (strings, preserving formatting like `2,025.00`)
- `bids` (int), `domain_age` (int)
- `revenue`, `visitors` (string or `null` when `-`)
- `auction_id` (from the `<tr id="...">`)
- `page_url`, `row_index`, `scraped_at`

Example (single line):

```json
{"domain":"shxx.com","tld":"com","time_left_raw":"19h, 10m","starting_price":"1.00","current_bid":"420.00","bids":70,"domain_age":0,"revenue":"0.00","visitors":null,"auction_id":"33327194","page_url":"https://porkbun.com/auctions","row_index":0,"scraped_at":"2025-12-28T20:49:40.639536694Z"}
```

