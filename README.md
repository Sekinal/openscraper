Based on the comprehensive codebase analysis, here's a simple, professional README for the openscraper project:

# openscraper

A production-grade Google SERP scraper and keyword harvester built with Python, Crawlee, and Playwright.

## Features

- **SERP Scraping**: Extract organic results, related keywords, and "People Also Ask" questions from Google search results
- **Keyword Harvesting**: Recursive keyword expansion using Google Autocomplete API with alphabet, question, and preposition modifiers
- **Production-Ready**: Browser automation with Playwright, proxy rotation, rate limiting, and fingerprint randomization
- **CLI-First**: Command-line interface with rich output formatting and progress tracking
- **Multiple Export Formats**: JSON, CSV, and JSONL output options

## Installation

Requires **Python 3.12+**

```bash
# Install dependencies using uv (recommended)
uv pip install -e .

# Install Playwright browsers
uv run playwright install chromium
```

## Quick Start

### Scrape Google SERP

```bash
# Scrape single keyword
harvester scrape -k "machine learning" -p 3

# Scrape from file with proxy
harvester scrape -f keywords.txt --proxy http://proxy:8080 -o results

# Advanced options
harvester scrape -k "data science" \
  --pages 5 \
  --headless \
  --min-delay 2 \
  --max-delay 5 \
  --format json
```

### Harvest Keywords

```bash
# Basic harvest
harvester harvest -k "python tutorial" --max-depth 2

# Advanced harvest with options
harvester harvest -f seeds.txt \
  --language en \
  --country us \
  --recursive \
  --alphabet \
  --questions \
  --format csv \
  -o my_keywords
```

### Analyze Results

```bash
# Analyze previous scrape
harvester analyze data/results/serp_results_20250930.json
```

## Configuration

Copy `.env.example` to `.env` and customize settings:

```bash
# Browser settings
HEADLESS=true
BROWSER_TYPE=chromium
REQUEST_TIMEOUT=60000

# Rate limiting
MIN_DELAY=2.0
MAX_DELAY=5.0
MAX_CONCURRENCY=1

# Proxy settings (optional)
PROXY_URLS=["http://proxy1:8080","http://proxy2:8080"]
ROTATE_PROXY=false

# Output
OUTPUT_DIR=data/results
EXPORT_FORMAT=json

# Keyword harvester
KEYWORD_LANGUAGE=en
KEYWORD_COUNTRY=us
KEYWORD_MAX_DEPTH=2
```

## Project Structure

```
openscraper/
├── src/harvester/
│   ├── cli.py              # Command-line interface
│   ├── scraper.py          # SERP scraping logic
│   ├── keyword_harvester.py # Keyword expansion
│   ├── config.py           # Configuration management
│   └── utils.py            # Helper functions
├── pyproject.toml          # Project dependencies
└── .env.example            # Configuration template
```

## Commands

| Command | Description |
|---------|-------------|
| `harvester scrape` | Scrape Google SERP for URLs and keywords |
| `harvester harvest` | Harvest keywords using Autocomplete API |
| `harvester analyze` | Analyze previously scraped results |
| `harvester validate` | Validate installation and configuration |

## Dependencies

- **crawlee[playwright]**: Browser automation and scraping framework
- **pydantic**: Configuration validation
- **click**: CLI framework
- **rich**: Console formatting and progress bars
- **aiohttp**: Async HTTP requests for keyword harvesting
- **aiofiles**: Async file operations

## Output Examples

### SERP Results (JSON)
```json
{
  "keyword": "machine learning",
  "organic_results": [
    {
      "url": "https://example.com",
      "title": "What is Machine Learning?",
      "description": "...",
      "domain": "example.com",
      "position": 1
    }
  ],
  "related_keywords": ["deep learning", "neural networks"],
  "people_also_ask": ["What is machine learning used for?"]
}
```

### Keyword Harvest (JSON)
```json
{
  "metadata": {
    "language": "en",
    "country": "US",
    "generated_at": "2025-09-30T11:41:00"
  },
  "keywords": [
    {
      "keyword": "python tutorial for beginners",
      "relevance": 1200,
      "depth": 1,
      "parent_keyword": "python tutorial"
    }
  ]
}
```

## License

This project is for educational and research purposes.