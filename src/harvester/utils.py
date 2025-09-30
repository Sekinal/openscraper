"""Utility functions and helpers."""
import re
import logging
from pathlib import Path
from typing import List, Dict, Any
from rich.logging import RichHandler
from rich.console import Console

# Setup rich console
console = Console()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, console=console)]
)

logger = logging.getLogger("harvester")


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove invalid characters."""
    # Remove invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Limit length
    filename = filename[:200]
    return filename


def load_keywords_from_file(filepath: str) -> List[str]:
    """Load keywords from a text file (one per line)."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    with open(path, 'r', encoding='utf-8') as f:
        keywords = [
            line.strip() 
            for line in f 
            if line.strip() and not line.startswith('#')
        ]
    
    logger.info(f"Loaded {len(keywords)} keywords from {filepath}")
    return keywords


def validate_proxy_url(proxy_url: str) -> bool:
    """Validate proxy URL format."""
    pattern = r'^(http|https|socks5)://.*'
    return bool(re.match(pattern, proxy_url))

def clean_serp_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and validate SERP data."""
    # Remove duplicates from organic results by URL
    seen_urls = set()
    unique_results = []
    for result in data.get('organic_results', []):
        url = result.get('url')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(result)
    
    data['organic_results'] = unique_results
    data['total_results'] = len(unique_results)
    
    # Deduplicate related keywords
    data['related_keywords'] = list(set(data.get('related_keywords', [])))
    
    # Deduplicate PAA
    data['people_also_ask'] = list(set(data.get('people_also_ask', [])))
    
    return data