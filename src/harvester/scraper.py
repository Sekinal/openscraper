"""Core scraper implementation using Crawlee and Playwright."""
import asyncio
import random
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, quote_plus
from pathlib import Path
from datetime import timedelta

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee import Request
from crawlee.storages import Dataset
from crawlee import ConcurrencySettings

from .config import HarvesterConfig
from .utils import logger, sanitize_filename


class GoogleSERPHarvester:
    """Production-grade Google SERP harvester using Crawlee."""
    
    def __init__(self, config: HarvesterConfig):
        """Initialize harvester with configuration."""
        self.config = config
        self.proxy_config: Optional[ProxyConfiguration] = None
        self.results: List[Dict[str, Any]] = []
        
        # Setup proxy if configured
        if config.proxy_urls and len(config.proxy_urls) > 0:
            self.proxy_config = ProxyConfiguration(
                proxy_urls=config.proxy_urls
            )
            logger.info(f"Configured {len(config.proxy_urls)} proxies")
    
    def _build_google_url(self, keyword: str, start: int = 0) -> str:
        """Build Google search URL with parameters."""
        params = {
            'q': keyword,
            'start': start,
            'num': self.config.results_per_page,
            'hl': 'en',
        }
        base_url = f"https://www.{self.config.google_domain}/search"
        return f"{base_url}?{urlencode(params)}"
    
    async def _handle_search_page(
        self, 
        context: PlaywrightCrawlingContext
    ) -> None:
        """Handle individual search result page."""
        page = context.page
        request = context.request
        
        try:
            # Wait for search results to load (Google requires JS rendering)
            await page.wait_for_selector('#search', timeout=30000)
            
            # Extract organic results
            results = await self._extract_results(page, request)
            
            # Store results
            await context.push_data(results)
            
            # Add delay to mimic human behavior
            delay = random.uniform(
                self.config.min_delay, 
                self.config.max_delay
            )
            await asyncio.sleep(delay)
            
            logger.info(
                f"Extracted {len(results.get('organic_results', []))} results from: "
                f"{request.user_data.get('keyword', 'unknown')}"
            )
            
        except Exception as e:
            logger.error(f"Error processing {request.url}: {e}")
            context.log.error(f"Failed to process page: {e}")
    
    async def _extract_results(
        self, 
        page, 
        request: Request
    ) -> Dict[str, Any]:
        """Extract search results from Google SERP."""
        from datetime import datetime
        
        keyword = request.user_data.get('keyword', '')
        
        # Extract organic search results with updated selectors
        organic_results = await page.evaluate('''() => {
            const results = [];
            
            // Updated selector for 2025 Google SERP structure
            // Try multiple selectors as Google frequently changes them
            const searchResults = document.querySelectorAll('.tF2Cxc, .Ww4FFb');
            
            searchResults.forEach((result) => {
                try {
                    // Extract URL - look for the main link
                    const linkElement = result.querySelector('a[href]:not([role="button"])');
                    const url = linkElement ? linkElement.href : null;
                    
                    // Extract title - multiple possible selectors
                    const titleElement = result.querySelector('h3.LC20lb, h3.DKV0Md, h3');
                    const title = titleElement ? titleElement.textContent : null;
                    
                    // Extract description/snippet - updated selectors
                    const descElement = result.querySelector('.VwiC3b, .yXK7lf, .lEBKkf, [data-sncf="1"]');
                    const description = descElement ? descElement.textContent : null;
                    
                    // Only add if we have both URL and title
                    if (url && title && url.startsWith('http')) {
                        results.push({
                            url: url,
                            title: title,
                            description: description,
                            position: results.length + 1
                        });
                    }
                } catch (e) {
                    console.error('Error extracting result:', e);
                }
            });
            
            return results;
        }''')
        
        # Extract related keywords/suggestions with updated selectors
        related_keywords = await page.evaluate('''() => {
            const keywords = [];
            
            // Updated selectors for related searches
            const relatedSearches = document.querySelectorAll(
                '.AJLUJb .b2Rnsc a, .dg6jd, [data-sncf]'
            );
            
            relatedSearches.forEach((el) => {
                const text = el.textContent.trim();
                // Filter out duplicates and empty strings
                if (text && !keywords.includes(text) && text.length > 2) {
                    keywords.push(text);
                }
            });
            
            // Limit to unique values
            return [...new Set(keywords)];
        }''')
        
        # Extract "People also ask" questions with updated selectors
        paa_questions = await page.evaluate('''() => {
            const questions = [];
            
            // Updated selectors for PAA
            const paaElements = document.querySelectorAll(
                '.related-question-pair span, [data-sgrd] div[role="button"]'
            );
            
            paaElements.forEach((el) => {
                const text = el.textContent.trim();
                // Filter meaningful questions only
                if (text && text.includes('?') && !questions.includes(text)) {
                    questions.push(text);
                }
            });
            
            return questions;
        }''')
        
        return {
            'keyword': keyword,
            'url': request.url,
            'organic_results': organic_results,
            'related_keywords': related_keywords,
            'people_also_ask': paa_questions,
            'total_results': len(organic_results),
            'scraped_at': datetime.now().isoformat()
        }

    
    async def scrape(
        self, 
        keywords: List[str], 
        pages_per_keyword: int = 1
    ) -> List[Dict[str, Any]]:
        """
        Scrape Google SERP for given keywords.
        
        Args:
            keywords: List of search keywords
            pages_per_keyword: Number of result pages to scrape per keyword
            
        Returns:
            List of scraped results
        """
        # Create concurrency settings
        concurrency_settings = ConcurrencySettings(
            min_concurrency=1,
            max_concurrency=self.config.max_concurrency,
            desired_concurrency=self.config.max_concurrency
        )
        
        # Create crawler instance with corrected parameters
        crawler = PlaywrightCrawler(
            headless=self.config.headless,
            browser_type=self.config.browser_type,
            max_requests_per_crawl=self.config.max_requests,
            concurrency_settings=concurrency_settings,  # Fixed: use concurrency_settings
            request_handler=self._handle_search_page,
            proxy_configuration=self.proxy_config,
            request_handler_timeout=timedelta(seconds=self.config.request_timeout / 1000),  # Fixed: use timedelta
        )
        
        # Generate requests for all keywords and pages
        requests = []
        for keyword in keywords:
            for page_num in range(pages_per_keyword):
                start = page_num * self.config.results_per_page
                url = self._build_google_url(keyword, start)
                
                requests.append(
                    Request.from_url(
                        url,
                        user_data={
                            'keyword': keyword,
                            'page': page_num + 1
                        }
                    )
                )
        
        logger.info(
            f"Starting scrape: {len(keywords)} keywords, "
            f"{len(requests)} total requests"
        )
        
        # Run crawler
        await crawler.run(requests)
        
        # Get results from dataset
        dataset = await Dataset.open()
        results = await dataset.get_data()
        
        self.results = results.items
        logger.info(f"Scraping complete: {len(self.results)} results")
        
        return self.results
    
    async def export_results(
        self, 
        filename: Optional[str] = None
    ) -> Path:
        """Export results to file."""
        if not self.results:
            logger.warning("No results to export")
            return None
        
        # Create output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename if not provided
        if not filename:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"serp_results_{timestamp}"
        
        filename = sanitize_filename(filename)
        
        # Export based on format
        dataset = await Dataset.open()
        
        if self.config.export_format == "json":
            filepath = output_dir / f"{filename}.json"
            await dataset.export_to_json(str(filepath))
        elif self.config.export_format == "csv":
            filepath = output_dir / f"{filename}.csv"
            await dataset.export_to_csv(str(filepath))
        elif self.config.export_format == "jsonl":
            filepath = output_dir / f"{filename}.jsonl"
            # Export as JSON Lines
            import aiofiles
            async with aiofiles.open(filepath, 'w') as f:
                import json
                for item in self.results:
                    await f.write(json.dumps(item) + '\n')
        
        logger.info(f"Results exported to: {filepath}")
        return filepath
