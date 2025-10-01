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
from crawlee.fingerprint_suite import DefaultFingerprintGenerator, HeaderGeneratorOptions

from .config import HarvesterConfig
from .utils import logger, sanitize_filename


class GoogleSERPHarvester:
    """Production-grade Google SERP harvester using Crawlee."""
    
    def __init__(self, config: HarvesterConfig):
        """Initialize harvester with configuration."""
        self.config = config
        self.proxy_config: Optional[ProxyConfiguration] = None
        self.results: List[Dict[str, Any]] = []
        
        self.dataset_name = 'serp-results-persistent'

        # Setup proxy if configured
        if config.proxy_urls and len(config.proxy_urls) > 0:
            self.proxy_config = ProxyConfiguration(
                proxy_urls=config.proxy_urls
            )
            logger.info(f"Configured {len(config.proxy_urls)} proxies")

        self.fingerprint_generator = DefaultFingerprintGenerator(
            header_options=HeaderGeneratorOptions(browsers=['chrome'])
        )
        
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
    
    async def _handle_search_page(self, context: PlaywrightCrawlingContext) -> None:
        """Handle individual search result page with streaming storage."""
        page = context.page
        request = context.request
        
        try:
            await page.wait_for_selector("#search", timeout=30000)
            
            # Extract and immediately push (streaming)
            results = await self._extract_results(page, request)
            
            # Push directly to dataset (no in-memory accumulation)
            await context.push_data(results)
            
            logger.info(
                f"Extracted {len(results.get('organic_results', []))} results "
                f"from {request.user_data.get('keyword', 'unknown')}"
            )
            
            # Rate limiting
            delay = random.uniform(self.config.min_delay, self.config.max_delay)
            await asyncio.sleep(delay)
            
        except Exception as e:
            logger.error(f"Error processing {request.url}: {e}")
            context.log.error(f"Failed to process page: {e}")
            # Push error record for tracking
            await context.push_data({
                "keyword": request.user_data.get('keyword'),
                "error": str(e),
                "url": request.url
            })
    
    async def _extract_results(
        self, 
        page, 
        request: Request
    ) -> Dict[str, Any]:
        """Extract search results from Google SERP with enhanced cleaning."""
        from datetime import datetime
        from urllib.parse import urlparse
        
        keyword = request.user_data.get('keyword', '')
        
        # Extract organic search results with better validation
        organic_results = await page.evaluate('''() => {
            const results = [];
            const searchResults = document.querySelectorAll('.tF2Cxc, .Ww4FFb');
            
            searchResults.forEach((result) => {
                try {
                    const linkElement = result.querySelector('a[href]:not([role="button"])');
                    const url = linkElement ? linkElement.href : null;
                    
                    const titleElement = result.querySelector('h3.LC20lb, h3.DKV0Md, h3');
                    const title = titleElement ? titleElement.textContent.trim() : null;
                    
                    const descElement = result.querySelector('.VwiC3b, .yXK7lf, .lEBKkf, [data-sncf="1"]');
                    const description = descElement ? descElement.textContent.trim() : null;
                    
                    // Extract domain
                    let domain = null;
                    try {
                        const urlObj = new URL(url);
                        domain = urlObj.hostname.replace('www.', '');
                    } catch (e) {}
                    
                    // Only add valid results
                    if (url && title && url.startsWith('http') && !url.includes('google.com')) {
                        results.push({
                            url: url,
                            title: title,
                            description: description || '',
                            domain: domain,
                            position: results.length + 1
                        });
                    }
                } catch (e) {
                    console.error('Error extracting result:', e);
                }
            });
            
            return results;
        }''')
        
        # Extract ACTUAL related keywords (bottom of page)
        related_keywords = await page.evaluate('''() => {
            const keywords = [];
            
            // Target the actual "Related searches" section at bottom
            const relatedSection = document.querySelectorAll('.y6Uyqe a, .k8XOCe a');
            
            relatedSection.forEach((link) => {
                const text = link.textContent.trim();
                // Filter: must be short, meaningful keywords only
                if (text && 
                    text.length > 3 && 
                    text.length < 100 && 
                    !text.includes('...') &&
                    !text.includes('—') &&
                    !keywords.includes(text)) {
                    keywords.push(text);
                }
            });
            
            return [...new Set(keywords)].slice(0, 10); // Max 10 related
        }''')
        
        # Extract clean PAA questions only
        paa_questions = await page.evaluate('''() => {
            const questions = [];
            
            // Target PAA container
            const paaContainer = document.querySelectorAll('[jsname="Cpkphb"] [role="button"], .related-question-pair');
            
            paaContainer.forEach((el) => {
                // Look for the question text specifically
                const questionEl = el.querySelector('div[role="button"]') || el;
                const text = questionEl.textContent.trim();
                
                // Must end with ? and be reasonable length
                if (text && 
                    text.endsWith('?') && 
                    text.length > 10 && 
                    text.length < 200 &&
                    !questions.includes(text)) {
                    questions.push(text);
                }
            });
            
            return [...new Set(questions)].slice(0, 8); // Max 8 questions
        }''')
        
        # Calculate additional metrics
        results_with_description = sum(1 for r in organic_results if r.get('description'))
        unique_domains = len(set(r.get('domain') for r in organic_results if r.get('domain')))
        
        return {
            'keyword': keyword,
            'url': request.url,
            'organic_results': organic_results,
            'related_keywords': related_keywords,
            'people_also_ask': paa_questions,
            'total_results': len(organic_results),
            'results_with_description': results_with_description,
            'unique_domains': unique_domains,
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
        
        # Create crawler instance
        crawler = PlaywrightCrawler(
            headless=self.config.headless,
            browser_type=self.config.browser_type,
            max_requests_per_crawl=self.config.max_requests,
            concurrency_settings=concurrency_settings,
            request_handler=self._handle_search_page,
            proxy_configuration=self.proxy_config,
            request_handler_timeout=timedelta(seconds=self.config.request_timeout / 1000),
            browser_launch_options={
                'args': ['--disable-blink-features=AutomationControlled']
            },
            fingerprint_generator=self.fingerprint_generator
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
        
        # Get results from default dataset (where context.push_data() stores them)
        dataset = await Dataset.open()  # Opens default dataset
        data = await dataset.get_data()
        
        # Store results in instance variable
        self.results = data.items if hasattr(data, 'items') else list(data)
        
        logger.info(f"Scraping complete: {len(self.results)} results")
        
        return self.results
    
    async def export_results(self, filename: Optional[str] = None) -> Path:
        """Export results using native Crawlee Dataset methods."""
        
        # Create output directory
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        if not filename:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"serp_results_{timestamp}"
        
        filename = sanitize_filename(filename)
        filepath = output_dir / f"{filename}.{self.config.export_format}"
        
        # Get the dataset that context.push_data() writes to
        dataset = await Dataset.open()  # Opens the default dataset
        
        # Get all data from dataset
        data = await dataset.get_data()
        items = data.items if hasattr(data, 'items') else list(data)
        
        # Export based on format
        if self.config.export_format == 'json':
            import json
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
        elif self.config.export_format == 'csv':
            import csv
            if items:
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=items[0].keys())
                    writer.writeheader()
                    writer.writerows(items)
        
        logger.info(f"Results exported to: {filepath}")
        return filepath

    async def get_dataset_stats(self) -> Dict[str, Any]:
        """Get statistics about the persistent dataset."""
        dataset = await Dataset.open(name=self.dataset_name)
        info = await dataset.get_info()
        
        return {
            "total_items": info.item_count,
            "dataset_name": info.name,
            "created_at": info.created_at,
            "modified_at": info.modified_at,
            "access_count": info.accessed_at
        }

    async def clear_dataset(self) -> None:
        """Manually clear the persistent dataset."""
        dataset = await Dataset.open(name=self.dataset_name)
        await dataset.drop()
        logger.info(f"Dataset '{self.dataset_name}' cleared")