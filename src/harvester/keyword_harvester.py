"""Google Autocomplete Keyword Harvester with recursive expansion."""
import asyncio
import aiohttp
from typing import List, Dict, Set, Optional, Any
from datetime import datetime
from collections import deque
from urllib.parse import quote_plus
import json

from .utils import logger


class KeywordHarvester:
    """
    Production-grade Google Autocomplete keyword harvester.
    
    Features:
    - Multiple seed keywords with recursive expansion
    - Language and geo-targeting
    - Search volume estimation via relevance scores
    - Depth-limited recursive expansion
    - Deduplication and filtering
    """
    
    # Alphabet modifiers for comprehensive expansion
    ALPHABET = list('abcdefghijklmnopqrstuvwxyz')
    NUMBERS = list('0123456789')
    QUESTION_WORDS = ['how', 'what', 'why', 'when', 'where', 'who', 'which', 'are', 'is', 'can', 'will']
    PREPOSITIONS = ['for', 'with', 'without', 'near', 'in', 'at', 'to', 'from', 'vs', 'versus']
    
    def __init__(
        self,
        language: str = 'en',
        country: str = 'us',
        domain_specific: Optional[str] = None,
        max_depth: int = 2,
        min_relevance: int = 0,
        rate_limit_delay: float = 0.5,
        timeout: int = 10,
        max_suggestions_per_seed: int = 100
    ):
        """
        Initialize keyword harvester.
        
        Args:
            language: Language code (e.g., 'en', 'es', 'de')
            country: Country code (e.g., 'us', 'uk', 'de')
            domain_specific: Domain filter ('yt' for YouTube, None for web)
            max_depth: Maximum recursion depth for expansion
            min_relevance: Minimum relevance score to keep suggestions
            rate_limit_delay: Delay between requests (seconds)
            timeout: Request timeout (seconds)
            max_suggestions_per_seed: Maximum suggestions to generate per seed
        """
        self.language = language
        self.country = country.upper()
        self.domain_specific = domain_specific
        self.max_depth = max_depth
        self.min_relevance = min_relevance
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.max_suggestions_per_seed = max_suggestions_per_seed
        
        self.base_url = "https://suggestqueries.google.com/complete/search"
        self.all_keywords: Set[str] = set()
        self.keyword_data: List[Dict[str, Any]] = []
        
    async def _fetch_suggestions(
        self,
        session: aiohttp.ClientSession,
        query: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch autocomplete suggestions for a single query.
        
        Returns list of dicts with 'keyword', 'relevance', 'type'.
        """
        params = {
            'client': 'chrome',
            'hl': self.language,
            'gl': self.country,
            'q': query
        }
        
        if self.domain_specific:
            params['ds'] = self.domain_specific
        
        try:
            async with session.get(
                self.base_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            ) as response:
                if response.status != 200:
                    logger.warning(f"Non-200 status {response.status} for query: {query}")
                    return []
                
                # Google returns text/javascript MIME type, so read as text and parse manually
                text = await response.text()
                
                # Parse the JSON manually
                try:
                    data = json.loads(text)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON for query '{query}': {e}")
                    return []
                
                # Parse Google Autocomplete response format
                # Format: [query, [suggestions], [metadata], {...}]
                if not isinstance(data, list) or len(data) < 2:
                    return []
                
                suggestions = data[1] if len(data) > 1 else []
                
                # Extract relevance scores if available
                metadata = data[4] if len(data) > 4 else {}
                relevance_scores = []
                
                if isinstance(metadata, dict):
                    relevance_scores = metadata.get('google:suggestrelevance', [])
                
                # Build result list
                results = []
                for idx, suggestion in enumerate(suggestions):
                    relevance = (
                        relevance_scores[idx] 
                        if idx < len(relevance_scores) 
                        else 0
                    )
                    
                    if relevance >= self.min_relevance:
                        results.append({
                            'keyword': suggestion,
                            'relevance': relevance,
                            'type': 'QUERY',
                            'source_query': query,
                            'depth': 0  # Will be updated during recursion
                        })
                
                # Rate limiting
                await asyncio.sleep(self.rate_limit_delay)
                
                return results
                
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching suggestions for: {query}")
            return []
        except Exception as e:
            logger.error(f"Error fetching suggestions for '{query}': {e}")
            return []
    
    async def _expand_with_modifiers(
        self,
        session: aiohttp.ClientSession,
        seed_keyword: str,
        modifiers: List[str],
        position: str = 'suffix'
    ) -> List[Dict[str, Any]]:
        """
        Expand a seed keyword with modifiers (letters, words, etc.).
        
        Args:
            seed_keyword: Base keyword to expand
            modifiers: List of modifiers to append/prepend
            position: 'suffix' or 'prefix'
        """
        tasks = []
        
        for modifier in modifiers:
            if position == 'suffix':
                query = f"{seed_keyword} {modifier}"
            else:
                query = f"{modifier} {seed_keyword}"
            
            tasks.append(self._fetch_suggestions(session, query))
        
        # Execute in batches to avoid overwhelming the API
        batch_size = 10
        all_results = []
        
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, list):
                    all_results.extend(result)
        
        return all_results
    
    async def harvest_keywords(
        self,
        seed_keywords: List[str],
        use_alphabet: bool = True,
        use_questions: bool = True,
        use_prepositions: bool = True,
        recursive: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Harvest keywords from seed list with optional recursive expansion.
        
        Args:
            seed_keywords: Initial list of keywords to expand
            use_alphabet: Append a-z to each seed
            use_questions: Prepend question words
            use_prepositions: Append prepositions
            recursive: Enable recursive expansion of found keywords
            
        Returns:
            List of keyword dictionaries with metadata
        """
        async with aiohttp.ClientSession() as session:
            # Queue for BFS-style expansion
            queue = deque()
            
            # Initialize queue with seeds at depth 0
            for seed in seed_keywords:
                queue.append((seed.strip().lower(), 0))
                self.all_keywords.add(seed.strip().lower())
            
            logger.info(f"Starting harvest with {len(seed_keywords)} seed keywords")
            logger.info(f"Recursive: {recursive}, Max depth: {self.max_depth}")
            
            processed = 0
            
            while queue:
                current_keyword, depth = queue.popleft()
                
                # Check depth limit
                if depth > self.max_depth:
                    continue
                
                logger.info(f"Processing: '{current_keyword}' (depth: {depth})")
                
                # 1. Get base suggestions
                base_suggestions = await self._fetch_suggestions(session, current_keyword)
                
                # 2. Alphabet expansion (only at depth 0)
                alphabet_suggestions = []
                if use_alphabet and depth == 0:
                    alphabet_suggestions = await self._expand_with_modifiers(
                        session,
                        current_keyword,
                        self.ALPHABET,
                        position='suffix'
                    )
                
                # 3. Question word expansion
                question_suggestions = []
                if use_questions:
                    question_suggestions = await self._expand_with_modifiers(
                        session,
                        current_keyword,
                        self.QUESTION_WORDS,
                        position='prefix'
                    )
                
                # 4. Preposition expansion
                preposition_suggestions = []
                if use_prepositions:
                    preposition_suggestions = await self._expand_with_modifiers(
                        session,
                        current_keyword,
                        self.PREPOSITIONS,
                        position='suffix'
                    )
                
                # Combine all suggestions
                all_suggestions = (
                    base_suggestions + 
                    alphabet_suggestions + 
                    question_suggestions + 
                    preposition_suggestions
                )
                
                # Process suggestions
                for suggestion in all_suggestions:
                    keyword = suggestion['keyword'].lower().strip()
                    
                    # Skip if already processed or too similar to seed
                    if keyword in self.all_keywords or keyword == current_keyword:
                        continue
                    
                    # Add to results
                    suggestion['depth'] = depth
                    suggestion['parent_keyword'] = current_keyword
                    suggestion['scraped_at'] = datetime.now().isoformat()
                    
                    self.keyword_data.append(suggestion)
                    self.all_keywords.add(keyword)
                    
                    # Add to queue for recursive expansion
                    if recursive and depth < self.max_depth:
                        queue.append((keyword, depth + 1))
                    
                    # Limit suggestions per seed to avoid explosion
                    if len(self.all_keywords) >= self.max_suggestions_per_seed * len(seed_keywords):
                        logger.warning(f"Reached max suggestions limit: {len(self.all_keywords)}")
                        queue.clear()
                        break
                
                processed += 1
                logger.info(
                    f"Found {len(all_suggestions)} suggestions for '{current_keyword}'. "
                    f"Total unique: {len(self.all_keywords)}"
                )
            
            logger.info(f"Harvest complete! Total keywords: {len(self.all_keywords)}")
            
            # Sort by relevance
            self.keyword_data.sort(key=lambda x: x.get('relevance', 0), reverse=True)
            
            return self.keyword_data
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get harvesting statistics and insights."""
        if not self.keyword_data:
            return {
                'total_keywords': 0,
                'unique_keywords': 0,
                'average_relevance': 0,
                'average_keyword_length': 0,
                'average_word_count': 0,
                'depth_distribution': {},
                'top_keywords': [],
                'long_tail_percentage': 0
            }
        
        # Calculate metrics
        total_keywords = len(self.keyword_data)
        avg_relevance = sum(k.get('relevance', 0) for k in self.keyword_data) / total_keywords
        
        # Depth distribution
        depth_distribution = {}
        for k in self.keyword_data:
            depth = k.get('depth', 0)
            depth_distribution[depth] = depth_distribution.get(depth, 0) + 1
        
        # Top keywords
        top_keywords = sorted(
            self.keyword_data,
            key=lambda x: x.get('relevance', 0),
            reverse=True
        )[:20]
        
        # Keyword length stats
        lengths = [len(k['keyword']) for k in self.keyword_data]
        avg_length = sum(lengths) / len(lengths)
        
        # Word count stats
        word_counts = [len(k['keyword'].split()) for k in self.keyword_data]
        avg_words = sum(word_counts) / len(word_counts)
        
        return {
            'total_keywords': total_keywords,
            'unique_keywords': len(self.all_keywords),
            'average_relevance': round(avg_relevance, 2),
            'average_keyword_length': round(avg_length, 1),
            'average_word_count': round(avg_words, 1),
            'depth_distribution': depth_distribution,
            'top_keywords': [
                {
                    'keyword': k['keyword'],
                    'relevance': k.get('relevance', 0),
                    'depth': k.get('depth', 0)
                }
                for k in top_keywords
            ],
            'long_tail_percentage': round(
                sum(1 for wc in word_counts if wc >= 3) / len(word_counts) * 100,
                1
            )
        }
    
    def export_to_json(self, filepath: str) -> None:
        """Export keywords to JSON file."""
        output = {
            'metadata': {
                'language': self.language,
                'country': self.country,
                'domain_specific': self.domain_specific,
                'max_depth': self.max_depth,
                'generated_at': datetime.now().isoformat(),
                'statistics': self.get_statistics()
            },
            'keywords': self.keyword_data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Keywords exported to: {filepath}")
    
    def export_to_txt(self, filepath: str, include_metadata: bool = False) -> None:
        """Export keywords to plain text file (one per line)."""
        with open(filepath, 'w', encoding='utf-8') as f:
            if include_metadata:
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write(f"# Language: {self.language}, Country: {self.country}\n")
                f.write(f"# Total keywords: {len(self.keyword_data)}\n")
                f.write("#" + "="*70 + "\n\n")
            
            for item in self.keyword_data:
                if include_metadata:
                    f.write(
                        f"{item['keyword']} "
                        f"(relevance: {item.get('relevance', 0)}, "
                        f"depth: {item.get('depth', 0)})\n"
                    )
                else:
                    f.write(f"{item['keyword']}\n")
        
        logger.info(f"Keywords exported to: {filepath}")
    
    def export_to_csv(self, filepath: str) -> None:
        """Export keywords to CSV file."""
        import csv
        
        if not self.keyword_data:
            logger.warning("No keywords to export")
            return
        
        headers = [
            'keyword',
            'relevance',
            'type',
            'depth',
            'parent_keyword',
            'source_query',
            'scraped_at'
        ]
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for item in self.keyword_data:
                writer.writerow({
                    'keyword': item.get('keyword', ''),
                    'relevance': item.get('relevance', 0),
                    'type': item.get('type', 'QUERY'),
                    'depth': item.get('depth', 0),
                    'parent_keyword': item.get('parent_keyword', ''),
                    'source_query': item.get('source_query', ''),
                    'scraped_at': item.get('scraped_at', '')
                })
        
        logger.info(f"Keywords exported to: {filepath}")
