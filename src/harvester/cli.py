"""Command-line interface for the harvester."""
import asyncio
from pathlib import Path
from typing import Optional, List
from collections import Counter
import json
import csv

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

from .config import HarvesterConfig
from .scraper import GoogleSERPHarvester
from .utils import logger, load_keywords_from_file

console = Console()


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Google SERP URL and Keyword Harvester - Production Grade."""
    pass


@cli.command()
@click.option(
    '-k', '--keywords',
    multiple=True,
    help='Search keywords (can be specified multiple times)'
)
@click.option(
    '-f', '--keywords-file',
    type=click.Path(exists=True),
    help='File containing keywords (one per line)'
)
@click.option(
    '-p', '--pages',
    default=1,
    type=int,
    help='Number of result pages per keyword'
)
@click.option(
    '--max-results',
    default=100,
    type=int,
    help='Maximum total results to scrape'
)
@click.option(
    '--proxy',
    multiple=True,
    help='Proxy URL (can be specified multiple times)'
)
@click.option(
    '--proxy-file',
    type=click.Path(exists=True),
    help='File containing proxy URLs (one per line)'
)
@click.option(
    '--headless/--no-headless',
    default=True,
    help='Run browser in headless mode'
)
@click.option(
    '--browser',
    type=click.Choice(['chromium', 'firefox', 'webkit']),
    default='chromium',
    help='Browser type to use'
)
@click.option(
    '-o', '--output',
    help='Output filename (without extension)'
)
@click.option(
    '--format',
    type=click.Choice(['json', 'csv', 'jsonl']),
    default='json',
    help='Export format'
)
@click.option(
    '--min-delay',
    default=2.0,
    type=float,
    help='Minimum delay between requests (seconds)'
)
@click.option(
    '--max-delay',
    default=5.0,
    type=float,
    help='Maximum delay between requests (seconds)'
)
@click.option(
    '--concurrency',
    default=1,
    type=int,
    help='Maximum concurrent requests'
)
def scrape(
    keywords: tuple,
    keywords_file: Optional[str],
    pages: int,
    max_results: int,
    proxy: tuple,
    proxy_file: Optional[str],
    headless: bool,
    browser: str,
    output: Optional[str],
    format: str,
    min_delay: float,
    max_delay: float,
    concurrency: int
):
    """Scrape Google SERP for URLs and keywords."""
    
    # Load keywords
    keyword_list = list(keywords)
    if keywords_file:
        keyword_list.extend(load_keywords_from_file(keywords_file))
    
    if not keyword_list:
        console.print(
            "[red]Error:[/red] No keywords provided. "
            "Use -k or --keywords-file"
        )
        return
    
    # Load proxies
    proxy_list = list(proxy)
    if proxy_file:
        proxy_list.extend(load_keywords_from_file(proxy_file))
    
    # Display configuration
    table = Table(title="Scraping Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Keywords", str(len(keyword_list)))
    table.add_row("Pages per keyword", str(pages))
    table.add_row("Browser", browser)
    table.add_row("Headless", str(headless))
    table.add_row("Proxies", str(len(proxy_list)) if proxy_list else "None")
    table.add_row("Concurrency", str(concurrency))
    table.add_row("Delay", f"{min_delay}s - {max_delay}s")
    table.add_row("Export format", format)
    
    console.print(table)
    
    # Create configuration
    config = HarvesterConfig(
        max_requests=max_results,
        headless=headless,
        browser_type=browser,
        proxy_urls=proxy_list if proxy_list else None,
        min_delay=min_delay,
        max_delay=max_delay,
        max_concurrency=concurrency,
        export_format=format
    )
    
    # Run scraper
    async def run_scraper():
        harvester = GoogleSERPHarvester(config)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(
                "Scraping Google SERP...", 
                total=None
            )
            
            results = await harvester.scrape(keyword_list, pages)
            
            progress.update(task, description="Exporting results...")
            
            # Export results manually
            filepath = None
            if results:
                # Generate filename
                output_name = output
                if not output_name:
                    from datetime import datetime
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_name = f"serp_results_{timestamp}"
                
                # Create output directory
                output_dir = Path(config.output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Save based on format
                if format == 'json':
                    filepath = output_dir / f"{output_name}.json"
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(results, f, indent=2, ensure_ascii=False)
                elif format == 'csv':
                    filepath = output_dir / f"{output_name}.csv"
                    # Flatten the nested structure
                    flat_results = []
                    for item in results:
                        flat_item = {
                            'keyword': item.get('keyword'),
                            'url': item.get('url'),
                            'total_results': item.get('total_results'),
                            'results_with_description': item.get('results_with_description', 0),
                            'unique_domains': item.get('unique_domains', 0),
                            'scraped_at': item.get('scraped_at'),
                            'organic_results': json.dumps(item.get('organic_results', []), ensure_ascii=False),
                            'related_keywords': json.dumps(item.get('related_keywords', []), ensure_ascii=False),
                            'people_also_ask': json.dumps(item.get('people_also_ask', []), ensure_ascii=False)
                        }
                        flat_results.append(flat_item)
                    
                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        if flat_results:
                            writer = csv.DictWriter(f, fieldnames=flat_results[0].keys())
                            writer.writeheader()
                            writer.writerows(flat_results)
                elif format == 'jsonl':
                    filepath = output_dir / f"{output_name}.jsonl"
                    with open(filepath, 'w', encoding='utf-8') as f:
                        for item in results:
                            f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                logger.info(f"Results exported to: {filepath}")
        
        # Display summary and analytics
        console.print(f"\n[green]✓[/green] Scraping complete!")
        if filepath:
            console.print(f"Results saved to: [cyan]{filepath}[/cyan]")
        
        # Enhanced Analytics Dashboard
        if results:
            console.print(f"\n[bold cyan]📊 Scraping Analytics[/bold cyan]")
            
            # Calculate metrics
            total_organic = sum(len(r.get('organic_results', [])) for r in results)
            avg_per_keyword = total_organic / len(results) if results else 0
            total_related = sum(len(r.get('related_keywords', [])) for r in results)
            total_paa = sum(len(r.get('people_also_ask', [])) for r in results)
            
            # Create metrics table
            metrics_table = Table(show_header=False, box=None)
            metrics_table.add_column(style="cyan")
            metrics_table.add_column(style="yellow")
            
            metrics_table.add_row("  Total URLs harvested:", f"{total_organic}")
            metrics_table.add_row("  Average per keyword:", f"{avg_per_keyword:.1f}")
            metrics_table.add_row("  Related keywords found:", f"{total_related}")
            metrics_table.add_row("  'People Also Ask' questions:", f"{total_paa}")
            
            console.print(metrics_table)
            
            # Show domain distribution
            all_domains = []
            for r in results:
                all_domains.extend([
                    o.get('domain') for o in r.get('organic_results', []) 
                    if o.get('domain')
                ])
            
            if all_domains:
                top_domains = Counter(all_domains).most_common(5)
                
                console.print(f"\n[bold cyan]🌐 Top Domains:[/bold cyan]")
                domain_table = Table(show_header=False, box=None)
                domain_table.add_column(style="white")
                domain_table.add_column(style="green", justify="right")
                
                for domain, count in top_domains:
                    domain_table.add_row(f"  {domain}", str(count))
                
                console.print(domain_table)
            
            # Show keyword summary
            console.print(f"\n[bold cyan]🔑 Keywords Summary:[/bold cyan]")
            keyword_table = Table(show_header=True, box=None)
            keyword_table.add_column("Keyword", style="white")
            keyword_table.add_column("URLs", style="green", justify="right")
            keyword_table.add_column("Related", style="blue", justify="right")
            keyword_table.add_column("PAA", style="magenta", justify="right")
            
            for r in results[:10]:  # Show first 10 keywords
                keyword_table.add_row(
                    f"  {r.get('keyword', 'N/A')[:40]}...",
                    str(len(r.get('organic_results', []))),
                    str(len(r.get('related_keywords', []))),
                    str(len(r.get('people_also_ask', [])))
                )
            
            console.print(keyword_table)
            
            if len(results) > 10:
                console.print(f"\n  [dim]... and {len(results) - 10} more keywords[/dim]")
            
            # Success message in a panel
            success_panel = Panel(
                f"[green]Successfully harvested {total_organic} URLs from {len(results)} keywords![/green]",
                title="[bold]✨ Complete[/bold]",
                border_style="green"
            )
            console.print(f"\n{success_panel}")
        else:
            console.print(f"[yellow]⚠[/yellow]  No results found")
    
    # Run async scraper
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        console.print("\n[yellow]Scraping interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        logger.exception("Scraping failed")

@cli.command()
@click.option(
    '-k', '--keywords',
    multiple=True,
    help='Seed keywords for expansion (can be specified multiple times)'
)
@click.option(
    '-f', '--keywords-file',
    type=click.Path(exists=True),
    help='File containing seed keywords (one per line)'
)
@click.option(
    '--language',
    default='en',
    help='Language code (e.g., en, es, de, fr)'
)
@click.option(
    '--country',
    default='us',
    help='Country code (e.g., us, uk, de, fr)'
)
@click.option(
    '--domain',
    type=click.Choice(['web', 'youtube'], case_sensitive=False),
    default='web',
    help='Domain to target (web=Google Search, youtube=YouTube)'
)
@click.option(
    '--max-depth',
    default=2,
    type=int,
    help='Maximum recursion depth for keyword expansion'
)
@click.option(
    '--min-relevance',
    default=0,
    type=int,
    help='Minimum relevance score to keep suggestions'
)
@click.option(
    '--max-suggestions',
    default=100,
    type=int,
    help='Maximum suggestions to generate per seed keyword'
)
@click.option(
    '--alphabet/--no-alphabet',
    default=True,
    help='Use alphabet modifiers (a-z) for expansion'
)
@click.option(
    '--questions/--no-questions',
    default=True,
    help='Use question words (how, what, why, etc.) for expansion'
)
@click.option(
    '--prepositions/--no-prepositions',
    default=True,
    help='Use prepositions (for, with, near, etc.) for expansion'
)
@click.option(
    '--recursive/--no-recursive',
    default=True,
    help='Enable recursive expansion of discovered keywords'
)
@click.option(
    '--delay',
    default=0.5,
    type=float,
    help='Delay between API requests (seconds)'
)
@click.option(
    '-o', '--output',
    help='Output filename (without extension)'
)
@click.option(
    '--format',
    type=click.Choice(['json', 'csv', 'txt'], case_sensitive=False),
    default='json',
    help='Export format'
)
def harvest(
    keywords: tuple,
    keywords_file: Optional[str],
    language: str,
    country: str,
    domain: str,
    max_depth: int,
    min_relevance: int,
    max_suggestions: int,
    alphabet: bool,
    questions: bool,
    prepositions: bool,
    recursive: bool,
    delay: float,
    output: Optional[str],
    format: str
):
    """Harvest keywords using Google Autocomplete API."""
    from .keyword_harvester import KeywordHarvester
    from pathlib import Path
    from datetime import datetime
    
    # Load keywords
    keyword_list = list(keywords)
    if keywords_file:
        keyword_list.extend(load_keywords_from_file(keywords_file))
    
    if not keyword_list:
        console.print(
            "[red]Error:[/red] No seed keywords provided. "
            "Use -k or --keywords-file"
        )
        return
    
    # Display configuration
    table = Table(title="Keyword Harvester Configuration")
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Seed Keywords", str(len(keyword_list)))
    table.add_row("Language", language)
    table.add_row("Country", country.upper())
    table.add_row("Domain", domain)
    table.add_row("Max Depth", str(max_depth))
    table.add_row("Min Relevance", str(min_relevance))
    table.add_row("Recursive", str(recursive))
    table.add_row("Alphabet Expansion", str(alphabet))
    table.add_row("Question Words", str(questions))
    table.add_row("Prepositions", str(prepositions))
    table.add_row("Rate Limit Delay", f"{delay}s")
    table.add_row("Export Format", format)
    
    console.print(table)
    
    # Initialize harvester
    domain_specific = 'yt' if domain.lower() == 'youtube' else None
    
    harvester = KeywordHarvester(
        language=language,
        country=country,
        domain_specific=domain_specific,
        max_depth=max_depth,
        min_relevance=min_relevance,
        rate_limit_delay=delay,
        max_suggestions_per_seed=max_suggestions
    )
    
    # Run harvester
    async def run_harvest():
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(
                "Harvesting keywords...",
                total=None
            )
            
            results = await harvester.harvest_keywords(
                seed_keywords=keyword_list,
                use_alphabet=alphabet,
                use_questions=questions,
                use_prepositions=prepositions,
                recursive=recursive
            )
            
            progress.update(task, description="Exporting results...")
            
            # Generate filename
            if not output:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_name = f"keywords_{timestamp}"
            else:
                output_name = output
            
            # Create output directory
            output_dir = Path("data/keywords")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Export based on format
            if format == 'json':
                filepath = output_dir / f"{output_name}.json"
                harvester.export_to_json(str(filepath))
            elif format == 'csv':
                filepath = output_dir / f"{output_name}.csv"
                harvester.export_to_csv(str(filepath))
            elif format == 'txt':
                filepath = output_dir / f"{output_name}.txt"
                harvester.export_to_txt(str(filepath), include_metadata=True)
            
            return filepath, results
    
    # Execute harvest
    try:
        filepath, results = asyncio.run(run_harvest())
        
        # Display results
        console.print(f"\n[green]✓[/green] Harvest complete!")
        console.print(f"Results saved to: [cyan]{filepath}[/cyan]\n")
        
        # Display statistics
        stats = harvester.get_statistics()

        if stats.get('total_keywords', 0) == 0:
            console.print("\n[yellow]⚠[/yellow]  No keywords harvested")
            console.print("This may be due to:")
            console.print("  • Rate limiting or blocking by Google")
            console.print("  • Network connectivity issues")
            console.print("  • Invalid seed keywords")
            console.print("\nTry:")
            console.print("  • Increasing --delay to 1.0 or higher")
            console.print("  • Using different seed keywords")
            console.print("  • Checking your network connection")
            return

        console.print("[bold cyan]📊 Harvest Statistics[/bold cyan]\n")

        stats_table = Table(show_header=False, box=None)
        stats_table.add_column(style="cyan")
        stats_table.add_column(style="yellow")

        stats_table.add_row("  Total keywords discovered:", f"{stats['total_keywords']}")
        stats_table.add_row("  Unique keywords:", f"{stats['unique_keywords']}")
        stats_table.add_row("  Average relevance score:", f"{stats['average_relevance']}")
        stats_table.add_row("  Average keyword length:", f"{stats['average_keyword_length']} chars")
        stats_table.add_row("  Average word count:", f"{stats['average_word_count']} words")
        stats_table.add_row("  Long-tail keywords (3+ words):", f"{stats['long_tail_percentage']}%")

        console.print(stats_table)
        
        # Depth distribution
        if stats.get('depth_distribution'):
            console.print(f"\n[bold cyan]🌳 Depth Distribution:[/bold cyan]")
            depth_table = Table(show_header=True, box=None)
            depth_table.add_column("Depth", style="white")
            depth_table.add_column("Keywords", style="green", justify="right")
            depth_table.add_column("Percentage", style="blue", justify="right")
            
            total = sum(stats['depth_distribution'].values())
            for depth, count in sorted(stats['depth_distribution'].items()):
                percentage = (count / total * 100) if total > 0 else 0
                depth_table.add_row(
                    f"  Level {depth}",
                    str(count),
                    f"{percentage:.1f}%"
                )
            
            console.print(depth_table)
        
        # Top keywords
        if stats.get('top_keywords'):
            console.print(f"\n[bold cyan]🔥 Top 10 Keywords (by relevance):[/bold cyan]")
            top_table = Table(show_header=True, box=None)
            top_table.add_column("Rank", style="dim")
            top_table.add_column("Keyword", style="white")
            top_table.add_column("Relevance", style="green", justify="right")
            top_table.add_column("Depth", style="blue", justify="right")
            
            for idx, kw in enumerate(stats['top_keywords'][:10], 1):
                top_table.add_row(
                    str(idx),
                    f"  {kw['keyword'][:60]}",
                    str(kw['relevance']),
                    str(kw['depth'])
                )
            
            console.print(top_table)
        
        # Success panel
        success_panel = Panel(
            f"[green]Successfully harvested {stats['total_keywords']} keywords from {len(keyword_list)} seeds![/green]",
            title="[bold]✨ Complete[/bold]",
            border_style="green"
        )
        console.print(f"\n{success_panel}")
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Harvest interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        logger.exception("Harvest failed")

@cli.command()
def validate():
    """Validate configuration and setup."""
    console.print("[cyan]Validating setup...[/cyan]\n")
    
    # Check Playwright installation
    try:
        import playwright
        console.print("[green]✓[/green] Playwright installed")
    except ImportError:
        console.print("[red]✗[/red] Playwright not installed")
        console.print("  Run: uv run playwright install chromium")
    
    # Check Crawlee
    try:
        import crawlee
        console.print(f"[green]✓[/green] Crawlee installed (v{crawlee.__version__})")
    except ImportError:
        console.print("[red]✗[/red] Crawlee not installed")
    
    # Check output directory
    config = HarvesterConfig()
    output_dir = Path(config.output_dir)
    if output_dir.exists():
        console.print(f"[green]✓[/green] Output directory exists: {output_dir}")
    else:
        console.print(f"[yellow]![/yellow] Output directory will be created: {output_dir}")
    
    # Check dependencies
    console.print("\n[cyan]Checking dependencies...[/cyan]")
    dependencies = [
        ('click', 'CLI framework'),
        ('rich', 'Console formatting'),
        ('pydantic', 'Configuration validation'),
        ('aiofiles', 'Async file operations'),
    ]
    
    for module, description in dependencies:
        try:
            __import__(module)
            console.print(f"[green]✓[/green] {description} ({module})")
        except ImportError:
            console.print(f"[red]✗[/red] {description} ({module}) - Missing")
    
    console.print("\n[green]Validation complete![/green]")


@cli.command()
@click.argument('results_file', type=click.Path(exists=True))
def analyze(results_file: str):
    """Analyze previously scraped results from a JSON file."""
    console.print(f"[cyan]Analyzing results from:[/cyan] {results_file}\n")
    
    try:
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        if not results:
            console.print("[yellow]No results found in file[/yellow]")
            return
        
        # Calculate comprehensive metrics
        total_organic = sum(len(r.get('organic_results', [])) for r in results)
        total_related = sum(len(r.get('related_keywords', [])) for r in results)
        total_paa = sum(len(r.get('people_also_ask', [])) for r in results)
        
        # Domain analysis
        all_domains = []
        all_urls = []
        for r in results:
            for org_result in r.get('organic_results', []):
                domain = org_result.get('domain')
                url = org_result.get('url')
                if domain:
                    all_domains.append(domain)
                if url:
                    all_urls.append(url)
        
        unique_domains = len(set(all_domains))
        unique_urls = len(set(all_urls))
        
        # Display comprehensive analytics
        console.print("[bold]📊 Analysis Results[/bold]\n")
        
        stats_table = Table(title="Overview Statistics")
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="yellow", justify="right")
        
        stats_table.add_row("Total Keywords", str(len(results)))
        stats_table.add_row("Total URLs Harvested", str(total_organic))
        stats_table.add_row("Unique URLs", str(unique_urls))
        stats_table.add_row("Unique Domains", str(unique_domains))
        stats_table.add_row("Related Keywords", str(total_related))
        stats_table.add_row("People Also Ask", str(total_paa))
        stats_table.add_row("Avg URLs/Keyword", f"{total_organic/len(results):.1f}")
        
        console.print(stats_table)
        
        # Top domains
        if all_domains:
            console.print("\n[bold]🌐 Top 10 Domains[/bold]\n")
            top_domains = Counter(all_domains).most_common(10)
            
            domain_table = Table()
            domain_table.add_column("Rank", style="dim")
            domain_table.add_column("Domain", style="cyan")
            domain_table.add_column("Count", style="green", justify="right")
            domain_table.add_column("Percentage", style="yellow", justify="right")
            
            for idx, (domain, count) in enumerate(top_domains, 1):
                percentage = (count / total_organic) * 100
                domain_table.add_row(
                    str(idx),
                    domain,
                    str(count),
                    f"{percentage:.1f}%"
                )
            
            console.print(domain_table)
        
        # Keywords with most results
        console.print("\n[bold]🔥 Keywords with Most Results[/bold]\n")
        keyword_results = [(r.get('keyword'), len(r.get('organic_results', []))) for r in results]
        top_keywords = sorted(keyword_results, key=lambda x: x[1], reverse=True)[:10]
        
        keyword_table = Table()
        keyword_table.add_column("Rank", style="dim")
        keyword_table.add_column("Keyword", style="cyan")
        keyword_table.add_column("URLs", style="green", justify="right")
        
        for idx, (keyword, count) in enumerate(top_keywords, 1):
            keyword_table.add_row(str(idx), keyword[:50], str(count))
        
        console.print(keyword_table)
        
    except json.JSONDecodeError:
        console.print("[red]Error:[/red] Invalid JSON file")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")


if __name__ == "__main__":
    cli()
