"""Command-line interface for the harvester."""
import asyncio
from pathlib import Path
from typing import Optional, List

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

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
    import json
    import csv
    
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
                    with open(filepath, 'w') as f:
                        json.dump(results, f, indent=2)
                elif format == 'csv':
                    filepath = output_dir / f"{output_name}.csv"
                    # Flatten the nested structure
                    flat_results = []
                    for item in results:
                        flat_item = {
                            'keyword': item.get('keyword'),
                            'url': item.get('url'),
                            'total_results': item.get('total_results'),
                            'scraped_at': item.get('scraped_at'),
                            'organic_results': json.dumps(item.get('organic_results', [])),
                            'related_keywords': json.dumps(item.get('related_keywords', [])),
                            'people_also_ask': json.dumps(item.get('people_also_ask', []))
                        }
                        flat_results.append(flat_item)
                    
                    with open(filepath, 'w', newline='') as f:
                        if flat_results:
                            writer = csv.DictWriter(f, fieldnames=flat_results[0].keys())
                            writer.writeheader()
                            writer.writerows(flat_results)
                elif format == 'jsonl':
                    filepath = output_dir / f"{output_name}.jsonl"
                    with open(filepath, 'w') as f:
                        for item in results:
                            f.write(json.dumps(item) + '\n')
                
                logger.info(f"Results exported to: {filepath}")
        
        # Display summary
        console.print(f"\n[green]✓[/green] Scraping complete!")
        if filepath:
            console.print(f"Results saved to: [cyan]{filepath}[/cyan]")
        console.print(f"Total results: [yellow]{len(results)}[/yellow]")
    
    # Run async scraper
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        console.print("\n[yellow]Scraping interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        logger.exception("Scraping failed")


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
    
    console.print("\n[green]Validation complete![/green]")


if __name__ == "__main__":
    cli()
