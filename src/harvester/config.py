"""Configuration management with Pydantic."""
from typing import Optional, List
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class HarvesterConfig(BaseSettings):
    """Main configuration for the harvester."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    # Scraping settings
    max_requests: int = Field(default=100, ge=1)
    max_results_per_query: int = Field(default=100, ge=1, le=1000)
    headless: bool = Field(default=True)
    browser_type: str = Field(default="chromium")
    request_timeout: int = Field(default=60000)  # milliseconds
    
    # Proxy settings
    proxy_urls: Optional[List[str]] = Field(default=None)
    rotate_proxy: bool = Field(default=False)
    
    # Rate limiting
    min_delay: float = Field(default=2.0, ge=0.5)
    max_delay: float = Field(default=5.0, ge=1.0)
    max_concurrency: int = Field(default=1, ge=1, le=5)
    
    # Output settings
    output_dir: str = Field(default="data/results")
    export_format: str = Field(default="json")  # json, csv, jsonl
    
    # Google-specific
    google_domain: str = Field(default="google.com")
    results_per_page: int = Field(default=10, ge=10, le=100)
    
    user_agents: Optional[List[str]] = Field(default=[
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ])
    
    @field_validator("export_format")
    @classmethod
    def validate_export_format(cls, v: str) -> str:
        """Validate export format."""
        allowed = ["json", "csv", "jsonl"]
        if v.lower() not in allowed:
            raise ValueError(f"Export format must be one of: {allowed}")
        return v.lower()
    
    @field_validator("browser_type")
    @classmethod
    def validate_browser(cls, v: str) -> str:
        """Validate browser type."""
        allowed = ["chromium", "firefox", "webkit"]
        if v.lower() not in allowed:
            raise ValueError(f"Browser must be one of: {allowed}")
        return v.lower()
