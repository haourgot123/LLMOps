import os
import asyncio
from openai import OpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer




class WebCrawler:
    
    def __init__(
        self, 
        system_prompt: str,
        **kwargs
    ):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.system_prompt = system_prompt
    
    def crawl_multi_urls(
        self,
        urls: list[str],
    ):
        pass


    def crawl_single_url(
        self,
        url: str,
    ):
        pass

    def extract_urls_from_markdown(
        self,
        md: str
    ):
        pass

    def post_process_markdown(
        self,
        md: str
    ):
        pass

    def run(
        self,
        url: str
    ):
        pass
    
    def runs(
        self,
        urls: list[str]
    ):
        pass
    
    
    