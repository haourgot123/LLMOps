import os
import re
import asyncio
from loguru import logger
import json
from typing import Dict, List, Any, Optional
from openai import OpenAI
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

from structure.base_model import WebScrappingResponseExtractor


class WebCrawler:
    """Web crawler class for crawling multiple URLs and extracting content."""
    
    def __init__(
        self, 
        system_prompt: Optional[str] = None,
        max_concurrent_requests: int = 3,
        enable_cache: bool = True,
        check_robots_txt: bool = True,
        **kwargs
    ):
        """
        Initialize WebCrawler.
        
        Args:
            system_prompt: System prompt for LLM extraction
            max_concurrent_requests: Maximum concurrent requests
            enable_cache: Whether to enable caching
            check_robots_txt: Whether to respect robots.txt
            **kwargs: Additional arguments
        """
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.system_prompt = system_prompt
        
        # Configure crawler
        self.config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED if enable_cache else CacheMode.DISABLED,
            check_robots_txt=check_robots_txt,
            semaphore_count=max_concurrent_requests
        )
        
        # Optional memory adaptive dispatcher
        self.dispatcher = None
        if kwargs.get('enable_memory_management'):
            self.dispatcher = MemoryAdaptiveDispatcher(
                memory_threshold_percent=kwargs.get('memory_threshold', 70.0),
                check_interval=kwargs.get('check_interval', 1.0),
                max_session_permit=kwargs.get('max_session_permit', 10)
            )
    
    async def crawl_multi_urls(
        self,
        urls: List[str],
    ) -> List[Dict[str, str]]:
        """
        Crawl multiple URLs and return the markdowns.
        
        Args:
            urls: List of URLs to crawl
            
        Returns:
            List of dictionaries containing URL and markdown content
            
        Raises:
            Exception: If crawling fails
        """
        if not urls:
            logger.warning("No URLs provided for crawling")
            return []
            
        try:
            async with AsyncWebCrawler() as crawler:
                logger.info(f"Starting to crawl {len(urls)} URLs")
                
                # Use dispatcher if available
                crawler_kwargs = {"config": self.config}
                if self.dispatcher:
                    crawler_kwargs["dispatcher"] = self.dispatcher
                
                results = await crawler.arun_many(urls, **crawler_kwargs)
                
                markdowns = []
                for result in results:
                    if result.markdown and result.markdown.raw_markdown:
                        markdowns.append({
                            "url": result.url,
                            "markdown": result.markdown.raw_markdown
                        })
                    else:
                        logger.warning(f"No markdown content found for {result.url}")
                
                logger.info(f"Successfully crawled {len(markdowns)} URLs")
                return markdowns
        
        except Exception as e:
            logger.error(f"Crawling failed: {e}")
            raise
    
    def extract_urls_with_regex(
        self,
        markdown_content: str,
        url_pattern: str = r'https?://[^\s)\]]+'
    ) -> List[str]:
        """
        Extract URLs from markdown content using regex.
        
        Args:
            markdown_content: Markdown content to extract URLs from
            url_pattern: Regex pattern for URL extraction
            
        Returns:
            List of extracted URLs
        """
        if not markdown_content:
            return []
            
        try:
            links = re.findall(url_pattern, markdown_content)
            # Filter for HTML files and remove duplicates
            html_links = list(set([
                link for link in links 
                if link.endswith(('.html', '.htm')) and link.startswith('http')
            ]))
            return html_links
        except Exception as e:
            logger.error(f"Regex extraction failed: {e}")
            return []

    async def extract_urls_from_markdowns_with_regex(
        self,
        markdowns: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Extract URLs from multiple markdowns using regex.
        
        Args:
            markdowns: List of markdown dictionaries
            
        Returns:
            List of extracted URLs
        """
        if not markdowns:
            return []
            
        extracted_urls = set()  # Use set to avoid duplicates
        
        for md in markdowns:
            if "markdown" in md and md["markdown"]:
                urls = self.extract_urls_with_regex(md["markdown"])
                extracted_urls.update(urls)
        
        unique_urls = list(extracted_urls)
        logger.info(f"Extracted {len(unique_urls)} unique URLs from {len(markdowns)} markdowns")
        return unique_urls
    
    async def extract_urls_from_markdown_with_llm(
        self,
        markdowns: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Extract URLs from markdowns using LLM.
        
        Args:
            markdowns: List of markdown dictionaries
            
        Returns:
            List of LLM responses
        """
        if not markdowns or not self.system_prompt:
            logger.warning("No markdowns or system prompt provided for LLM extraction")
            return []
            
        try:
            tasks = []
            for md in markdowns:
                if "markdown" in md and md["markdown"]:
                    task = self.client.chat.completions.parse(
                        model="gpt-4o-mini",  # Use more cost-effective model
                        messages=[
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": md["markdown"][:4000]}  # Limit content length
                        ],
                        response_format=WebScrappingResponseExtractor
                    )
                    tasks.append(task)
            
            if not tasks:
                return []
                
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out exceptions and log them
            valid_responses = []
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    logger.error(f"LLM extraction failed for markdown {i}: {response}")
                else:
                    valid_responses.append(response)
            
            logger.info(f"Successfully extracted URLs from {len(valid_responses)} markdowns using LLM")
            
            # Save responses to file
            self._save_responses_to_file(valid_responses)
            
            return valid_responses
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return []
    
    def _save_responses_to_file(self, responses: List[Dict[str, Any]], filename: str = "responses.json") -> None:
        """Save responses to a JSON file."""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(responses, f, ensure_ascii=False, indent=2)
            logger.info(f"Responses saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save responses to file: {e}")
    
    def _save_markdowns_to_file(self, markdowns: List[Dict[str, Any]], filename: str = "raw_markdowns.md") -> None:
        """Save markdowns to a file."""
        try:
            results_dir = os.path.join(os.path.dirname(__file__), "results")
            os.makedirs(results_dir, exist_ok=True)
            
            filepath = os.path.join(results_dir, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                for md in markdowns:
                    if "markdown" in md and md["markdown"]:
                        f.write(f"<!-- URL: {md.get('url', 'Unknown')} -->\n")
                        f.write(md["markdown"])
                        f.write("\n\n" + "="*80 + "\n\n")
            
            logger.info(f"Markdowns saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save markdowns to file: {e}")
    
    async def run_crawling_pipeline(
        self,
        urls: List[str],
        use_llm_extraction: bool = False,
        save_intermediate_results: bool = True
    ) -> List[Dict[str, str]]:
        """
        Run the complete web crawling pipeline.
        
        Args:
            urls: List of URLs to crawl
            use_llm_extraction: Whether to use LLM for URL extraction
            save_intermediate_results: Whether to save intermediate results
            
        Returns:
            List of final markdown results
        """
        if not urls:
            logger.warning("No URLs provided")
            return []
            
        try:
            # Step 1: Initial crawling
            logger.info(f"Starting initial crawling of {len(urls)} URLs")
            initial_markdowns = await self.crawl_multi_urls(urls)
            
            if save_intermediate_results:
                self._save_markdowns_to_file(initial_markdowns)
            
            logger.info(f"Initial crawling completed: {len(initial_markdowns)} markdowns")
            
            # Step 2: Extract URLs from markdowns
            if use_llm_extraction and self.system_prompt:
                logger.info("Extracting URLs using LLM")
                extracted_urls = await self.extract_urls_from_markdown_with_llm(initial_markdowns)
                # Extract URLs from LLM responses
                urls_to_crawl = [response.get("url", "") for response in extracted_urls if response.get("url")]
            else:
                logger.info("Extracting URLs using regex")
                urls_to_crawl = await self.extract_urls_from_markdowns_with_regex(initial_markdowns)
            
            if not urls_to_crawl:
                logger.info("No additional URLs found to crawl")
                return initial_markdowns
            
            # Step 3: Crawl extracted URLs
            logger.info(f"Crawling {len(urls_to_crawl)} extracted URLs")
            final_markdowns = await self.crawl_multi_urls(urls_to_crawl)
            
            # Combine results
            all_markdowns = initial_markdowns + final_markdowns
            
            if save_intermediate_results:
                self._save_markdowns_to_file(all_markdowns, "all_markdowns.md")
            
            logger.info(f"Pipeline completed successfully: {len(all_markdowns)} total markdowns")
            return all_markdowns
            
        except Exception as e:
            logger.error(f"Crawling pipeline failed: {e}")
            raise
    
    # Alias for backward compatibility
    async def runs(self, urls: List[str]) -> List[Dict[str, str]]:
        """Alias for run_crawling_pipeline for backward compatibility."""
        return await self.run_crawling_pipeline(urls)


async def main():
    """Main function for testing the WebCrawler."""
    urls = [
        "https://vnexpress.net/",
        "https://tuoitre.vn/",
        "https://vietnamnet.vn/"
    ]
    
    web_crawler = WebCrawler(
        system_prompt="You are a web scraper that extracts URLs from markdown content. Extract only valid HTML URLs.",
        max_concurrent_requests=5,
        enable_cache=True,
        check_robots_txt=True
    )
    
    try:
        final_markdowns = await web_crawler.run_crawling_pipeline(
            urls, 
            use_llm_extraction=False,  # Set to True to use LLM extraction
            save_intermediate_results=True
        )
        print(f"Successfully crawled {len(final_markdowns)} markdowns")
        
    except Exception as e:
        print(f"Error during crawling: {e}")


if __name__ == "__main__":
    asyncio.run(main())
        
    
    
    