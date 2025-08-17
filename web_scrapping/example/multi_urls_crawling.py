import asyncio
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

async def main():
    urls = [
        "https://vietnamnet.vn/",
        "https://vnexpress.net/",
        "https://tuoitre.vn/"
    ]

    config = CrawlerRunConfig(
        cache_mode=CacheMode.ENABLED,
        check_robots_txt=True,  # Will respect robots.txt for each URL
        semaphore_count=3      # Max concurrent requests
    )

    async with AsyncWebCrawler() as crawler:
        results = await crawler.arun_many(urls, config=config)
        
        for result in results:
            if result.success:
                print(f"Successfully crawled {result.url}")
            elif result.status_code == 403 and "robots.txt" in result.error_message:
                print(f"Skipped {result.url} - blocked by robots.txt")
            else:
                print(f"Failed to crawl {result.url}: {result.error_message}")
            
            with open("results.json", "a") as f:
                f.write(result.markdown.raw_markdown)

if __name__ == "__main__":
    asyncio.run(main())
