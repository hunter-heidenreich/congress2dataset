import asyncio
import gzip
import logging
import os
from argparse import ArgumentParser
from datetime import datetime

from playwright.async_api import async_playwright



async def scrape(congress: int, sleep: int, timeout: int = 60, logger: logging.Logger = None):
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()
        
        for bill_type in [
            'house-bill', 'house-resolution', 'house-concurrent-resolution', 'house-joint-resolution',
            'senate-bill', 'senate-resolution', 'senate-concurrent-resolution', 'senate-joint-resolution'
        ]:
            logger.info(f"Scraping {congress}th congress {bill_type} bill texts") if logger else None
            i = 1
            run = True
            while run:
                
                # check if already have compressed html
                if os.path.exists(f'data/117/{bill_type}-{i:06d}/bill_text.html.gz'):
                    logger.info(f"Bill {congress}-{bill_type}-{i} already scraped") if logger else None
                    i += 1
                    continue
            
                # load the page
                url = f"https://www.congress.gov/bill/{congress}th-congress/{bill_type}/{i}/text/?format=txt"
                logger.info(f"Loading {url}") if logger else None
                await page.goto(url, wait_until="load", timeout=timeout * 1000)
                logger.info(f"Loaded {page.url}") if logger else None

                # validate title
                title = await page.title()
                try:
                    if "Library of Congress" not in title:
                        raise ValueError(f"Invalid page title: {title}")
                    elif title == "Congress.gov | Library of Congress":
                        raise ValueError(f"Page not found: {page.url}")
                except ValueError:
                    run = input(f"Error: {title}. Retry? (y/n) ") == "y"
                    print()
                    continue
                
                logger.info(f"Title: {title}") if logger else None
                
                # compress the html content
                html = await page.content()
                html = gzip.compress(html.encode("utf-8"))
                
                # save local copy
                bill_dir = f'data/117/{bill_type}-{i:06d}'
                os.makedirs(bill_dir, exist_ok=True)
                with open(f'{bill_dir}/bill_text.html.gz', 'wb') as f:
                    f.write(html)
                    
                # sleep for a while
                await asyncio.sleep(sleep)
                
                # increment the bill number
                i += 1
                    
        await page.close()
        await browser.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--congress", type=int, default=117)
    parser.add_argument("--sleep", type=int, required=False, default=10)

    args = parser.parse_args()
    
    # setup logger to write to file and console
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s::%(name)s::%(levelname)s::%(message)s")
    handlers = [
        logging.FileHandler(f"logs/scrape-bill_text-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"),
        logging.StreamHandler(),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        
    logger.info(f"Scraping {args.congress}th congress bill texts")
    logger.info(args)
    asyncio.run(scrape(args.congress, args.sleep, logger=logger))
    logger.info(f"Scraping completed")
