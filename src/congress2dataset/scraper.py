import asyncio
import gzip
import logging
import os
from argparse import ArgumentParser
from datetime import datetime

from playwright.async_api import async_playwright
from pymongo import MongoClient


async def scrape(
    congress: int, type: str, start: int, end: int, sleep: int, timeout: int = 60, logger: logging.Logger = None
):
    client = MongoClient()
    db = client.federal
    collection = db.bills
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        page = await browser.new_page()

        for i in range(start, end + 1):
            # check if the bill is already in the database
            if collection.find_one({"congress": congress, "type": type, "number": i}):
                logger.info(f"Bill {congress}-{type}-{i} already in the database") if logger else None
                continue

            # load the page
            # https://www.congress.gov/bill/117th-congress/house-bill/1/all-info?allSummaries=show
            url = f"https://www.congress.gov/bill/{congress}th-congress/{type}/{i}/all-info/?allSummaries=show"
            logger.info(f"Loading {url}") if logger else None
            await page.goto(url, wait_until="load", timeout=timeout * 1000)
            logger.info(f"Loaded {page.url}") if logger else None

            # validate title
            title = await page.title()
            if "Library of Congress" not in title:
                raise ValueError(f"Invalid page title: {title}")
            elif title == "Congress.gov | Library of Congress":
                raise ValueError(f"Page not found: {page.url}")
            logger.info(f"Title: {title}") if logger else None

            # get description of page
            website_description = await page.query_selector('meta[name="description"]')
            if website_description is None:
                description = ""
            else:
                description = await website_description.evaluate(
                    "(element) => element.content"
                ) 
            logger.info(f"Description: {description}") if logger else None

            # store the html content in MongoDB
            html = await page.content()
            html = gzip.compress(html.encode("utf-8"))
            collection.insert_one(
                {
                    "congress": congress,
                    "type": type,
                    "number": i,
                    "source": {
                        "url": url,
                        "title": title,
                        "html": html,
                        "description": description,
                    },
                }
            )
            # print(f"Bill {congress}-{type}-{i} saved HTML to the database")
            logger.info(f"Bill {congress}-{type}-{i} saved HTML to the database") if logger else None

            # sleep for a while
            await asyncio.sleep(sleep)

        await page.close()
        await browser.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--congress", type=int, default=117)
    parser.add_argument("--type", type=str, required=False, default="house-bill")
    parser.add_argument("--start", type=int, required=False, default=1)
    parser.add_argument("--end", type=int, required=False, default=1)
    parser.add_argument("--sleep", type=int, required=False, default=10)

    args = parser.parse_args()
    
    # setup logger to write to file and console
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s::%(name)s::%(levelname)s::%(message)s")
    handlers = [
        logging.FileHandler(f"scraper-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"),
        logging.StreamHandler(),
    ]
    for handler in handlers:
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
    
    logger.info(f"Scraping {args.congress}th congress {args.type} bills {args.start} to {args.end}")
    
    asyncio.run(scrape(args.congress, args.type, args.start, args.end, args.sleep, logger=logger))
