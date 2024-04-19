import asyncio
import os
from argparse import ArgumentParser

from playwright.async_api import async_playwright
from pymongo import MongoClient


async def scrape(
    congress: int, type: str, start: int, end: int, sleep: int, timeout: int = 60
):
    os.makedirs("./screenshots", exist_ok=True)

    client = MongoClient()
    db = client.federal
    collection = db.bills
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        page = await browser.new_page()

        for i in range(start, end + 1):
            # check if the bill is already in the database
            if collection.find_one({"congress": congress, "type": type, "number": i}):
                print(f"Bill {congress}-{type}-{i} already in the database")
                continue

            # load the page
            # https://www.congress.gov/bill/117th-congress/house-bill/1/all-info?allSummaries=show
            url = f"https://www.congress.gov/bill/{congress}th-congress/{type}/{i}/all-info/?allSummaries=show"
            print(f"Loading {url}")
            await page.goto(url, timeout=timeout * 1000)
            await page.wait_for_load_state("domcontentloaded")
            print(f"Loaded {page.url}")

            # validate title
            title = await page.title()
            if "Library of Congress" not in title:
                raise ValueError(f"Invalid page title: {title}")

            # get description of page
            website_description = await page.query_selector('meta[name="description"]')
            if website_description is None:
                description = ""
            else:
                description = await website_description.evaluate(
                    "(element) => element.content"
                )

            # take a screenshot
            await page.screenshot(path=f"./screenshots/{congress}-{type}-{i}.jpg")

            # store the html content in MongoDB
            html = await page.content()
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
            print(f"Bill {congress}-{type}-{i} saved HTML to the database")

            # sleep for a while
            await asyncio.sleep(sleep)

        await browser.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("congress", type=int, default=117)
    parser.add_argument("--type", type=str, required=False, default="house-bill")
    parser.add_argument("--start", type=int, required=False, default=1)
    parser.add_argument("--end", type=int, required=False, default=1)
    parser.add_argument("--sleep", type=int, required=False, default=10)

    args = parser.parse_args()
    asyncio.run(scrape(args.congress, args.type, args.start, args.end, args.sleep))
