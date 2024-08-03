from argparse import ArgumentParser
import logging
import os
from glob import glob
from urllib3.exceptions import ProtocolError
from requests.exceptions import ChunkedEncodingError 

import asyncio
import gzip
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from tqdm import tqdm

from playwright.async_api import async_playwright



def read_local_html(path: str) -> BeautifulSoup:
    """
    Reads a local HTML file and returns a BeautifulSoup object.

    Args:
        path (str): The path to the local HTML file.

    Returns:
        BeautifulSoup: A BeautifulSoup object representing the parsed HTML.
    """
    with open(path, "rb") as f:
        html = gzip.decompress(f.read()).decode("utf-8")
    return BeautifulSoup(html, "html.parser")


def handle_version(soup, text_dir, name="text"):
    if not os.path.exists(os.path.join(text_dir, f"{name}.txt.gz")):
        text = soup.find("pre", id="billTextContainer").text
        with gzip.open(os.path.join(text_dir, f"{name}.txt.gz"), "wb") as f:
            f.write(text.encode("utf-8"))
    
    format_lis = soup.find("ul", class_="cdg-summary-wrapper-list").find_all("li", recursive=False)
    
    # download PDF (if not already downloaded)
    if not os.path.exists(os.path.join(text_dir, f"{name}.pdf.gz")):
        pdf_url = [
            'https://www.congress.gov' + li.a['href']
            for li in format_lis
            if 'PDF' in li.text
        ][0]
        pdf = requests.get(pdf_url)
        with gzip.open(os.path.join(text_dir, f"{name}.pdf.gz"), "wb") as f:
            f.write(pdf.content)
    
    # xml URL should have XML in text and should have target=_blank
    if name != 'pl' and not os.path.exists(os.path.join(text_dir, f"{name}.xml.gz")):
        try:
            xml_url = [
                'https://www.congress.gov' + li.a['href']
                for li in format_lis
                if 'XML' in li.text and '?' not in li.a['href']
            ][0]
            xml = requests.get(xml_url)
            with gzip.open(os.path.join(text_dir, f"{name}.xml.gz"), "wb") as f:
                f.write(xml.content)
        except IndexError:
            print(f"Could not find XML link for {name}")
        except ChunkedEncodingError:
            print(f"ChunkedEncodingError for {name}")


async def parse(congress: int, logger: logging.Logger = None, timeout: int = 60, sleep: int = 10):
    
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        page = await browser.new_page()

        for bill_type in [
            "house-bill",
            "house-resolution",
            "house-concurrent-resolution",
            "house-joint-resolution",
            "senate-bill",
            "senate-resolution",
            "senate-concurrent-resolution",
            "senate-joint-resolution",
        ]:
            logger.info(
                f"Parsing {congress}th congress {bill_type} bills"
            ) if logger else None

            fs = glob(f"data/{congress}/{bill_type}-*/bill_text.html.gz")
            fs = sorted(fs)

            for f in tqdm(fs, desc=f"{bill_type} bills"):
                bill_soup = read_local_html(f)
                bill_dir = os.path.dirname(f)
                bill_id = os.path.basename(bill_dir)
                bill_id = bill_id.split("-")[-1]
                i = int(bill_id)

                if not bill_soup.title.text.startswith("Text - "):
                    logger.error(
                        f"Invalid page title: {bill_soup.title.text}"
                    ) if logger else None
                    continue
                
                text_dir = os.path.join(bill_dir, "text")
                os.makedirs(text_dir, exist_ok=True)
                
                # div w/ id=bill-summary
                summary = bill_soup.find("div", id="bill-summary")
                
                # get text selector: div w/ id=textSelector
                text_selector = summary.find("div", id="textSelector")
                
                # get options, if any
                options = text_selector.find_all("option")
                
                # when there are options, the value contains the relative URL
                # where the text can be found (we'll need to scrape it)
                # extract the text, and then download the XML and PDF versions as well
                if options: 
                    vers = [option['value'].split('/')[-1] for option in options]
                    urls = ['https://www.congress.gov' + option['value'] + '?format=txt' for option in options]
                    
                    # for a multi-version bill, we'll name the text files with the version
                    # e.g. ih.txt.gz, ih.pdf.gz, ih.xml.gz, ih.html.gz
                    # Note: the HTML version is for the full HTML page of the text format
                    # This is the same as the original bill_text.html.gz file for single-version bills
                    # and will duplicate the content of at least one of the files.
                    
                    for ver, url in zip(vers, urls):
                        if os.path.exists(os.path.join(text_dir, f"{ver}.html.gz")):
                            ver_soup = read_local_html(os.path.join(text_dir, f"{ver}.html.gz"))
                            read_cache = True
                        else:
                            await page.goto(url, wait_until="load", timeout=timeout * 1000)
                            
                            # validate title
                            title = await page.title()
                            if "Library of Congress" not in title:
                                raise ValueError(f"Invalid page title: {title}")
                            elif title == "Congress.gov | Library of Congress":
                                raise ValueError(f"Page not found: {page.url}")
                            
                            # compress the html content
                            html = await page.content()
                            with gzip.open(os.path.join(text_dir, f"{ver}.html.gz"), "wb") as f:
                                f.write(html.encode("utf-8"))
                            
                            ver_soup = BeautifulSoup(html, "html.parser")
                            read_cache = False
                        
                        handle_version(ver_soup, text_dir, name=ver)
                        
                        # sleep for a while
                        if not read_cache:
                            await asyncio.sleep(sleep)

                # when there are no options, the text is directly in the page
                # extract the text, and then download the XML and PDF versions as well
                else:
                    handle_version(bill_soup, text_dir)

                logger.info(
                    f"Parsed {congress}th congress {bill_type} bill {i}"
                ) if logger else None

        await page.close()
        await browser.close()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--congress", type=int, default=117)
    args = parser.parse_args()

    asyncio.run(parse(args.congress))
