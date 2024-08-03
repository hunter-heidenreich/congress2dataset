import gzip
import os
import time
import requests

from bs4 import BeautifulSoup


def scrape_house_roll_call(year: int, roll_call: int):
    url = f"https://clerk.house.gov/Votes/{year}{roll_call}"
    html = requests.get(url).text
    return html


def scrape_house_year(year: int, outdir: str, sleep: int = 1):
    roll_call = 1
    
    while True:
        path = os.path.join(outdir, f"{year}-{roll_call:04d}.html.gz")
        if os.path.exists(path):
            roll_call += 1
            continue
        
        html = scrape_house_roll_call(year, roll_call)
        soup = BeautifulSoup(html, "html.parser")
        
        # validate <h1> tag
        h1 = soup.find("h1")
        if h1 is None:
            break
        
        if "roll call vote not available" in h1.text.lower():
            break
        
        # save html
        with gzip.open(path, "wb") as f:
            f.write(html.encode("utf-8"))

        time.sleep(sleep)
        
        if roll_call % 10 == 0:
            print(f"Scraped {year} roll call {roll_call}")
    
def scrape_house_117():
    outdir = "data/117/house-roll-call"
    os.makedirs(outdir, exist_ok=True)
    scrape_house_year(2021, outdir)
    scrape_house_year(2022, outdir)


if __name__ == "__main__":
    scrape_house_117()
    