import gzip
import logging
import os
from argparse import ArgumentParser
from datetime import datetime
from glob import glob

from bs4 import BeautifulSoup
from pymongo import MongoClient
from tqdm import tqdm


# def read_local_html(congress: int, bill_type: str, i: int):
#     with open(f"data/{congress}/{bill_type}-{i:06d}/src.html.gz", "rb") as f:
#         html = gzip.decompress(f.read()).decode("utf-8")
#     return BeautifulSoup(html, "html.parser")

def read_local_html(path: str):
    with open(path, "rb") as f:
        html = gzip.decompress(f.read()).decode("utf-8")
    return BeautifulSoup(html, "html.parser")


def read_local_db(congress: int, bill_type: str, i: int, bill_collection):
    try:
        return bill_collection.find_one(
            {"congress": congress, "type": bill_type, "number": i}
        )
    except Exception as e:
        return None


def parse_php_array(array_str):
    array_str = array_str.strip("Array\n(\n").strip("\n)").strip()
    pairs = array_str.split("\n")
    parsed_dict = {}
    for pair in pairs:
        key, value = map(str.strip, pair.split("=>"))
        parsed_dict[key.strip("[] ")] = value
    return parsed_dict


def parse_overview_sponsor(string: str):
    out = dict()

    valid_titles = {"Rep.", "Sen.", "Del.", "Resident Commissioner"}
    valids = (string.startswith(title) for title in valid_titles)
    valid = any(valids)
    if not valid:
        raise ValueError(f"Invalid sponsor: {string}")

    out["title"] = [title for title in valid_titles if string.startswith(title)][0]

    string = string.lstrip(out["title"]).strip()
    # intro_str = string.split(' (')[-1].rstrip(')').replace('Introduced ', '').strip()
    string = " (".join(string.split(" (")[:-1])
    # out['date_introduced'] = datetime.strptime(intro_str, '%m/%d/%Y')

    pos_str = string.split(" [")[1].rstrip("]").strip()
    string = string.split(" [")[0]
    # either `{party}-{state}` or `{party}-{state}-{district}`
    num_hyphens = pos_str.count("-")
    if num_hyphens == 1:
        out["party"], out["state"] = pos_str.split("-")
        out["district"] = None
    elif num_hyphens == 2:
        out["party"], out["state"], out["district"] = pos_str.split("-")
    else:
        raise ValueError(f"Invalid position: {pos_str}")

    out["last_name"] = string.split(", ")[0]
    out["full_name"] = string.split(", ")[1] + " " + string.split(", ")[0]  # naive?

    return out


def parse_overview(bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None):
    # obtain overview table
    overview = (
        bill_soup.find("div", class_="overview_wrapper bill")
        .find("div", class_="overview")
        .find("table")
    )
    out = dict()
    for row in overview.find_all("tr"):
        # check if th exists
        th = row.find("th")
        tds = row.find_all("td")
        if th is None and len(tds) == 2:
            k, v = tds
        elif th and len(tds) == 1:
            k = th
            v = tds[0]
        else:
            raise ValueError(f"Invalid row: {row}")

        key = k.text.strip().rstrip(":").lower().replace(" ", "_")
        if key == "sponsor":
            out["sponsor"] = parse_overview_sponsor(v.text)
            out["sponsor"]["url"] = "https://www.congress.gov" + v.find("a")["href"]
            out["sponsor"]["bioguide_id"] = v.find("a")["href"].split("/")[-1]
        elif key in {
            "committees",
            "latest_action",
            "latest_action_(modified)",
            "roll_call_votes",
            "committee_meetings",
            "committee_prints",
            "notes",
        }:
            continue
        elif key.startswith("tracker"):
            li_elements = v.select("ol.bill_progress li")
            steps = []
            for li in li_elements:
                div = li.find("div", class_="sol-step-info")
                if div:
                    # Get the text content of the 'div'
                    array_text = div.get_text()
                    # Parse the text content into a dictionary
                    step_info = parse_php_array(array_text)
                    step_info["actionDate"] = datetime.strptime(
                        step_info["actionDate"], "%Y-%m-%d"
                    )

                    update_mapping = {
                        "actionDate": "date",
                        "description": "type",
                        "displayText": "text",
                        "externalActionCode": "code",
                        "chamberOfAction": "chamber",
                    }
                    for k, v in update_mapping.items():
                        step_info[v] = step_info.pop(k)

                    steps.append(step_info)
            out["tracker"] = steps
        elif key == "committee_reports":
            out["reports"] = [
                {"url": "https://www.congress.gov" + a["href"], "title": a.text}
                for a in v.find_all("a")
            ]
        else:
            raise NotImplementedError(f"Key {key} not implemented")

    bill.update(out)
    return bill


def parse_authority_statement(soup: BeautifulSoup):
    script_tag = soup.find("script").string

    # Extract the part of the script with the constitutional authority statement
    start = script_tag.find("var msg = '") + len("var msg = '")
    end = script_tag.find("';", start)
    html = script_tag[start:end]
    html = html.replace('\\"', '"')

    bs4_html = BeautifulSoup(html, "html.parser")

    def _f(tag):
        out = ""
        for child in tag.children:
            if isinstance(child, str):
                out += child
            elif child.name == "br":
                out += "\n"
            elif child.name == "a":
                out += child.text
            elif child.name == "h3":
                break
            elif child.name == "ls-thn-eq" or child.name == "bullet":
                # recurse
                out += _f(child)
            else:
                raise ValueError(f"Invalid tag: {child.name}")
        return out

    constitutional_authority_statement = _f(bs4_html)
    return constitutional_authority_statement.strip()


def parse_cbo_estimates(soup: BeautifulSoup):
    script_tag = soup.find("script").string

    # Extract the part of the script with the CBO estimates
    start = script_tag.find("var msg = '") + len("var msg = '")
    end = script_tag.find("';", start)
    html = script_tag[start:end]
    html = html.replace('\\"', '"')

    bs4_html = BeautifulSoup(html, "html.parser")

    cbo_estimates = []
    for a in bs4_html.find_all("a"):
        if "Cost Estimates Search page" in a.text:
            continue
        cbo_estimates.append({"url": a["href"], "title": a.text})

    return cbo_estimates


def parse_tertiary(bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None):
    overview = bill_soup.find("div", class_="overview_wrapper bill").find(
        "div", class_="tertiary"
    )

    bill["constitutional_authority_statement"] = None
    bill["cbo_estimates"] = None
    bill["policy_area"] = None
    for div in overview.find_all("div", class_="tertiary_section"):
        try:
            head = div.find("h3").text.strip()
        except AttributeError:
            continue
        if head == "More on This Bill":
            lis = div.find_all("li")
            for li in lis:
                a = li.find("a")
                if a["id"] == "constAuthButton":
                    bill["constitutional_authority_statement"] = (
                        parse_authority_statement(li)
                    )
                elif a["id"] == "cboEstimateButton":
                    bill["cbo_estimates"] = parse_cbo_estimates(li)
                elif a["id"] == "supportingMembersBtn":
                    pass  # skip for now, doesn't seem to appear in all but a handful of bills
                else:
                    raise NotImplementedError(f"ID {a['id']} not implemented")
        elif head == "Subject — Policy Area:":
            lis = div.find_all("li")
            bill["policy_area"] = lis[0].text.strip()

    return bill


def parse_titles(bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None):
    titles_content = bill_soup.find("div", id="titles-content").find(
        "div", id="titles_main"
    )
    child_divs = titles_content.find_all("div", recursive=False)

    if len(child_divs) == 3:
        child_div_classes = [div["class"][0] for div in child_divs]
        child_div_expected = ["shortTitles", "titles-row", "officialTitles"]
        assert all(
            [a == b for a, b in zip(child_div_classes, child_div_expected)]
        ), f"Invalid div classes: {child_div_classes}"
        all_titles = child_divs[1:]
    elif len(child_divs) == 1:
        all_titles = child_divs[:]
    else:
        raise ValueError(f"Invalid number of child divs: {len(child_divs)}")

    titles = []
    for _titles in all_titles:
        for chamber in ["house", "senate"]:
            _sel = _titles.find("div", class_=chamber + "-column")
            if _sel:
                for h4 in _sel.find_all("h4"):
                    neighbor = h4.find_next_sibling()
                    if neighbor.name == "p":
                        txts = set()
                        for txt in neighbor.stripped_strings:
                            txts.add(txt)

                        for txt in txts:
                            titles.append(
                                {
                                    "scope": "full",
                                    "type": "official"
                                    if "Official" in h4.text
                                    else "short",
                                    "chamber": chamber,
                                    "title": txt,
                                    "label": h4.text,
                                }
                            )
                    else:
                        continue

                for h5 in _sel.find_all("h5"):
                    neighbor = h5.find_next_sibling()
                    if neighbor.name == "ul":
                        for li in neighbor.find_all("li"):
                            titles.append(
                                {
                                    "scope": "partial",
                                    "type": "official"
                                    if "Official" in h5.text
                                    else "short",
                                    "chamber": chamber,
                                    "title": li.text,
                                    "label": h5.text,
                                }
                            )
                    else:
                        continue
            else:
                logger.warning(
                    f"Chamber titles column not found: {chamber}"
                ) if logger else None

    bill["titles"] = titles

    return bill


def parse_actions(bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None):
    actions = []
    
    div = bill_soup.find("div", {"id": "allActions-content"})
    if div is None:
        return bill
    
    # parse and validate action table columns
    header = div.find("thead").find("tr")
    if header is None:
        return bill
    
    def _parse_date_time(date_str):
        # Define the patterns
        date_time_pattern = "%m/%d/%Y-%I:%M%p"
        date_pattern = "%m/%d/%Y"
        
        try:
            # Try to parse the full datetime pattern
            return datetime.strptime(date_str, date_time_pattern)
        except ValueError:
            # If it fails, try to parse just the date pattern
            return datetime.strptime(date_str, date_pattern)
        
    columns = header.find_all("th")
    col_names = [col.text.strip().lower() for col in columns]
    x = set(col_names)
    
    body = div.find("tbody")
    
    if x == {"date", "chamber", "all actions"}:        
        key_upd = {
            'date': 'date',
            'chamber': 'by',
            'all actions': 'action'
        }
        keys = [key_upd[col] for col in col_names]
        for row in body.find_all("tr"):
            action = dict(zip(keys, row.find_all("td")))
            y = {
                'date': _parse_date_time(action['date'].text.strip()),
                'by': action['by'].text.strip(),
                'action': action['action'].text.strip(),
                'links': [
                    {
                        'text': link.text.strip(),
                        'url': link['href'] if link['href'].startswith("http") else "https://www.congress.gov" + link['href']
                    }
                    for link in action['action'].find_all("a")
                ]
            }
            actions.append(y)
    elif x == {"date", "all actions"}:        
        key_upd = {
            'date': 'date',
            'all actions': 'action'
        }
        keys = [key_upd[col] for col in col_names]
        
        # because no chamber key, by is now frequently mentioned
        # in a <span> tag within the action column
        # so we need to extract it from there
        # and validate that:
        # - it is present
        # - it starts with "Action By:" once stripped
        # additionally, a <br> tag precedes this <span> and 
        # neither should be included in the action text itself
        
        for row in body.find_all("tr"):
            action = dict(zip(keys, row.find_all("td")))
            by = action['action'].find("span")
            action_text = action['action'].text.strip()
            if by is not None:
                by = by.text.strip()
                if by == '':
                    by = None
                else:
                    if not by.startswith("Action By:"):
                        print("Invalid 'by' column")
                        print(by)
                        raise ValueError("Invalid 'by' column")
                    action_text = action_text.replace(by, "").strip()
                    by = by.replace("Action By:", "").strip()
                
            y = {
                'date': _parse_date_time(action['date'].text.strip()),
                'by': by,
                'action': action_text,
                'links': [
                    {
                        'text': link.text.strip(),
                        'url': link['href'] if link['href'].startswith("http") else "https://www.congress.gov" + link['href']
                    }
                    for link in action['action'].find_all("a")
                ]
            }
            actions.append(y)
    else:
        print("Invalid columns")
        print(x)
        raise ValueError("Invalid columns")
    
    bill["actions"] = actions
    
    return bill


def parse(congress: int, logger: logging.Logger = None):
    client = MongoClient()
    db = client.federal
    collection = db.bills

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
        
        fs = glob(f"data/{congress}/{bill_type}-*/src.html.gz")
        fs = sorted(fs)
        
        for f in tqdm(fs, desc=f"{bill_type} bills"):
            bill_soup = read_local_html(f)
            bill_dir = os.path.dirname(f)
            bill_id = os.path.basename(bill_dir)
            bill_id = bill_id.split("-")[-1]
            i = int(bill_id)
            
            if not bill_soup.title.text.startswith("All Info - "):
                logger.error(
                    f"Invalid page title: {bill_soup.title.text}"
                ) if logger else None
                continue

            bill = read_local_db(congress, bill_type, i, collection)
            bill_ = {"congress": congress, "type": bill_type, "number": i}
            if bill is None:
                # initial insert
                collection.insert_one(bill_)
            bill = bill_

            bill = parse_overview(bill, bill_soup, logger=logger)
            bill = parse_tertiary(bill, bill_soup, logger=logger)
            bill = parse_titles(bill, bill_soup, logger=logger)
            bill = parse_actions(bill, bill_soup, logger=logger)

            # save updated bill
            collection.update_one(
                {"congress": congress, "type": bill_type, "number": i}, {"$set": bill}
            )

            logger.info(
                f"Parsed {congress}th congress {bill_type} bill {i}"
            ) if logger else None


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--congress", type=int, default=117)
    args = parser.parse_args()

    # setup logger to write to file and console
    # os.makedirs("logs", exist_ok=True)
    # logger = logging.getLogger(__name__)
    # logger.setLevel(logging.INFO)
    # formatter = logging.Formatter("%(asctime)s::%(name)s::%(levelname)s::%(message)s")
    # handlers = [
    #     logging.FileHandler(
    #         f"logs/parse-bill-{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
    #     ),
    #     logging.StreamHandler(),
    # ]
    # for handler in handlers:
    #     handler.setFormatter(formatter)
    #     handler.setLevel(logging.INFO)
    #     logger.addHandler(handler)

    # logger.info(f"Parsing {args.congress}th congress bills")
    # logger.info(args)
    # parse(args.congress, logger=logger)
    # logger.info(f"Parsing completed")
    
    parse(args.congress)
