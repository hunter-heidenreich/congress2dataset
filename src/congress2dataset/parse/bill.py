import gzip
import logging
import os
from argparse import ArgumentParser
from datetime import datetime
from glob import glob

from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from tqdm import tqdm


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


def read_local_db(
    congress: int, bill_type: str, i: int, bill_collection: Collection
) -> dict:
    """
    Retrieves a bill from the local database based on the given parameters.

    Args:
        congress (int): The congress number.
        bill_type (str): The type of bill.
        i (int): The bill number.
        bill_collection (Collection): The MongoDB collection containing the bills.

    Returns:
        dict: The bill document from the local database, or None if not found.
    """
    try:
        return bill_collection.find_one(
            {"congress": congress, "type": bill_type, "number": i}
        )
    except Exception as e:
        return None


def parse_php_array(array_str: str) -> dict:
    """
    Parses a PHP array string and returns a dictionary.

    Args:
        array_str (str): The PHP array string to be parsed.

    Returns:
        dict: A dictionary containing the parsed key-value pairs from the PHP array.

    Example:
        >>> array_str = "Array\n(\n    [key1] => value1\n    [key2] => value2\n)"
        >>> parse_php_array(array_str)
        {'key1': 'value1', 'key2': 'value2'}
    """
    array_str = array_str.strip("Array\n(\n").strip("\n)").strip()
    pairs = array_str.split("\n")
    parsed_dict = {}
    for pair in pairs:
        key, value = map(str.strip, pair.split("=>"))
        parsed_dict[key.strip("[] ")] = value
    return parsed_dict


def parse_overview_sponsor(string: str) -> dict:
    """
    Parse the sponsor information from a string.

    Args:
        string (str): The string containing the sponsor information.

    Returns:
        dict: A dictionary containing the parsed sponsor information.

    Raises:
        ValueError: If the sponsor information is invalid.
    """
    out = dict()

    valid_titles = {"Rep.", "Sen.", "Del.", "Resident Commissioner"}
    valids = (string.startswith(title) for title in valid_titles)
    valid = any(valids)
    if not valid:
        raise ValueError(f"Invalid sponsor: {string}")

    out["title"] = [title for title in valid_titles if string.startswith(title)][0]

    if "(Private Legislation)" in string:
        string = string.replace("(Private Legislation)", "").strip()

    string = string.lstrip(out["title"]).strip()
    string = " (".join(string.split(" (")[:-1])

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


def parse_overview(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parses the overview table of a bill and updates the bill dictionary with the parsed information.

    Args:
        bill (dict): The bill dictionary to be updated.
        bill_soup (BeautifulSoup): The BeautifulSoup object containing the bill HTML.
        logger (logging.Logger, optional): The logger object for logging messages. Defaults to None.

    Returns:
        dict: The updated bill dictionary.
    """

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


def parse_authority_statement(soup: BeautifulSoup) -> str:
    """
    Parses the constitutional authority statement from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object representing the HTML page.

    Returns:
        str: The parsed constitutional authority statement.
    """

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
    constitutional_authority_statement = constitutional_authority_statement.strip()

    # lambda x: '\n'.join([l for l in x.split('\n') if not l.startswith('[') and not l.endswith(']') and l != ''])
    constitutional_authority_statement = "\n".join(
        [
            l
            for l in constitutional_authority_statement.split("\n")
            if not l.startswith("[") and not l.endswith("]") and l != ""
        ]
    )

    return constitutional_authority_statement


def parse_cbo_estimates(soup: BeautifulSoup) -> list[dict]:
    """
    Parses CBO estimates from the given BeautifulSoup object.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object containing the HTML.

    Returns:
        list[dict]: A list of dictionaries representing the CBO estimates.
            Each dictionary contains the URL and title of an estimate.
    """
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


def parse_tertiary(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parses the tertiary section of a bill's overview and updates the bill dictionary with the parsed information.

    Args:
        bill (dict): The bill dictionary to be updated.
        bill_soup (BeautifulSoup): The BeautifulSoup object containing the bill's HTML.
        logger (logging.Logger, optional): The logger object for logging messages. Defaults to None.

    Returns:
        dict: The updated bill dictionary.
    """

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


def parse_titles(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parses the titles of a bill from the given BeautifulSoup object.

    Args:
        bill (dict): The bill dictionary to update with the parsed titles.
        bill_soup (BeautifulSoup): The BeautifulSoup object representing the bill HTML.
        logger (logging.Logger, optional): The logger object for logging warnings. Defaults to None.

    Returns:
        dict: The updated bill dictionary with the parsed titles.

    Raises:
        ValueError: If the number of child divs is invalid.
    """
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
                                    "label": h4.text.split("as")[-1]
                                    .split("for")[0]
                                    .strip(),
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
                                    "label": h5.text.split("as")[-1]
                                    .split("for")[0]
                                    .strip(),
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


def parse_actions(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parse the actions of a bill from the given BeautifulSoup object and update the bill dictionary.

    Args:
        bill (dict): The bill dictionary to update with the parsed actions.
        bill_soup (BeautifulSoup): The BeautifulSoup object representing the bill page.
        logger (logging.Logger, optional): The logger object for logging messages. Defaults to None.

    Returns:
        dict: The updated bill dictionary.

    Raises:
        ValueError: If the columns in the action table are invalid.

    """
    actions = []

    div = bill_soup.find("div", {"id": "allActions-content"})
    if div is None:
        bill["actions"] = actions
        return bill

    # parse and validate action table columns
    header = div.find("thead").find("tr")
    if header is None:
        bill["actions"] = actions
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
        key_upd = {"date": "date", "chamber": "by", "all actions": "action"}
        keys = [key_upd[col] for col in col_names]
        for row in body.find_all("tr"):
            action = dict(zip(keys, row.find_all("td")))
            y = {
                "date": _parse_date_time(action["date"].text.strip()),
                "by": action["by"].text.strip(),
                "action": action["action"].text.strip(),
                "links": [
                    {
                        "text": link.text.strip(),
                        "url": link["href"]
                        if link["href"].startswith("http")
                        else "https://www.congress.gov" + link["href"],
                    }
                    for link in action["action"].find_all("a")
                ],
            }
            actions.append(y)
    elif x == {"date", "all actions"}:
        key_upd = {"date": "date", "all actions": "action"}
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
            by = action["action"].find("span")
            action_text = action["action"].text.strip()
            if by is not None:
                by = by.text.strip()
                if by == "":
                    by = None
                else:
                    if not by.startswith("Action By:"):
                        print("Invalid 'by' column")
                        print(by)
                        raise ValueError("Invalid 'by' column")
                    action_text = action_text.replace(by, "").strip()
                    by = by.replace("Action By:", "").strip()

            y = {
                "date": _parse_date_time(action["date"].text.strip()),
                "by": by,
                "action": action_text,
                "links": [
                    {
                        "text": link.text.strip(),
                        "url": link["href"]
                        if link["href"].startswith("http")
                        else "https://www.congress.gov" + link["href"],
                    }
                    for link in action["action"].find_all("a")
                ],
            }
            actions.append(y)
    else:
        print("Invalid columns")
        print(x)
        raise ValueError("Invalid columns")

    bill["actions"] = actions

    return bill


def parse_cosponsor(string: str) -> dict:
    """
    Parse the cosponsor information from a string.

    Args:
        string (str): The string containing the cosponsor information.

    Returns:
        dict: A dictionary containing the parsed cosponsor information.

    Raises:
        ValueError: If the cosponsor information is invalid.
    """
    out = dict()

    valid_titles = {"Rep.", "Sen.", "Del.", "Resident Commissioner"}
    valids = (string.startswith(title) for title in valid_titles)
    valid = any(valids)
    if not valid:
        raise ValueError(f"Invalid sponsor: {string}")

    out["title"] = [title for title in valid_titles if string.startswith(title)][0]

    string = string.lstrip(out["title"]).strip()

    pos_str = string.split(" [")[1].split("]")[0].strip()
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


def parse_consponsors(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parse the cosponsors of a bill from the given BeautifulSoup object and update the bill dictionary.

    Args:
        bill (dict): The bill dictionary to update with the parsed cosponsors.
        bill_soup (BeautifulSoup): The BeautifulSoup object representing the bill page.
        logger (logging.Logger, optional): The logger object for logging messages. Defaults to None.

    Returns:
        dict: The updated bill dictionary.

    Raises:
        ValueError: If the columns in the cosponsors table are invalid.
    """
    consponsors = []

    div = bill_soup.find("div", {"id": "cosponsors-content"})
    if div is None:
        bill["cosponsors"] = consponsors
        return bill

    # parse and validate cosponsors table columns
    try:
        header = div.find("thead").find("tr")
    except AttributeError:
        bill["cosponsors"] = consponsors
        return bill

    if header is None:
        bill["cosponsors"] = consponsors
        return bill

    columns = header.find_all("th")
    col_names = [col.text.strip().lower() for col in columns]
    x = set(col_names)

    body = div.find("tbody")

    if x == {"cosponsor", "date cosponsored"}:
        key_upd = {"cosponsor": "cosponsor", "date cosponsored": "date"}
        keys = [key_upd[col] for col in col_names]
        for row in body.find_all("tr"):
            consponsor = dict(zip(keys, row.find_all("td")))
            y = {
                "cosponsor": parse_cosponsor(consponsor["cosponsor"].text.strip()),
                "date": datetime.strptime(consponsor["date"].text.strip(), "%m/%d/%Y"),
                "withdrawn": None,
            }
            consponsors.append(y)
    elif x == {
        "cosponsors who withdrew",
        "date cosponsored",
        "date withdrawn",
        "cr explanation",
    }:
        key_upd = {
            "cosponsors who withdrew": "cosponsor",
            "date cosponsored": "date",
            "date withdrawn": "date withdrawn",
            "cr explanation": "cr explanation",
        }
        keys = [key_upd[col] for col in col_names]
        for row in body.find_all("tr"):
            consponsor = dict(zip(keys, row.find_all("td")))
            y = {
                "cosponsor": parse_cosponsor(consponsor["cosponsor"].text.strip()),
                "date": datetime.strptime(consponsor["date"].text.strip(), "%m/%d/%Y"),
                "withdrawn": {
                    "date": datetime.strptime(
                        consponsor["date withdrawn"].text.strip(), "%m/%d/%Y"
                    ),
                    "explanation": {
                        "text": consponsor["cr explanation"].text.strip(),
                        "url": consponsor["cr explanation"].find("a")["href"],
                    },
                },
            }
            consponsors
    else:
        raise ValueError(f"Invalid columns: {x} ({bill['source']})")

    bill["cosponsors"] = consponsors

    return bill


def parse_committees(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    """
    Parse the committees of a bill from the given BeautifulSoup object and update the bill dictionary.

    Args:
        bill (dict): The bill dictionary to update with the parsed committees.
        bill_soup (BeautifulSoup): The BeautifulSoup object representing the bill page.
        logger (logging.Logger, optional): The logger object for logging messages. Defaults to None.

    Returns:
        dict: The updated bill dictionary.

    Raises:
        ValueError: If the columns in the committees table are invalid.
    """
    committees = []

    div = bill_soup.find("div", {"id": "committees-content"})
    if div is None:
        bill["committees"] = committees
        return bill

    # parse and validate committees table columns
    head = div.find("thead")
    if head is None:
        bill["committees"] = committees
        return bill

    header = head.find("tr")
    if header is None:
        bill["committees"] = committees
        return bill

    columns = header.find_all("th")
    col_names = [col.text.strip().lower() for col in columns]
    x = set(col_names)

    body = div.find("tbody")

    if x == {"committee / subcommittee", "date", "activity", "related documents"}:
        key_upd = {
            "committee / subcommittee": "name",
            "date": "date",
            "activity": "activity",
            "related documents": "related documents",
        }
        keys = [key_upd[col] for col in col_names]

        # each row in the committee table should be a <th> and 3 <td> elements per row
        # the <th> is the committee name
        # the first <td> is the date
        # the second <td> is the activity
        # the third <td> is the related documents

        def _parse_date_time(date_str):
            # Define the patterns
            date_time_pattern = "%m/%d/%Y-%I:%M%p"
            date_pattern = "%m/%d/%Y"

            try:
                # Try to parse the full datetime pattern
                return datetime.strptime(date_str, date_time_pattern)
            except ValueError:
                # If it fails, try to parse just the date pattern
                try:
                    return datetime.strptime(date_str, date_pattern)
                except ValueError:
                    return None

        # when <th> isn't present, the same committee/subcommittee is repeated
        cur_th = None
        cur_sub = False
        for row in body.find_all("tr"):
            if row.find("th"):
                cur_th = row.find("th")
                cur_sub = "subcommittee" in row["class"]

            committee = dict(zip(keys, [cur_th] + row.find_all("td")))
            y = {
                "name": committee["name"].text.strip(),
                "date": _parse_date_time(committee["date"].text.strip()),
                "activity": committee["activity"].text.strip(),
                "related_documents": [
                    {
                        "text": link.text.strip(),
                        "url": link["href"]
                        if link["href"].startswith("http")
                        else "https://www.congress.gov" + link["href"],
                    }
                    for link in committee["related documents"].find_all("a")
                ],
                "is_subcommittee": cur_sub,
            }
            committees.append(y)
    else:
        raise ValueError(f"Invalid columns: {x} ({bill['source']})")

    bill["committees"] = committees

    return bill


def parse_related(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    related = []

    div = bill_soup.find("div", {"id": "relatedBills-content"})
    if div is None:
        bill["related"] = related
        return bill

    # parse and validate related table columns
    head = div.find("thead")
    if head is None:
        bill["related"] = related
        return bill

    header = head.find("tr")
    if header is None:
        bill["related"] = related
        return bill

    columns = header.find_all("th")
    col_names = [col.text.strip().lower() for col in columns]
    col_names = [
        "relationships to" if col.startswith("relationships to") else col
        for col in col_names
    ]
    x = set(col_names)

    body = div.find("tbody")

    if x == {
        "bill",
        "latest title",
        "relationships to",
        "relationships identified by",
        "latest action",
    }:
        key_upd = {
            "bill": "bill",
            "latest title": "title",
            "relationships to": "relationship",
            "relationships identified by": "by",
            "latest action": "latest action",
        }
        keys = [key_upd[col] for col in col_names]

        for row in body.find_all("tr"):
            # skip extra rows for now
            # check against class="relatedbill_exrow"
            if "relatedbill_exrow" in row.get("class", []):
                continue

            related_bill = dict(zip(keys, row.find_all("td")))
            y = {
                # "bill": {
                # "text": related_bill["bill"].text.strip(),
                "url": related_bill["bill"].find("a")["href"]
                if "http" in related_bill["bill"].find("a")["href"]
                else "https://www.congress.gov"
                + related_bill["bill"].find("a")["href"],
                # },
                # "title": related_bill["title"].text.strip(),
                "relationship": "Procedurally related"
                if related_bill["relationship"]
                .text.strip()
                .startswith("Procedurally related")
                else related_bill["relationship"].text.strip(),
                "by": related_bill["by"].text.strip(),
                # "latest_action": related_bill["latest action"].text.strip(),
            }

            related.append(y)
    else:
        raise ValueError(f"Invalid columns: {x} ({bill['source']})")

    bill["related"] = related

    return bill


def parse_subjects(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    subjects = []

    div = bill_soup.find("div", {"id": "subjects-content"})
    if div is None:
        bill["subjects"] = subjects
        return bill

    # first, the policy area (if present)
    pa_div = div.find("div", class_="search-column-nav")
    lis = pa_div.find_all("li")
    if len(lis) == 0:
        pass
    elif len(lis) == 1:
        assert (
            bill["policy_area"] == lis[0].text.strip()
        ), f"Policy area mismatch: {bill['policy_area']} vs {lis[0].text.strip()}"
    else:
        raise ValueError(f"Invalid number of policy areas: {len(lis)}")

    # parse and validate subject div
    sub_div = div.find("div", class_="search-column-main")
    if sub_div is None:
        bill["subjects"] = subjects
        return bill

    for li in sub_div.find_all("li"):
        subjects.append(li.text.strip())

    bill["subjects"] = subjects

    if (
        len(subjects)
        and bill["policy_area"] is None
        and "Private Legislation" in subjects
    ):
        # for a subset of private legislation bills, the policy area is not present but should be
        bill["policy_area"] = "Private Legislation"

    return bill


def parse_summaries(
    bill: dict, bill_soup: BeautifulSoup, logger: logging.Logger = None
) -> dict:
    summaries = []

    div = bill_soup.find("div", {"id": "allSummaries-content"})
    if div is None:
        bill["summaries"] = summaries
        return bill

    for sum_div in div.find_all("div", recursive=False):
        if sum_div.get("id").startswith("summary-"):
            summary = {
                "title": sum_div.find("h3")
                .text.strip()
                .replace("Shown Here:", "")
                .strip()
                .split("(")[0]
                .strip(),
                "text": "\n".join([p.text for p in sum_div.find_all("p")]),
            }
            summaries.append(summary)

    bill["summaries"] = summaries

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
            bill_ = {
                "congress": congress,
                "type": bill_type,
                "number": i,
                "source": f"https://www.congress.gov/bill/{congress}th-congress/{bill_type}/{i}/all-info/?allSummaries=show",
            }
            if bill is None:
                # initial insert
                collection.insert_one(bill_)
            bill = bill_

            bill = parse_overview(bill, bill_soup, logger=logger)
            bill = parse_tertiary(bill, bill_soup, logger=logger)
            bill = parse_titles(bill, bill_soup, logger=logger)
            bill = parse_actions(bill, bill_soup, logger=logger)
            bill = parse_consponsors(bill, bill_soup, logger=logger)
            bill = parse_committees(bill, bill_soup, logger=logger)
            bill = parse_related(bill, bill_soup, logger=logger)
            bill = parse_subjects(bill, bill_soup, logger=logger)
            bill = parse_summaries(bill, bill_soup, logger=logger)

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
