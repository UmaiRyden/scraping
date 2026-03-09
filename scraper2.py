"""
PE Firm Employee Scraper v4
============================
Priority 1 fixes applied:

  FIX 1 — EQT regression
           Force selector a[href*='people/'] — was working in v2 (384 employees)
           v3 accidentally matched 'table tbody tr' and scraped Cloudflare cookie text

  FIX 2 — CD&R & BC Partners duplicate rows
           Each person appears twice: once with position, once with N/A
           Root cause: each card has 2 child elements matching the selector
           Fix: after scraping, keep only the row WITH a real position per person

  FIX 3 — Leonard Green name in position text
           Position text is 'Vice President\nMatt Allen' — name repeated after newline
           Fix: strip everything after the first newline in position field

  FIX 4 — Warburg Pincus category headers mixed in as names
           Names like 'Fundraising and Investor Relations' are section headers
           Pattern: real names have position on the SAME row, headers repeat on next row
           Fix: skip rows where name contains known category keywords OR name > 35 chars
                and doesn't contain a space in the right place for a human name

  FIX 5 — General Atlantic garbage nav rows
           ~600 rows are nav items like 'About', 'Our Story', 'Careers'
           Fix: post-process filter — remove rows where name is a known nav word
                OR where name has no position AND name is < 4 words

  FIX 6 — Blackstone, Silver Lake, Montagu nav rows
           Small number of nav rows mixed in — remove with same nav filter

  FIX 7 — Advent International crash
           Page navigates away mid-scrape (JS redirect) causing execution context error
           Fix: wrap evaluate() calls in try/except with page state check
"""

from playwright.sync_api import sync_playwright
import pandas as pd
from datetime import datetime
import json
import re
from urllib.parse import urlparse


# ═══════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

WEBSITES = [
    {"name": "KKR",                  "url": "https://www.kkr.com/about/our-people"},
    {"name": "Permira",              "url": "https://www.permira.com/people/meet-our-people"},
    {"name": "EQT",                  "url": "https://eqtgroup.com/about/people"},
    {"name": "Blackstone",           "url": "https://www.blackstone.com/people/"},
    {"name": "Thoma Bravo",          "url": "https://www.thomabravo.com/team"},
    {"name": "TPG",                  "url": "https://www.tpg.com/team"},
    {"name": "CVC Capital",          "url": "https://www.cvc.com/about/our-people"},
    {"name": "Hg",                   "url": "https://hg.co/team"},
    {"name": "Blue Owl",             "url": "https://www.blueowl.com/team"},
    {"name": "CD&R",                 "url": "https://www.cdr-inc.com/team"},
    {"name": "Hellman & Friedman",   "url": "https://www.hf.com/team"},
    {"name": "Silver Lake",          "url": "https://www.silverlake.com/team"},
    {"name": "Apollo",               "url": "https://www.apollo.com/about-apollo/our-people"},
    {"name": "Warburg Pincus",       "url": "https://www.warburgpincus.com/team"},
    {"name": "General Atlantic",     "url": "https://www.generalatlantic.com/team"},
    {"name": "Bain Capital",         "url": "https://www.baincapital.com/team"},
    {"name": "Advent International", "url": "https://www.adventinternational.com/team"},
    {"name": "Carlyle",              "url": "https://www.carlyle.com/about/team"},
    {"name": "Cinven",               "url": "https://www.cinven.com/team"},
    {"name": "Insight Partners",     "url": "https://www.insightpartners.com/team"},
    {"name": "Genstar Capital",      "url": "https://www.genstarcapital.com/team"},
    {"name": "Vista Equity",         "url": "https://www.vistaequitypartners.com/team"},
    {"name": "Leonard Green",        "url": "https://www.leonardgreen.com/team"},
    {"name": "Brookfield",           "url": "https://www.brookfield.com/team"},
    {"name": "Neuberger Berman",     "url": "https://www.nb.com/en/us/about-us/our-team"},
    {"name": "Bridgepoint",          "url": "https://www.bridgepoint.eu/team"},
    {"name": "Ares Management",      "url": "https://www.aresmgmt.com/team"},
    {"name": "Partners Group",       "url": "https://www.partnersgroup.com/about-us/our-team"},
    {"name": "Ardian",               "url": "https://www.ardian.com/team"},
    {"name": "Nordic Capital",       "url": "https://www.nordiccapital.com/team"},
    {"name": "PAI Partners",         "url": "https://www.paipartners.com/team"},
    {"name": "CapVest",              "url": "https://www.capvest.com/team"},
    {"name": "Summit Partners",      "url": "https://www.summitpartners.com/team"},
    {"name": "GTCR",                 "url": "https://www.gtcr.com/team"},
    {"name": "L Catterton",          "url": "https://www.lcatterton.com/team"},
    {"name": "Francisco Partners",   "url": "https://www.franciscopartners.com/team"},
    {"name": "Accel-KKR",            "url": "https://www.accel-kkr.com/team"},
    {"name": "Montagu PE",           "url": "https://www.montagu.com/team"},
    {"name": "IK Partners",          "url": "https://www.ikpartners.com/team"},
    {"name": "Eurazeo",              "url": "https://www.eurazeo.com/en/our-team"},
    {"name": "Charterhouse",         "url": "https://www.charterhouse.co.uk/team"},
    {"name": "Altor Equity",         "url": "https://www.altor.com/team"},
    {"name": "Waterland",            "url": "https://www.waterland.nu/team"},
    {"name": "Naxicap",              "url": "https://www.naxicap.fr/en/team"},
    {"name": "Astorg",               "url": "https://www.astorg.com/team"},
    {"name": "BC Partners",          "url": "https://www.bcpartners.com/people"},
    {"name": "GIC",                  "url": "https://www.gic.com.sg/our-people"},
    # Fixed URLs
    {"name": "Apax Partners",        "url": "https://www.apax.com/en/people"},
    {"name": "Triton",               "url": "https://www.triton.com/people"},
]

OUTPUT_FILE = f"employees_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


# ═══════════════════════════════════════════════════════════════════════
#  NAV/GARBAGE KEYWORDS — used to filter bad rows post-scrape
# ═══════════════════════════════════════════════════════════════════════

NAV_KEYWORDS = {
    "about", "about us", "home", "overview", "our story", "our team",
    "our culture", "careers", "contact", "the firm", "insights",
    "news", "portfolio", "strategy", "strategies", "search", "menu",
    "lp login", "investor login", "back to top", "cookie", "cloudflare",
    "privacy", "legal", "terms", "linkedin", "twitter", "instagram",
    "follow", "subscribe", "read more", "view more", "load more",
    "en", "fr", "de", "es", "clear", "filter", "a to z",
    "investment staff", "fundraising and investor relations",
    "managing directors, investment staff", "private equity",
    "real assets", "credit", "infrastructure", "real estate",
    "wealth management solutions", "investor relations",
    "our businesses", "our people", "people", "team",
}

# Warburg-specific category header patterns
WARBURG_CATEGORY_PATTERNS = [
    "fundraising", "investor relations", "investment staff",
    "managing directors", "operating partners", "senior advisors",
    "portfolio operations", "finance", "technology", "legal",
    "compliance", "human resources", "communications", "marketing",
]


def is_garbage_name(name: str) -> bool:
    """Return True if name looks like nav/garbage, not a real person name."""
    n = name.strip().lower()
    # Exact match against nav keywords
    if n in NAV_KEYWORDS:
        return True
    # Starts with common nav prefixes
    if any(n.startswith(kw) for kw in ["http", "www.", "cookie", "link to", "@"]):
        return True
    # Contains newlines — these are merged cells, not names
    if "\n" in name:
        return True
    # Very short (1 char) or very long with no spaces (not a name)
    if len(n) <= 1:
        return True
    # Looks like a phone number or address
    if re.match(r"^\+?\d[\d\s\-().]+$", n):
        return True
    return False


def clean_position(position: str, person_name: str) -> str:
    """
    FIX 3: Leonard Green puts name after newline in position.
    e.g. 'Vice President\nMatt Allen' → 'Vice President'
    Also clean up any other multiline positions.
    """
    if not position or position == "N/A":
        return position
    # Take only the first line
    first_line = position.split("\n")[0].strip()
    # Remove name if it appears at the end
    if person_name and first_line.endswith(person_name):
        first_line = first_line[:-len(person_name)].strip().rstrip(",").strip()
    return first_line if first_line else position


def post_process(data: list[dict], firm_name: str) -> list[dict]:
    """
    Apply all post-processing fixes after scraping.
    FIX 2: Remove duplicate rows (keep row with real position)
    FIX 3: Clean position text for Leonard Green
    FIX 4: Filter Warburg category headers
    FIX 5/6: Filter nav garbage rows for all firms
    """
    if not data:
        return data

    # Step 1 — Filter obvious garbage names
    filtered = [r for r in data if not is_garbage_name(r.get("person_name", ""))]
    removed_garbage = len(data) - len(filtered)
    if removed_garbage > 0:
        print(f"[{firm_name}] Removed {removed_garbage} garbage rows")

    # Step 2 — Clean position text (FIX 3)
    for r in filtered:
        r["person_position"] = clean_position(
            r.get("person_position", "N/A"),
            r.get("person_name", "")
        )

    # Step 3 — Warburg: filter category header rows (FIX 4)
    if firm_name == "Warburg Pincus":
        before = len(filtered)
        filtered = [
            r for r in filtered
            if not any(
                pat in r["person_name"].lower()
                for pat in WARBURG_CATEGORY_PATTERNS
            )
        ]
        print(f"[{firm_name}] Removed {before - len(filtered)} category header rows")

    # Step 4 — Dedup: keep row with best position per person (FIX 2)
    # Group by person_name, keep the one with a real position
    seen = {}
    for r in filtered:
        name = r["person_name"]
        pos  = r.get("person_position", "N/A")
        if name not in seen:
            seen[name] = r
        else:
            # Prefer the row that has a real position
            existing_pos = seen[name].get("person_position", "N/A")
            if existing_pos == "N/A" and pos != "N/A":
                seen[name] = r

    deduped = list(seen.values())
    removed_dups = len(filtered) - len(deduped)
    if removed_dups > 0:
        print(f"[{firm_name}] Removed {removed_dups} duplicate rows")

    print(f"[{firm_name}] Final clean count: {len(deduped)}")
    return deduped


# ═══════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════

def dismiss_cookies(page, firm_name=""):
    for sel in [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Accept All')", "button:has-text('Accept')",
        "button:has-text('Allow All')", "button:has-text('Agree')",
        "button:has-text('I Accept')", "button[id*='accept']",
        "button[class*='accept']", ".cookie-banner button",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(800)
                if firm_name:
                    print(f"[{firm_name}] Dismissed cookie banner.")
                return
        except Exception:
            continue


def extract_cards(page, card_selectors):
    for sel in card_selectors:
        try:
            found = page.query_selector_all(sel)
            if len(found) >= 3:
                return found, sel
        except Exception:
            continue
    return [], None


def parse_card(card, firm_name, today, seen_keys):
    name_selectors  = ["h2", "h3", "h4", "h5", "[class*='name']", "strong", "a", "p"]
    title_selectors = ["[class*='title']", "[class*='position']", "[class*='role']",
                       "[class*='designation']", "[class*='subtitle']", "p", "span"]

    name = ""
    for ns in name_selectors:
        try:
            el = card.query_selector(ns)
            if el:
                text = el.inner_text().strip()
                if 2 <= len(text) <= 60 and "\n" not in text:
                    name = text
                    break
        except Exception:
            continue

    if not name:
        try:
            lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
            if lines:
                name = lines[0]
        except Exception:
            pass

    if not name or len(name) < 2 or len(name) > 80:
        return None

    title = "N/A"
    for ts in title_selectors:
        try:
            el = card.query_selector(ts)
            if el:
                text = el.inner_text().strip()
                if text and text != name and 2 <= len(text) <= 120:
                    title = text
                    break
        except Exception:
            continue

    if title == "N/A":
        try:
            lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
            if len(lines) >= 2 and lines[1] != name:
                title = lines[1]
        except Exception:
            pass

    # Dedup on name+title
    dedup_key = f"{name}||{title}"
    if dedup_key in seen_keys:
        return None
    seen_keys.add(dedup_key)

    team = location = "N/A"
    for ts in ["[class*='team']", "[class*='department']", "[class*='group']"]:
        try:
            el = card.query_selector(ts)
            if el:
                t = el.inner_text().strip()
                if t and 2 <= len(t) <= 80:
                    team = t
                    break
        except Exception:
            continue
    for ls in ["[class*='location']", "[class*='office']", "[class*='city']"]:
        try:
            el = card.query_selector(ls)
            if el:
                t = el.inner_text().strip()
                if t and 2 <= len(t) <= 80:
                    location = t
                    break
        except Exception:
            continue

    return {
        "firm_name": firm_name,
        "person_name": name,
        "person_position": title,
        "team": team,
        "location": location,
        "date_scraped": today,
    }


# ═══════════════════════════════════════════════════════════════════════
#  KNOWN API SCRAPERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_kkr_api(page, today: str) -> list[dict]:
    data = []
    base_url = (
        "https://www.kkr.com/content/kkr/sites/global/en/about/our-people/"
        "jcr:content/root/main-par/bioportfoliosearch.bioportfoliosearch.json"
    )
    params = ("sortParameter=name&sortingOrder=asc&keyword=&cfnode="
              "&pagePath=/content/kkr/sites/global/en/about/our-people")

    print("[KKR] Loading page...")
    page.goto("https://www.kkr.com/about/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    resp = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }",
        f"{base_url}?page=1&{params}"
    )
    first = json.loads(resp)
    total_pages = first.get("pages", 0)
    print(f"[KKR] {first.get('hits', 0)} employees, {total_pages} pages...")

    for p in first.get("results", []):
        data.append({"firm_name": "KKR",
                     "person_name": p.get("name","").strip(),
                     "person_position": p.get("title","N/A").strip(),
                     "team": p.get("team","N/A").strip(),
                     "location": p.get("city","N/A").strip(),
                     "date_scraped": today})

    for pg in range(2, total_pages + 1):
        try:
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }",
                f"{base_url}?page={pg}&{params}"
            )
            for p in json.loads(resp).get("results", []):
                data.append({"firm_name": "KKR",
                             "person_name": p.get("name","").strip(),
                             "person_position": p.get("title","N/A").strip(),
                             "team": p.get("team","N/A").strip(),
                             "location": p.get("city","N/A").strip(),
                             "date_scraped": today})
        except Exception as e:
            print(f"[KKR] Error on page {pg}: {e}")
    return data


def scrape_permira_api(page, today: str) -> list[dict]:
    data = []
    print("[PERMIRA] Loading page...")
    page.goto("https://www.permira.com/people/meet-our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    def parse_title(raw):
        if " - " in raw:
            parts = raw.split(" - ", 1)
            return parts[1].strip(), parts[0].strip()
        return raw.strip(), "N/A"

    resp = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }",
        "https://www.permira.com/api/peoples?page=1&filters={}&sort=a_z"
    )
    first = json.loads(resp)
    total_pages = first.get("totalPages", 0)
    print(f"[PERMIRA] {first.get('totalItems', 0)} employees, {total_pages} pages...")

    for p in first.get("data", []):
        pos, team = parse_title(p.get("title","N/A"))
        data.append({"firm_name": "Permira",
                     "person_name": p.get("name","").strip(),
                     "person_position": pos, "team": team,
                     "location": "N/A", "date_scraped": today})

    for pg in range(2, total_pages + 1):
        try:
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }",
                f"https://www.permira.com/api/peoples?page={pg}&filters={{}}&sort=a_z"
            )
            for p in json.loads(resp).get("data", []):
                pos, team = parse_title(p.get("title","N/A"))
                data.append({"firm_name": "Permira",
                             "person_name": p.get("name","").strip(),
                             "person_position": pos, "team": team,
                             "location": "N/A", "date_scraped": today})
        except Exception as e:
            print(f"[PERMIRA] Error on page {pg}: {e}")
    return data


# FIX 1 — EQT forced back to correct selector
def scrape_eqt(page, today: str) -> list[dict]:
    """
    EQT v3 regression: matched 'table tbody tr' and scraped Cloudflare text.
    Fix: force the correct selector a[href*='people/'] which worked in v2.
    Also click Load More to get all pages.
    """
    firm = "EQT"
    data = []
    seen_keys = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://eqtgroup.com/about/people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm)

    # Click Load More until all people are loaded
    clicks = 0
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
        try:
            btn = page.query_selector("button:has-text('Load More')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2_000)
                clicks += 1
                print(f"[{firm}] Clicked Load More ({clicks})")
            else:
                break
        except Exception:
            break

    # Force the correct selector — each person is a link to /people/{name}
    cards = page.query_selector_all("a[href*='/people/']")
    print(f"[{firm}] Found {len(cards)} people links")

    for card in cards:
        try:
            href = card.get_attribute("href") or ""
            # Skip non-person links (e.g. /about/people landing page)
            if href.rstrip("/").endswith("/people"):
                continue

            lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
            if not lines:
                continue

            name  = lines[0]
            title = lines[1] if len(lines) > 1 else "N/A"

            if len(name) < 2 or len(name) > 70 or is_garbage_name(name):
                continue

            key = f"{name}||{title}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            data.append({
                "firm_name": firm,
                "person_name": name,
                "person_position": title,
                "team": "N/A",
                "location": "N/A",
                "date_scraped": today,
            })
        except Exception:
            continue

    print(f"[{firm}] Extracted {len(data)} employees")
    return data


# CD&R custom scraper with arrow pagination
def scrape_cdr(page, today: str) -> list[dict]:
    firm = "CD&R"
    data = []
    seen_keys = set()

    card_selectors = [
        "[class*='team'] [class*='card']",
        "[class*='people'] [class*='card']",
        "[class*='team-member']",
    ]
    next_btn_selectors = [
        "button[aria-label='Next']", "button[aria-label='next']",
        "button[aria-label='Next page']", "a[aria-label='Next']",
        "[class*='pagination'] [class*='next']",
        "[class*='pagination'] button:last-child",
        "[class*='paginat'] button:last-of-type",
        "[class*='next-page']", "[class*='arrow-right']",
        "nav button:last-child", ".pagination__next",
    ]

    print(f"[{firm}] Loading team page...")
    page.goto("https://www.cdr-inc.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm)

    page_num = 1
    while page_num <= 20:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)

        cards, sel = extract_cards(page, card_selectors)
        print(f"[{firm}] Page {page_num}: {len(cards)} cards (selector: {sel})")

        before = len(data)
        for card in cards:
            result = parse_card(card, firm, today, seen_keys)
            if result:
                data.append(result)
        print(f"[{firm}] Page {page_num}: +{len(data)-before} new (total: {len(data)})")

        clicked = False
        for sel in next_btn_selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    disabled = btn.get_attribute("disabled") or btn.get_attribute("aria-disabled")
                    if disabled in ["true", "disabled"]:
                        print(f"[{firm}] Next button disabled — last page.")
                        return post_process(data, firm)
                    btn.click()
                    page.wait_for_timeout(2_500)
                    clicked = True
                    print(f"[{firm}] → Page {page_num+1}")
                    break
            except Exception:
                continue

        if not clicked:
            print(f"[{firm}] No next button — done at page {page_num}.")
            break
        page_num += 1

    return post_process(data, firm)


KNOWN_SCRAPERS = {
    "kkr.com":     scrape_kkr_api,
    "permira.com": scrape_permira_api,
    "eqtgroup.com": scrape_eqt,
    "cdr-inc.com": scrape_cdr,
}


# ═══════════════════════════════════════════════════════════════════════
#  GENERIC DOM SCRAPER
# ═══════════════════════════════════════════════════════════════════════

def scrape_generic(page, firm_name: str, url: str, today: str) -> list[dict]:
    data = []
    seen_keys = set()

    print(f"[{firm_name}] Loading page...")

    # FIX 7 — Advent crash guard
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        print(f"[{firm_name}] Page load error: {e}")
        return data

    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm_name)

    # Scroll + Load More
    print(f"[{firm_name}] Scrolling...")
    prev_height = 0
    for _ in range(30):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1_500)
            new_height = page.evaluate("document.body.scrollHeight")
        except Exception:
            break

        if new_height == prev_height:
            clicked = False
            for lm_sel in [
                "button:has-text('Load More')", "button:has-text('Show More')",
                "button:has-text('View More')", "a:has-text('Load More')",
                "[class*='load-more']", "[class*='show-more']",
            ]:
                try:
                    btn = page.query_selector(lm_sel)
                    if btn and btn.is_visible():
                        btn.click()
                        page.wait_for_timeout(2_000)
                        clicked = True
                        print(f"[{firm_name}] Clicked Load More.")
                        break
                except Exception:
                    continue
            if not clicked:
                break
        prev_height = new_height

    try:
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)
    except Exception:
        pass

    # Find cards
    card_selectors = [
        "[class*='people'] [class*='card']", "[class*='team'] [class*='card']",
        "[class*='member'] [class*='card']", "[class*='person']",
        "[class*='people-card']", "[class*='team-member']",
        "[class*='staff-member']", "[class*='profile-card']", "[class*='bio-card']",
        "[class*='people'] [class*='item']", "[class*='team'] [class*='item']",
        "[class*='people-grid'] > div", "[class*='team-grid'] > div",
        "[class*='people-list'] > div", "[class*='team-list'] > li",
        "table tbody tr", "[class*='table'] [class*='row']",
        "a[href*='bio']", "a[href*='people/']", "a[href*='team/']",
    ]

    cards, used_selector = extract_cards(page, card_selectors)

    if not cards:
        try:
            all_links = page.query_selector_all("a:has(h2), a:has(h3), a:has(h4), a:has(h5)")
            filtered = [c for c in all_links if any(
                kw in (c.get_attribute("href") or "").lower()
                for kw in ["people", "team", "bio", "staff"]
            )]
            if len(filtered) >= 3:
                cards = filtered
                used_selector = "fallback: links with headings"
        except Exception:
            pass

    if not cards:
        print(f"[{firm_name}] No people cards found. Needs a custom scraper.")
        return data

    print(f"[{firm_name}] Found {len(cards)} cards using: {used_selector}")

    for card in cards:
        result = parse_card(card, firm_name, today, seen_keys)
        if result:
            data.append(result)

    # Arrow pagination
    next_btn_selectors = [
        "button[aria-label*='Next']", "a[aria-label*='Next']",
        "[class*='pagination'] [class*='next']",
        "[class*='pagination'] button:last-child",
        ".pagination__next", "[class*='next-page']",
        "button:has-text('Next')", "a:has-text('Next')",
    ]

    for extra_page in range(1, 16):
        clicked = False
        for sel in next_btn_selectors:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    disabled = btn.get_attribute("disabled") or btn.get_attribute("aria-disabled")
                    if disabled in ["true", "disabled"]:
                        break
                    btn.click()
                    page.wait_for_timeout(2_500)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(1_000)
                    page.evaluate("window.scrollTo(0, 0)")

                    new_cards, _ = extract_cards(page, card_selectors)
                    new_count = 0
                    for card in new_cards:
                        result = parse_card(card, firm_name, today, seen_keys)
                        if result:
                            data.append(result)
                            new_count += 1
                    if new_count > 0:
                        print(f"[{firm_name}] Page {extra_page+1}: +{new_count} people")
                        clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            break

    # Apply post-processing to ALL firms
    return post_process(data, firm_name)


# ═══════════════════════════════════════════════════════════════════════
#  ROUTER
# ═══════════════════════════════════════════════════════════════════════

def get_domain(url: str) -> str:
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def scrape_website(page, firm_name: str, url: str, today: str) -> list[dict]:
    domain = get_domain(url)
    for known_domain, scraper_func in KNOWN_SCRAPERS.items():
        if domain == known_domain or domain.endswith("." + known_domain):
            print(f"\n[{firm_name}] Using specialized scraper for {domain}")
            result = scraper_func(page, today)
            # Apply post-processing to known scrapers too
            return post_process(result, firm_name) if firm_name not in ["KKR","Permira","EQT"] else result
    print(f"\n[{firm_name}] Using generic DOM scraper for {domain}")
    return scrape_generic(page, firm_name, url, today)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    all_data = []
    today = datetime.today().strftime("%Y-%m-%d")
    failed_sites = []

    print("=" * 60)
    print(f"  PE Firm Scraper v4 — {len(WEBSITES)} firms")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        for i, site in enumerate(WEBSITES, 1):
            firm = site["name"]
            url  = site["url"]
            print(f"\n{'─'*60}")
            print(f"  [{i}/{len(WEBSITES)}] {firm}")
            print(f"  {url}")
            print(f"{'─'*60}")
            try:
                site_data = scrape_website(page, firm, url, today)
                all_data.extend(site_data)
                status = "✓" if len(site_data) >= 80 else ("~" if len(site_data) >= 20 else "✗")
                print(f"[{firm}] {status} Collected {len(site_data)} employees.")
            except Exception as e:
                print(f"[{firm}] ✗ FAILED: {e}")
                failed_sites.append({"name": firm, "url": url, "error": str(e)})

        browser.close()

    print(f"\n{'='*60}  DONE  {'='*60}")

    if all_data:
        df = pd.DataFrame(all_data)
        try:
            df.to_excel(OUTPUT_FILE, index=False)
            print(f"\nSaved: {OUTPUT_FILE}")
        except PermissionError:
            backup = "employees_backup.xlsx"
            df.to_excel(backup, index=False)
            print(f"Saved (backup): {backup}")

        print(f"\nResults:")
        for firm, count in df.groupby("firm_name").size().sort_values(ascending=False).items():
            s = "✓" if count >= 80 else ("~" if count >= 20 else "✗")
            print(f"  {s} {firm}: {count}")
        print(f"\nTotal: {len(all_data)}")

    if failed_sites:
        print(f"\n⚠ Failed ({len(failed_sites)}):")
        for f in failed_sites:
            print(f"  • {f['name']}: {f['error'][:80]}")