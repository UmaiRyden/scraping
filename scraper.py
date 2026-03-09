"""
Private Equity Firm Employee Scraper
======================================
A data-driven scraper for collecting employee data from PE firm "Our People"
pages. To scrape a new site, simply add its name and URL to the WEBSITES list
below — no other code changes needed.

For sites with known JSON APIs (KKR, Permira), a fast API-based scraper is
used automatically. For all other sites, a generic DOM-based scraper detects
people cards/rows and extracts Name, Title, Team, and Location.

Output: A single Excel file with all employees from all firms.
"""

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import pandas as pd
from datetime import datetime
import json
import math
import re
from urllib.parse import urlparse


# ═══════════════════════════════════════════════════════════════════════
#  CONFIGURATION — Just add new websites here!
# ═══════════════════════════════════════════════════════════════════════

WEBSITES = [
    # ── JSON API scrapers (specialized) ──────────────────────────────
    {"name": "KKR",                 "url": "https://www.kkr.com/about/our-people"},
    {"name": "Permira",             "url": "https://www.permira.com/people/meet-our-people"},

    # ── HTML scrapers (generic DOM) ───────────────────────────────────
    {"name": "EQT",                 "url": "https://eqtgroup.com/about/people"},
    {"name": "Blackstone",          "url": "https://www.blackstone.com/people/"},
    {"name": "Thoma Bravo",         "url": "https://www.thomabravo.com/team"},
    {"name": "TPG",                 "url": "https://www.tpg.com/team"},
    {"name": "CVC Capital",         "url": "https://www.cvc.com/about/our-people"},
    {"name": "Hg",                  "url": "https://hg.co/team"},
    {"name": "Blue Owl",            "url": "https://www.blueowl.com/team"},
    {"name": "CD&R",                "url": "https://www.cdr-inc.com/team"},
    {"name": "Hellman & Friedman",  "url": "https://www.hf.com/team"},
    {"name": "Silver Lake",         "url": "https://www.silverlake.com/team"},
    {"name": "Apollo",              "url": "https://www.apollo.com/about-apollo/our-people"},
    {"name": "Warburg Pincus",      "url": "https://www.warburgpincus.com/team"},
    {"name": "General Atlantic",    "url": "https://www.generalatlantic.com/team"},
    {"name": "Bain Capital",        "url": "https://www.baincapital.com/team"},
    {"name": "Advent International","url": "https://www.adventinternational.com/team"},
    {"name": "Carlyle",             "url": "https://www.carlyle.com/about/team"},
    {"name": "Cinven",              "url": "https://www.cinven.com/team"},
    {"name": "Insight Partners",    "url": "https://www.insightpartners.com/team"},
    {"name": "Genstar Capital",     "url": "https://www.genstarcapital.com/team"},
    {"name": "Vista Equity",        "url": "https://www.vistaequitypartners.com/team"},
    {"name": "Leonard Green",       "url": "https://www.leonardgreen.com/team"},
    {"name": "Brookfield",          "url": "https://www.brookfield.com/team"},
    {"name": "Neuberger Berman",    "url": "https://www.nb.com/en/us/about-us/our-team"},
    {"name": "Bridgepoint",         "url": "https://www.bridgepoint.eu/team"},
    {"name": "Ares Management",     "url": "https://www.aresmgmt.com/team"},
    {"name": "Partners Group",      "url": "https://www.partnersgroup.com/about-us/our-team"},
    {"name": "Apax Partners",       "url": "https://www.apax.com/people/our-team"},
    {"name": "Ardian",              "url": "https://www.ardian.com/team"},
    {"name": "Nordic Capital",      "url": "https://www.nordiccapital.com/team"},
    {"name": "Triton",              "url": "https://www.triton-int.com/team"},
    {"name": "PAI Partners",        "url": "https://www.paipartners.com/team"},
    {"name": "CapVest",             "url": "https://www.capvest.com/team"},
    {"name": "Summit Partners",     "url": "https://www.summitpartners.com/team"},
    {"name": "GTCR",                "url": "https://www.gtcr.com/team"},
    {"name": "L Catterton",         "url": "https://www.lcatterton.com/team"},
    {"name": "Francisco Partners",  "url": "https://www.franciscopartners.com/team"},
    {"name": "Accel-KKR",           "url": "https://www.accel-kkr.com/team"},
    {"name": "Montagu PE",          "url": "https://www.montagu.com/team"},
    {"name": "IK Partners",         "url": "https://www.ikpartners.com/team"},
    {"name": "Eurazeo",             "url": "https://www.eurazeo.com/en/our-team"},
    {"name": "Charterhouse",        "url": "https://www.charterhouse.co.uk/team"},
    {"name": "Altor Equity",        "url": "https://www.altor.com/team"},
    {"name": "Waterland",           "url": "https://www.waterland.nu/team"},
    {"name": "Naxicap",             "url": "https://www.naxicap.fr/en/team"},
    {"name": "Astorg",              "url": "https://www.astorg.com/team"},
    {"name": "BC Partners",         "url": "https://www.bcpartners.com/people"},
    {"name": "GIC",                 "url": "https://www.gic.com.sg/our-people"},
]

MAX_EMPLOYEES = float('inf')  # No limit — scrape ALL employees from each firm
                              # Set to a number (e.g. 100) to cap per firm
OUTPUT_FILE = f"employees_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"


# ═══════════════════════════════════════════════════════════════════════
#  KNOWN API SCRAPERS (fast, reliable — used automatically)
# ═══════════════════════════════════════════════════════════════════════

def scrape_kkr_api(page, today: str) -> list[dict]:
    """Scrape KKR via their internal JSON API (10 results/page)."""
    data = []
    base_url = (
        "https://www.kkr.com/content/kkr/sites/global/en/about/our-people/"
        "jcr:content/root/main-par/bioportfoliosearch.bioportfoliosearch.json"
    )
    params = (
        "sortParameter=name&sortingOrder=asc&keyword=&cfnode="
        "&pagePath=/content/kkr/sites/global/en/about/our-people"
    )

    print("[KKR] Loading page to initialize session...")
    page.goto("https://www.kkr.com/about/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    # Fetch page 1
    url = f"{base_url}?page=1&{params}"
    resp = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }", url
    )
    first = json.loads(resp)
    total_pages = first.get("pages", 0)
    per_page = 10
    pages_needed = total_pages if MAX_EMPLOYEES == float('inf') else min(total_pages, math.ceil(MAX_EMPLOYEES / per_page))
    print(f"[KKR] {first.get('hits', 0)} employees, fetching {pages_needed} pages...")

    # Collect from page 1
    for p in first.get("results", []):
        if len(data) >= MAX_EMPLOYEES:
            break
        data.append({
            "firm_name": "KKR",
            "person_name": p.get("name", "N/A").strip(),
            "person_position": p.get("title", "N/A").strip(),
            "team": p.get("team", "N/A").strip(),
            "location": p.get("city", "N/A").strip(),
            "date_scraped": today,
        })

    # Remaining pages
    for pg in range(2, pages_needed + 1):
        if len(data) >= MAX_EMPLOYEES:
            break
        try:
            url = f"{base_url}?page={pg}&{params}"
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }", url
            )
            for p in json.loads(resp).get("results", []):
                if len(data) >= MAX_EMPLOYEES:
                    break
                data.append({
                    "firm_name": "KKR",
                    "person_name": p.get("name", "N/A").strip(),
                    "person_position": p.get("title", "N/A").strip(),
                    "team": p.get("team", "N/A").strip(),
                    "location": p.get("city", "N/A").strip(),
                    "date_scraped": today,
                })
        except Exception as e:
            print(f"[KKR] Error on page {pg}: {e}")

    return data


def scrape_permira_api(page, today: str) -> list[dict]:
    """Scrape Permira via their JSON API (16 results/page)."""
    data = []

    print("[PERMIRA] Loading page to initialize session...")
    page.goto("https://www.permira.com/people/meet-our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    def parse_title(raw: str) -> tuple[str, str]:
        """Split 'Team - Position' into (position, team)."""
        if " - " in raw:
            parts = raw.split(" - ", 1)
            return parts[1].strip(), parts[0].strip()
        return raw.strip(), "N/A"

    # Fetch page 1
    api_url = "https://www.permira.com/api/peoples?page=1&filters={}&sort=a_z"
    resp = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }", api_url
    )
    first = json.loads(resp)
    total_pages = first.get("totalPages", 0)
    per_page = first.get("itemsPerPage", 16)
    pages_needed = total_pages if MAX_EMPLOYEES == float('inf') else min(total_pages, math.ceil(MAX_EMPLOYEES / per_page))
    print(f"[PERMIRA] {first.get('totalItems', 0)} employees, fetching {pages_needed} pages...")

    for p in first.get("data", []):
        if len(data) >= MAX_EMPLOYEES:
            break
        position, team = parse_title(p.get("title", "N/A"))
        data.append({
            "firm_name": "Permira",
            "person_name": p.get("name", "N/A").strip(),
            "person_position": position,
            "team": team,
            "location": "N/A",
            "date_scraped": today,
        })

    for pg in range(2, pages_needed + 1):
        if len(data) >= MAX_EMPLOYEES:
            break
        try:
            url = f"https://www.permira.com/api/peoples?page={pg}&filters={{}}&sort=a_z"
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }", url
            )
            for p in json.loads(resp).get("data", []):
                if len(data) >= MAX_EMPLOYEES:
                    break
                position, team = parse_title(p.get("title", "N/A"))
                data.append({
                    "firm_name": "Permira",
                    "person_name": p.get("name", "N/A").strip(),
                    "person_position": position,
                    "team": team,
                    "location": "N/A",
                    "date_scraped": today,
                })
        except Exception as e:
            print(f"[PERMIRA] Error on page {pg}: {e}")

    return data


# Registry: map domain patterns to their specialized scrapers
KNOWN_SCRAPERS = {
    "kkr.com": scrape_kkr_api,
    "permira.com": scrape_permira_api,
}


# ═══════════════════════════════════════════════════════════════════════
#  GENERIC DOM SCRAPER (for any unknown PE firm website)
# ═══════════════════════════════════════════════════════════════════════

def scrape_generic(page, firm_name: str, url: str, today: str) -> list[dict]:
    """
    Generic scraper for any PE firm "Our People" page.

    Strategy:
      1. Load the page and dismiss any cookie banners.
      2. Scroll down to trigger lazy-loading of all people cards.
      3. Try multiple common selectors to find people cards.
      4. Extract name (from headings/links) and title (from paragraphs/spans).
      5. Attempt to find team and location if present.

    This works for most PE firm pages that display people as cards or list items.
    """
    data = []

    print(f"[{firm_name}] Loading page...")
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    # ── Dismiss common cookie banners ───────────────────────────────
    cookie_selectors = [
        "#onetrust-accept-btn-handler",
        "[id*='cookie'] button",
        "[class*='cookie'] button",
        "button[id*='accept']",
        "button[class*='accept']",
        ".cookie-banner button",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "#accept-cookies",
        ".consent-banner button",
    ]
    for sel in cookie_selectors:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                print(f"[{firm_name}] Dismissed cookie banner.")
                page.wait_for_timeout(1_000)
                break
        except Exception:
            continue

    # ── Scroll to load lazy content ─────────────────────────────────
    print(f"[{firm_name}] Scrolling to load all content...")
    prev_height = 0
    scroll_attempts = 0
    max_scrolls = 30

    while scroll_attempts < max_scrolls:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            # Try clicking "Load More" / "Show More" buttons
            load_more_clicked = False
            load_more_selectors = [
                "button:has-text('Load More')",
                "button:has-text('Show More')",
                "button:has-text('View More')",
                "a:has-text('Load More')",
                "a:has-text('Show More')",
                "a:has-text('View More')",
                "[class*='load-more']",
                "[class*='show-more']",
                "[class*='view-more']",
            ]
            for lm_sel in load_more_selectors:
                try:
                    lm_btn = page.query_selector(lm_sel)
                    if lm_btn and lm_btn.is_visible():
                        lm_btn.click()
                        page.wait_for_timeout(2_000)
                        load_more_clicked = True
                        print(f"[{firm_name}] Clicked 'Load More' button.")
                        break
                except Exception:
                    continue

            if not load_more_clicked:
                break  # No more content to load

        prev_height = new_height
        scroll_attempts += 1

    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    # ── Find people cards using common selector patterns ────────────
    # PE firm websites commonly use these patterns for people listings.
    # We try each until we find one that returns results.
    card_selectors = [
        # Common class-name patterns for people cards
        "[class*='people'] [class*='card']",
        "[class*='team'] [class*='card']",
        "[class*='member'] [class*='card']",
        "[class*='person']",
        "[class*='people-card']",
        "[class*='team-member']",
        "[class*='staff-member']",
        "[class*='profile-card']",
        "[class*='bio-card']",
        # Grid/list item patterns
        "[class*='people'] [class*='item']",
        "[class*='team'] [class*='item']",
        "[class*='people-grid'] > div",
        "[class*='team-grid'] > div",
        "[class*='people-list'] > div",
        "[class*='team-list'] > li",
        # Table row patterns
        "table tbody tr",
        "[class*='table'] [class*='row']",
        # Link-card patterns (each person is a link)
        "a[href*='people'] [class*='card']",
        "a[href*='team'] [class*='card']",
        "a[href*='bio']",
        "a[href*='people/']",
        "a[href*='team/']",
    ]

    cards = []
    used_selector = None

    for sel in card_selectors:
        try:
            found = page.query_selector_all(sel)
            # Filter: must have at least some text content, and we need
            # a reasonable number of results (at least 3 to be a people grid)
            if len(found) >= 3:
                cards = found
                used_selector = sel
                break
        except Exception:
            continue

    if not cards:
        print(f"[{firm_name}] WARNING: Could not detect people cards with standard selectors.")
        print(f"[{firm_name}] Trying fallback: looking for heading elements inside links...")

        # Fallback: find all links that contain headings (common pattern)
        try:
            cards = page.query_selector_all("a:has(h2), a:has(h3), a:has(h4), a:has(h5)")
            # Filter to only those whose href contains people-related paths
            filtered = []
            for card in cards:
                href = card.get_attribute("href") or ""
                if any(kw in href.lower() for kw in ["people", "team", "bio", "staff", "about"]):
                    filtered.append(card)
            if len(filtered) >= 3:
                cards = filtered
                used_selector = "fallback: links with headings"
        except Exception:
            pass

    if not cards:
        print(f"[{firm_name}] ERROR: No people cards found on this page.")
        print(f"[{firm_name}] This site may need a specialized scraper.")
        return data

    print(f"[{firm_name}] Found {len(cards)} people cards using selector: {used_selector}")

    # ── Extract data from each card ─────────────────────────────────
    # Name selectors (in priority order)
    name_selectors = ["h2", "h3", "h4", "h5", "a", "[class*='name']", "strong"]
    # Title/position selectors
    title_selectors = [
        "[class*='title']", "[class*='position']", "[class*='role']",
        "[class*='designation']", "[class*='subtitle']",
        "p", "span",
    ]
    # Team selectors
    team_selectors = [
        "[class*='team']", "[class*='department']", "[class*='group']",
        "[class*='division']", "[class*='sector']",
    ]
    # Location selectors
    location_selectors = [
        "[class*='location']", "[class*='office']", "[class*='city']",
        "[class*='region']",
    ]

    seen_names = set()  # Avoid duplicates

    for card in cards:
        if len(data) >= MAX_EMPLOYEES:
            break

        try:
            # Extract name
            name = ""
            for ns in name_selectors:
                try:
                    el = card.query_selector(ns)
                    if el:
                        text = el.inner_text().strip()
                        # Names should be 2-60 chars, not menu items or buttons
                        if 2 <= len(text) <= 60 and "\n" not in text:
                            name = text
                            break
                except Exception:
                    continue

            if not name:
                # Try the card's own text, taking the first line
                try:
                    full_text = card.inner_text().strip()
                    lines = [l.strip() for l in full_text.split("\n") if l.strip()]
                    if lines:
                        name = lines[0]
                except Exception:
                    continue

            if not name or name in seen_names:
                continue

            # Basic name validation: should contain at least one space
            # (first + last name) and no excessive special characters
            if len(name) < 3 or len(name) > 80:
                continue

            seen_names.add(name)

            # Extract title/position
            title = "N/A"
            for ts in title_selectors:
                try:
                    el = card.query_selector(ts)
                    if el:
                        text = el.inner_text().strip()
                        if text and text != name and 2 <= len(text) <= 100:
                            title = text
                            break
                except Exception:
                    continue

            if title == "N/A":
                # Try second line of card text
                try:
                    full_text = card.inner_text().strip()
                    lines = [l.strip() for l in full_text.split("\n") if l.strip()]
                    if len(lines) >= 2 and lines[1] != name:
                        title = lines[1]
                except Exception:
                    pass

            # Extract team
            team = "N/A"
            for tms in team_selectors:
                try:
                    el = card.query_selector(tms)
                    if el:
                        text = el.inner_text().strip()
                        if text and 2 <= len(text) <= 80:
                            team = text
                            break
                except Exception:
                    continue

            # Extract location
            location = "N/A"
            for ls in location_selectors:
                try:
                    el = card.query_selector(ls)
                    if el:
                        text = el.inner_text().strip()
                        if text and 2 <= len(text) <= 80:
                            location = text
                            break
                except Exception:
                    continue

            data.append({
                "firm_name": firm_name,
                "person_name": name,
                "person_position": title,
                "team": team,
                "location": location,
                "date_scraped": today,
            })

        except Exception as e:
            continue

    print(f"[{firm_name}] Extracted {len(data)} employees.")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  SCRAPER ROUTER — Picks the best scraper for each site
# ═══════════════════════════════════════════════════════════════════════

def get_domain(url: str) -> str:
    """Extract the base domain from a URL (e.g. 'kkr.com' from 'https://www.kkr.com/...')."""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    # Remove 'www.' prefix
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def scrape_website(page, firm_name: str, url: str, today: str) -> list[dict]:
    """
    Route to the best scraper for the given website.

    If we have a known API-based scraper for this domain, use it (faster/reliable).
    Otherwise, fall back to the generic DOM scraper.
    """
    domain = get_domain(url)

    # Check if we have a specialized scraper for this domain
    for known_domain, scraper_func in KNOWN_SCRAPERS.items():
        if known_domain in domain:
            print(f"\n[{firm_name}] Using specialized API scraper for {domain}")
            return scraper_func(page, today)

    # Otherwise use the generic scraper
    print(f"\n[{firm_name}] Using generic DOM scraper for {domain}")
    return scrape_generic(page, firm_name, url, today)


# ═══════════════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    all_data = []
    today = datetime.today().strftime("%Y-%m-%d")
    failed_sites = []

    print("=" * 60)
    print(f"  PE Firm Employee Scraper")
    print(f"  Sites: {len(WEBSITES)} | Max per firm: {MAX_EMPLOYEES}")
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
            url = site["url"]
            print(f"\n{'─' * 60}")
            print(f"  [{i}/{len(WEBSITES)}] {firm}")
            print(f"  {url}")
            print(f"{'─' * 60}")

            try:
                site_data = scrape_website(page, firm, url, today)
                all_data.extend(site_data)
                print(f"[{firm}] ✓ Collected {len(site_data)} employees.")
            except Exception as e:
                print(f"[{firm}] ✗ FAILED: {e}")
                failed_sites.append({"name": firm, "url": url, "error": str(e)})

        browser.close()

    # ── Save results ────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  SCRAPING COMPLETE")
    print(f"{'=' * 60}")

    if all_data:
        df = pd.DataFrame(all_data)
        try:
            df.to_excel(OUTPUT_FILE, index=False)
            print(f"\nSaved to: {OUTPUT_FILE}")
        except PermissionError:
            backup = "employees_backup.xlsx"
            print(f"\n[ERROR] '{OUTPUT_FILE}' is locked (open in Excel?).")
            print(f"  → Saving to '{backup}' instead...")
            df.to_excel(backup, index=False)
            print(f"  → Saved to: {backup}")

        # Summary
        summary = df.groupby("firm_name").size()
        print(f"\nRecords per firm:")
        for firm, count in summary.items():
            print(f"  {firm}: {count}")
        print(f"\nTotal records: {len(all_data)}")
    else:
        print("\n[WARNING] No data was scraped. Excel file was NOT created.")

    # Report failures
    if failed_sites:
        print(f"\n⚠ Failed sites ({len(failed_sites)}):")
        for f in failed_sites:
            print(f"  • {f['name']}: {f['error'][:80]}")