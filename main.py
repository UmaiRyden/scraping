"""
PE Firm Employee Scraper — main.py (combined)
=============================================
Single script that runs ALL working scrapers and saves one combined Excel.

Firms covered: 34 (custom + API + generic DOM)
Skipped (blocked/no public page): 13

Run:
    .venv/Scripts/python.exe main.py
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from playwright.sync_api import sync_playwright
import pandas as pd
import json
import re
from datetime import datetime
from urllib.parse import urlparse


# ═══════════════════════════════════════════════════════════════════════
#  GLOBALS
# ═══════════════════════════════════════════════════════════════════════

TODAY       = datetime.today().strftime("%Y-%m-%d")
OUTPUT_FILE = f"employees_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

NAV_KEYWORDS = {
    "about", "about us", "home", "overview", "our story", "our team",
    "our culture", "careers", "contact", "the firm", "insights",
    "news", "portfolio", "strategy", "strategies", "search", "menu",
    "lp login", "investor login", "back to top", "cookie", "cloudflare",
    "privacy", "legal", "terms", "linkedin", "twitter", "instagram",
    "follow", "subscribe", "read more", "view more", "load more",
    "en", "fr", "de", "es", "clear", "filter", "a to z",
    "people", "team", "the firm", "back to top",
    "investment staff", "fundraising and investor relations",
    "managing directors, investment staff", "private equity",
    "real assets", "credit", "infrastructure", "real estate",
    "wealth management solutions", "investor relations",
    "our businesses", "our people",
    "watch", "contact", "press", "events", "impact",
    "our portfolio", "our investments", "our approach", "our values",
    "leadership team", "senior leadership", "executive team",
    "meet our team", "meet the team", "meet our people",
    "view all", "see all", "show all", "all team members",
    "all people", "all staff",
}

# Words that are job titles — if a "name" consists entirely of these words
# it's a section label, not a person.
TITLE_WORDS = {
    "chairman", "emeritus", "managing", "director", "partner", "partners",
    "principal", "associate", "analyst", "vice", "president", "officer",
    "executive", "head", "senior", "junior", "chief", "co-head", "co",
    "ceo", "cfo", "coo", "cto", "cio", "cso", "evp", "svp", "vp",
    "manager", "advisor", "advisors", "consultant", "consultants",
    "investor", "investors", "investment", "operating", "portfolio",
    "general", "limited", "global", "regional", "group", "board",
    "trustee", "secretary", "treasurer", "controller", "delegate",
    "staff", "professionals", "team", "members", "leadership",
    "emeriti", "founders", "founder", "co-founder", "managing partner",
}

WARBURG_CATEGORY_PATTERNS = [
    "fundraising", "investor relations", "investment staff",
    "managing directors", "operating partners", "senior advisors",
    "portfolio operations", "finance", "technology", "legal",
    "compliance", "human resources", "communications", "marketing",
]


# ═══════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "").strip())


def make_row(firm, name, position="N/A", team="N/A", location="N/A") -> dict:
    return {
        "firm_name":       firm,
        "person_name":     clean_text(name),
        "person_position": clean_text(position),
        "team":            clean_text(team),
        "location":        clean_text(location),
        "date_scraped":    TODAY,
    }


def is_garbage_name(name: str) -> bool:
    n = name.strip().lower()
    if n in NAV_KEYWORDS:
        return True
    if any(n.startswith(kw) for kw in ["http", "www.", "cookie", "link to", "@"]):
        return True
    if "\n" in name:
        return True
    if len(n) <= 1:
        return True
    if re.match(r"^\+?\d[\d\s\-().]+$", n):
        return True
    if " " not in n and len(n) < 15:
        return True
    # Reject names that are entirely job-title words (e.g. "Chairman Emeritus",
    # "Managing Director", "Senior Partner") — these are section labels, not people.
    words = set(re.split(r"[\s\-/]+", n))
    if words and words.issubset(TITLE_WORDS):
        return True
    # Reject all-digit or mostly-symbol strings
    if re.match(r"^[\W\d]+$", n):
        return True
    return False


def clean_position(position: str, person_name: str) -> str:
    if not position or position == "N/A":
        return position
    lines = [l.strip() for l in position.split("\n") if l.strip()]
    if not lines:
        return position

    # Newline-split: first line IS the name → use second line
    if person_name and lines[0].strip() == person_name.strip():
        return lines[1] if len(lines) > 1 else "N/A"

    first_line = lines[0]

    # First line ends with name → strip it
    if person_name and first_line.endswith(person_name):
        first_line = first_line[: -len(person_name)].strip().rstrip(",").strip()

    # Comma-separated "Name, Role" pattern → strip the name prefix
    # e.g. position = "Ahmed Khairat, Director" → "Director"
    if person_name and first_line.lower().startswith(person_name.lower()):
        remainder = first_line[len(person_name):].strip().lstrip(",").strip()
        if remainder:
            first_line = remainder

    return first_line if first_line else position


def post_process(data: list, firm_name: str) -> list:
    if not data:
        return data

    filtered = [r for r in data if not is_garbage_name(r.get("person_name", ""))]
    removed_garbage = len(data) - len(filtered)
    if removed_garbage > 0:
        print(f"[{firm_name}] Removed {removed_garbage} garbage rows")

    for r in filtered:
        r["person_position"] = clean_position(
            r.get("person_position", "N/A"), r.get("person_name", "")
        )

    if firm_name == "Warburg Pincus":
        before = len(filtered)
        filtered = [
            r for r in filtered
            if not any(pat in r["person_name"].lower() for pat in WARBURG_CATEGORY_PATTERNS)
        ]
        print(f"[{firm_name}] Removed {before - len(filtered)} category header rows")
        for r in filtered:
            pos  = r.get("person_position", "N/A")
            name = r.get("person_name", "")
            if pos and pos != "N/A" and "\n" in pos:
                lines = [l.strip() for l in pos.split("\n") if l.strip()]
                for line in lines:
                    if line == name:
                        continue
                    if any(pat in line.lower() for pat in WARBURG_CATEGORY_PATTERNS):
                        continue
                    if len(line) > 2 and not line.isupper():
                        r["person_position"] = line
                        break

    if firm_name == "Hellman & Friedman":
        before = len(filtered)
        filtered = [
            r for r in filtered
            if not (r["person_name"].isupper() and r.get("person_position", "N/A") == "N/A")
        ]
        print(f"[Hellman & Friedman] Removed {before - len(filtered)} section header rows")

    # Universal validation: remove rows where name looks like a description or
    # a single long keyword phrase (no space = single word > 15 chars already caught,
    # but also catch: name that is all-caps section header AND has no position).
    before_universal = len(filtered)
    valid = []
    for r in filtered:
        name = r.get("person_name", "")
        pos  = r.get("person_position", "N/A")
        # All-caps AND no position → section header, not a person
        if name.isupper() and pos == "N/A":
            continue
        # Name that is a single word ≥15 chars with no position is likely garbage
        if " " not in name and len(name) >= 15 and pos == "N/A":
            continue
        valid.append(r)
    removed_universal = before_universal - len(valid)
    if removed_universal > 0:
        print(f"[{firm_name}] Removed {removed_universal} invalid name/position rows")
    filtered = valid

    seen = {}
    for r in filtered:
        name = r["person_name"]
        pos  = r.get("person_position", "N/A")
        if name not in seen:
            seen[name] = r
        else:
            if seen[name].get("person_position", "N/A") == "N/A" and pos != "N/A":
                seen[name] = r

    deduped      = list(seen.values())
    removed_dups = len(filtered) - len(deduped)
    if removed_dups > 0:
        print(f"[{firm_name}] Removed {removed_dups} duplicate rows")

    print(f"[{firm_name}] Final clean count: {len(deduped)}")
    return deduped


def dismiss_cookies(page, firm_name: str = ""):
    for sel in [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Accept All')", "button:has-text('Accept')",
        "button:has-text('Allow All')", "button:has-text('Agree')",
        "button:has-text('I Accept')", "button[id*='accept']",
        "button[class*='accept']", ".cookie-banner button",
        ".cc-allow",
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


def parse_card(card, firm_name, seen_keys) -> dict | None:
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

    return make_row(firm_name, name, title, team, location)


# ═══════════════════════════════════════════════════════════════════════
#  1. KKR  (JSON API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_kkr(page) -> list:
    firm = "KKR"
    data = []
    base_url = (
        "https://www.kkr.com/content/kkr/sites/global/en/about/our-people/"
        "jcr:content/root/main-par/bioportfoliosearch.bioportfoliosearch.json"
    )
    params = ("sortParameter=name&sortingOrder=asc&keyword=&cfnode="
              "&pagePath=/content/kkr/sites/global/en/about/our-people")

    print(f"[{firm}] Loading page...")
    page.goto("https://www.kkr.com/about/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    resp  = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }",
        f"{base_url}?page=1&{params}"
    )
    first = json.loads(resp)
    total_pages = first.get("pages", 0)
    print(f"[{firm}] {first.get('hits', 0)} employees, {total_pages} pages...")

    for p in first.get("results", []):
        data.append(make_row(firm, p.get("name", ""), p.get("title", "N/A"),
                             p.get("team", "N/A"), p.get("city", "N/A")))

    for pg in range(2, total_pages + 1):
        try:
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }",
                f"{base_url}?page={pg}&{params}"
            )
            for p in json.loads(resp).get("results", []):
                data.append(make_row(firm, p.get("name", ""), p.get("title", "N/A"),
                                     p.get("team", "N/A"), p.get("city", "N/A")))
        except Exception as e:
            print(f"[{firm}] Error on page {pg}: {e}")
    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  2. PERMIRA  (JSON API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_permira(page) -> list:
    firm = "Permira"
    data = []
    print(f"[{firm}] Loading page...")
    page.goto("https://www.permira.com/people/meet-our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    def parse_title(raw):
        if " - " in raw:
            parts = raw.split(" - ", 1)
            return parts[1].strip(), parts[0].strip()
        return raw.strip(), "N/A"

    resp  = page.evaluate(
        "async (url) => { const r = await fetch(url); return await r.text(); }",
        "https://www.permira.com/api/peoples?page=1&filters={}&sort=a_z"
    )
    first = json.loads(resp)
    total_pages = first.get("totalPages", 0)
    print(f"[{firm}] {first.get('totalItems', 0)} employees, {total_pages} pages...")

    for p in first.get("data", []):
        pos, team = parse_title(p.get("title", "N/A"))
        data.append(make_row(firm, p.get("name", ""), pos, team))

    for pg in range(2, total_pages + 1):
        try:
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }",
                f"https://www.permira.com/api/peoples?page={pg}&filters={{}}&sort=a_z"
            )
            for p in json.loads(resp).get("data", []):
                pos, team = parse_title(p.get("title", "N/A"))
                data.append(make_row(firm, p.get("name", ""), pos, team))
        except Exception as e:
            print(f"[{firm}] Error on page {pg}: {e}")
    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  3. EQT
# ═══════════════════════════════════════════════════════════════════════

def scrape_eqt(page) -> list:
    firm = "EQT"
    data = []
    seen_keys = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://eqtgroup.com/about/people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm)

    clicks = 0
    no_change_count = 0
    prev_card_count = len(page.query_selector_all("a[href*='/people/']"))
    print(f"[{firm}] Initial cards visible: {prev_card_count}")

    while no_change_count < 3:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)

        btn = None
        for btn_sel in [
            "button:has-text('Load more')", "button:has-text('Load More')",
            "button:text-matches('load\\\\s*more', 'i')",
            "[class*='load-more'] button", "button[class*='load-more']",
        ]:
            try:
                candidate = page.query_selector(btn_sel)
                if candidate and candidate.is_visible():
                    btn = candidate
                    break
            except Exception:
                continue

        if not btn:
            print(f"[{firm}] No Load more button — all loaded.")
            break

        try:
            btn.scroll_into_view_if_needed()
            page.wait_for_timeout(300)
            btn.click()
            clicks += 1
            for _ in range(10):
                page.wait_for_timeout(500)
                current_count = len(page.query_selector_all("a[href*='/people/']"))
                if current_count > prev_card_count:
                    break
            current_count = len(page.query_selector_all("a[href*='/people/']"))
            new_cards = current_count - prev_card_count
            no_change_count = 0 if new_cards > 0 else no_change_count + 1
            if clicks % 5 == 0:
                print(f"[{firm}] Click {clicks}: {current_count} cards loaded (+{new_cards})")
            prev_card_count = current_count
        except Exception as e:
            print(f"[{firm}] Load more click error: {e}")
            no_change_count += 1

    print(f"[{firm}] Finished loading: {clicks} clicks, {prev_card_count} total cards")

    for card in page.query_selector_all("a[href*='/people/']"):
        try:
            href = card.get_attribute("href") or ""
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
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] Extracted {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  4. CD&R  (arrow pagination)
# ═══════════════════════════════════════════════════════════════════════

def scrape_cdr(page) -> list:
    """
    CD&R team page (verified 2026-03-10):
      URL: https://www.cdr.com/team  (cdr-inc.com redirects here)
      Cards: div.card  (84 per page, 4 pages total, ~323 employees)
      Name:  .card-link text
      Title: line 2 of card inner_text (after filtering noise lines)
      Pagination: <select> with <option value="...?page=N"> for pages 1-4.
      Strategy: collect page URLs from <option> values, navigate each directly.
    """
    firm = "CD&R"
    data = []
    seen = set()

    def extract_cards_on_page():
        count = 0
        for card in page.query_selector_all(".card"):
            try:
                # Name: the .card-link anchor text
                link_el = card.query_selector(".card-link")
                name = clean_text(link_el.inner_text() if link_el else "")
                if not name or len(name) < 2:
                    continue

                # Profile URL
                href = link_el.get_attribute("href") if link_el else ""
                profile_url = href or "N/A"

                # Title: second non-noise line in card text (skip name, photos, etc.)
                title = "N/A"
                lines = [
                    l.strip() for l in card.inner_text().split("\n")
                    if l.strip() and l.strip() != name
                    and l.strip().lower() not in {"read more", "image", "photo"}
                    and len(l.strip()) > 1
                ]
                if lines:
                    title = lines[0]

                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title))
                count += 1
            except Exception:
                continue
        return count

    print(f"[{firm}] Loading team page...")
    page.goto("https://www.cdr.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm)

    # Collect all page URLs from the <select> dropdown
    page_urls = []
    try:
        options = page.query_selector_all("select option")
        for opt in options:
            val = opt.get_attribute("value") or ""
            if "page=" in val and val not in page_urls:
                page_urls.append(val)
        print(f"[{firm}] Found {len(page_urls)} pages in dropdown: {page_urls}")
    except Exception as e:
        print(f"[{firm}] Could not read page dropdown: {e}")

    # If dropdown has no URLs, fall back to ?page=1..4
    if not page_urls:
        page_urls = [f"https://www.cdr.com/team?page={n}" for n in range(1, 5)]
        print(f"[{firm}] Using fallback page URLs")

    # Scrape page 1 (already loaded)
    c = extract_cards_on_page()
    print(f"[{firm}] Page 1: {c} cards ({len(data)} total)")

    # Navigate to remaining pages
    for pg_url in page_urls[1:]:
        try:
            page.goto(pg_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
            # Wait for cards to appear
            try:
                page.wait_for_selector(".card", timeout=8_000)
            except Exception:
                pass
            c = extract_cards_on_page()
            print(f"[{firm}] {pg_url}: {c} cards ({len(data)} total)")
        except Exception as e:
            print(f"[{firm}] Error loading {pg_url}: {e}")

    print(f"[{firm}] ✓ {len(data)} employees across {len(page_urls)} pages")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  5. WARBURG PINCUS
# ═══════════════════════════════════════════════════════════════════════

def scrape_warburg(page) -> list:
    firm = "Warburg Pincus"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.warburgpincus.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page, firm)

    total = 500
    try:
        pag_el = page.query_selector("[class*='pagination']")
        if pag_el:
            m = re.search(r"OF\s+(\d+)", pag_el.inner_text().upper())
            if m:
                total = int(m.group(1))
    except Exception:
        pass
    print(f"[{firm}] Expected total: {total}")

    clicks = 0
    while True:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
        current = len(page.query_selector_all(".person"))
        if current >= total:
            break
        try:
            btn = page.query_selector("button:has-text('Load More'), a:has-text('Load More')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2_000)
                clicks += 1
                if clicks % 10 == 0:
                    print(f"[{firm}] Load More click {clicks}: {len(page.query_selector_all('.person'))} loaded")
            else:
                break
        except Exception:
            break

    page.evaluate("window.scrollTo(0, 0)")
    cards = page.query_selector_all(".person")
    print(f"[{firm}] Total cards loaded: {len(cards)}")

    for card in cards:
        try:
            title_div = card.query_selector(".person--title")
            if not title_div:
                continue
            h2    = title_div.query_selector("h2")
            small = title_div.query_selector("h2 small")
            if not h2:
                continue
            h2_text    = h2.inner_text().strip()
            small_text = small.inner_text().strip() if small else ""
            name       = h2_text.replace(small_text, "").strip()
            if not name or len(name) < 2:
                continue
            title    = small_text if small_text else "N/A"
            dept_el  = title_div.query_selector("p:first-of-type")
            dept     = dept_el.inner_text().strip() if dept_el else "N/A"
            all_p    = title_div.query_selector_all("p")
            location = all_p[-1].inner_text().strip() if all_p else "N/A"
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, dept, location))
        except Exception:
            continue

    print(f"[{firm}] Extracted {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  6. APOLLO  (leadership + ag-grid)
# ═══════════════════════════════════════════════════════════════════════

def scrape_apollo(page) -> list:
    firm = "Apollo"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.apollo.com/aboutus/leadership-and-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(5_000)
    dismiss_cookies(page)

    # Phase 1: Leadership (5 pages)
    def extract_leadership_cards():
        count = 0
        for card in page.query_selector_all(".pagignated-people-container__info"):
            try:
                name_el  = card.query_selector("a.text-link-blck-bold, div > a, a")
                title_el = card.query_selector(".pagignated-people-container__description")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")
                if not name:
                    lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                    name  = lines[0] if lines else ""
                    title = lines[1] if len(lines) > 1 else "N/A"
                if not name or len(name) < 2:
                    continue
                key = f"{name}||leadership"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, "Leadership"))
                count += 1
            except Exception:
                continue
        return count

    c = extract_leadership_cards()
    print(f"[{firm}] Leadership page 1: {c} profiles")

    for pg_num in range(2, 6):
        try:
            pg_link = page.query_selector(
                f'.people-list-pagination a.page-link[href="#page-{pg_num}"]'
            )
            if pg_link and pg_link.is_visible():
                pg_link.click()
                page.wait_for_timeout(2_000)
                c = extract_leadership_cards()
                print(f"[{firm}] Leadership page {pg_num}: {c} new profiles")
            else:
                break
        except Exception as e:
            print(f"[{firm}] Leadership page {pg_num} error: {e}")
            break

    leadership_count = len(data)
    print(f"[{firm}] Leadership done: {leadership_count} profiles")

    # Phase 2: ag-grid (550 rows via rowModel API)
    print(f"[{firm}] Scrolling to Our Team table...")
    for _ in range(12):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)

    try:
        page.wait_for_selector(".ag-row", timeout=15_000)
    except Exception:
        print(f"[{firm}] ag-grid not found — returning leadership only")
        return data

    all_rows = page.evaluate("""() => {
        try {
            const wrapper = document.querySelector('.ag-root-wrapper');
            if (!wrapper || !wrapper.__agComponent) return [];
            const rowModel = wrapper.__agComponent.context.getBean('rowModel');
            const count = rowModel.getRowCount();
            const result = [];
            for (let i = 0; i < count; i++) {
                const row = rowModel.getRow(i);
                if (row && row.data) result.push(row.data);
            }
            return result;
        } catch(e) { return []; }
    }""")

    print(f"[{firm}] ag-grid rowModel returned {len(all_rows)} rows")

    for row in all_rows:
        try:
            pname = row.get("preferredName", {})
            name  = clean_text(
                pname.get("text", "") if isinstance(pname, dict) else str(pname)
            )
            title = clean_text(row.get("businessTitle") or "N/A")
            dept  = clean_text(row.get("businessUnit")  or "N/A")
            loc   = clean_text(row.get("city")          or "N/A")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, dept, loc))
        except Exception:
            continue

    # Fallback: DOM pagination if rowModel returned nothing
    if len(data) == leadership_count:
        print(f"[{firm}] rowModel empty — falling back to DOM pagination")
        ag_page = 1

        def extract_dom_rows():
            count = 0
            for row in page.query_selector_all(".ag-row"):
                try:
                    cell_data = row.evaluate("""el => {
                        const obj = {};
                        el.querySelectorAll('[col-id]').forEach(c => {
                            obj[c.getAttribute('col-id')] = (c.innerText||'').trim();
                        });
                        return obj;
                    }""")
                    name  = clean_text(cell_data.get("preferredName") or "")
                    title = clean_text(cell_data.get("businessTitle") or "N/A")
                    dept  = clean_text(cell_data.get("businessUnit")  or "N/A")
                    loc   = clean_text(cell_data.get("city")          or "N/A")
                    if not name or len(name) < 2:
                        continue
                    key = f"{name}||{title}"
                    if key in seen:
                        continue
                    seen.add(key)
                    data.append(make_row(firm, name, title, dept, loc))
                    count += 1
                except Exception:
                    continue
            return count

        extract_dom_rows()
        while ag_page < 60:
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                next_li = page.query_selector(".cmp-employee-data__pagination li.next")
                if not next_li or "disabled" in (next_li.get_attribute("class") or ""):
                    break
                next_li.click()
                page.wait_for_timeout(2_500)
                c = extract_dom_rows()
                ag_page += 1
                if c == 0:
                    break
            except Exception as e:
                print(f"[{firm}] DOM fallback pagination error: {e}")
                break

    team_count = len(data) - leadership_count
    print(f"[{firm}] ✓ {len(data)} total ({leadership_count} leadership + {team_count} team)")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  7. PAI PARTNERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_pai_partners(page) -> list:
    firm = "PAI Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.paipartners.com/team/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    cards = page.query_selector_all(".individual-profile")
    print(f"[{firm}] Found {len(cards)} profile cards")

    for card in cards:
        try:
            name_el  = card.query_selector("h3, .info header h3")
            pos_els  = card.query_selector_all(".position")
            title_el = pos_els[0] if pos_els else None
            loc_el   = pos_els[1] if len(pos_els) > 1 else None
            name     = clean_text(name_el.inner_text()  if name_el  else "")
            title    = clean_text(title_el.inner_text() if title_el else "N/A")
            loc      = clean_text(loc_el.inner_text()   if loc_el   else "N/A")
            sub_el   = card.query_selector(".sub-info em, .sub-info")
            team     = clean_text(sub_el.inner_text()   if sub_el   else "N/A")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, team, loc))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  8. FRANCISCO PARTNERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_francisco_partners(page) -> list:
    firm = "Francisco Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.franciscopartners.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    links = page.query_selector_all("a[href*='/team/']")
    print(f"[{firm}] Found {len(links)} team links")

    for link in links:
        try:
            href = link.get_attribute("href") or ""
            if href.rstrip("/") == "/team":
                continue
            name_el  = link.query_selector(".base_link--wrap")
            title_el = link.query_selector("p.typo_paragraph, p[class*='typo']")
            if name_el:
                name  = clean_text(name_el.inner_text())
                title = clean_text(title_el.inner_text()) if title_el else "N/A"
            else:
                raw_lines = [l.strip() for l in link.inner_text().split("\n") if l.strip()]
                name  = raw_lines[0] if raw_lines else ""
                title = raw_lines[1] if len(raw_lines) > 1 else "N/A"
            if not name or len(name) < 2 or name.lower() in {"team", "our team"}:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  9. GTCR  (WordPress REST API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_gtcr(page) -> list:
    firm = "GTCR"
    data = []
    seen = set()
    base = "https://www.gtcr.com/wp-json/wp/v2/team/"

    print(f"[{firm}] Using WordPress REST API...")
    page.goto("https://www.gtcr.com/team/", wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(2_000)

    pg = 1
    while True:
        try:
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); "
                "const h = r.headers.get('X-WP-TotalPages'); "
                "return { body: await r.text(), totalPages: h }; }",
                f"{base}?per_page=100&page={pg}&_fields=id,title,acf,slug"
            )
            items = json.loads(resp["body"])
            total_pages = int(resp.get("totalPages") or 1)
            if not items:
                break
            for item in items:
                name  = clean_text(item.get("title", {}).get("rendered", ""))
                acf   = item.get("acf", {}) or {}
                title = clean_text(
                    acf.get("team_title") or acf.get("title") or
                    acf.get("job_title") or acf.get("position") or "N/A"
                )
                team = clean_text(acf.get("team") or acf.get("group") or "N/A")
                loc  = clean_text(acf.get("location") or acf.get("office") or "N/A")
                if not name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, team, loc))
            print(f"[{firm}] Page {pg}/{total_pages}: {len(data)} total")
            if pg >= total_pages:
                break
            pg += 1
        except Exception as e:
            print(f"[{firm}] API error page {pg}: {e}")
            break

    if not data:
        print(f"[{firm}] API empty, trying DOM...")
        for card in page.query_selector_all(".team-member"):
            try:
                name_el  = card.query_selector(".team-member-name")
                title_el = card.query_selector(".team-member-title")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")
                if not name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title))
            except Exception:
                continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  10. SUMMIT PARTNERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_summit_partners(page) -> list:
    firm = "Summit Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.summitpartners.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    for _ in range(10):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
    page.evaluate("window.scrollTo(0, 0)")

    cards = page.query_selector_all(".team-member-item")
    print(f"[{firm}] Found {len(cards)} cards")

    for card in cards:
        try:
            name_el = card.query_selector(".member-name")
            name = clean_text(name_el.inner_text() if name_el else "")
            if not name or len(name) < 2:
                continue

            title = "N/A"
            margin_div = card.query_selector("[class*='margin-top']")
            if margin_div:
                title_el = margin_div.query_selector("div:not([class])")
                if title_el:
                    title = clean_text(title_el.inner_text())

            team_val = loc = "N/A"
            if margin_div:
                hidden_divs = margin_div.query_selector_all("div.hide")
                texts = [clean_text(d.inner_text()) for d in hidden_divs if d.inner_text().strip()]
                if texts:
                    team_val = texts[0]
                for t in texts:
                    if any(city in t.lower() for city in [
                        "london", "boston", "menlo", "munich", "amsterdam",
                        "new york", "miami", "nashville"
                    ]):
                        loc = t
                        break

            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, team_val, loc))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  11. INSIGHT PARTNERS  (WordPress JSON API with route interception)
# ═══════════════════════════════════════════════════════════════════════

def scrape_insight_partners(page) -> list:
    firm = "Insight Partners"
    data = []
    seen = set()

    dept_name_map = {
        12:  "Investors",
        13:  "Onsite Experts",
        14:  "Firm Operations",
        15:  "Capital Partnerships",
        127: "Advisors",
    }
    people_by_dept: dict = {}

    def handle_route(route):
        resp = route.fetch()
        url  = route.request.url
        if "get-users" in url:
            m = re.search(r"department=(\d+)", url)
            if m:
                dept_id = int(m.group(1))
                if dept_id not in people_by_dept:
                    try:
                        body   = resp.text()
                        parsed = json.loads(body)
                        if isinstance(parsed, str):
                            parsed = json.loads(parsed)
                        rows = parsed.get("rows", []) if isinstance(parsed, dict) else (
                            parsed if isinstance(parsed, list) else []
                        )
                        people_by_dept[dept_id] = rows
                        label = dept_name_map.get(dept_id, f"Dept {dept_id}")
                        print(f"[{firm}] Captured dept '{label}' ({dept_id}): {len(rows)} people")
                    except Exception as e:
                        print(f"[{firm}] Parse error dept {dept_id}: {e}")
        route.fulfill(response=resp)

    print(f"[{firm}] Loading page...")
    page.route("**/wp-json/insight/v1/get-users**", handle_route)
    page.goto("https://www.insightpartners.com/team/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    dept_tabs = page.query_selector_all(".department")
    print(f"[{firm}] Found {len(dept_tabs)} department tabs — clicking each...")
    for tab in dept_tabs:
        try:
            tab.click()
            page.wait_for_timeout(3_000)
        except Exception:
            continue

    page.unroute("**/wp-json/insight/v1/get-users**")
    print(f"[{firm}] Captured {len(people_by_dept)} departments")

    for dept_id, rows in people_by_dept.items():
        dept_name = dept_name_map.get(dept_id, f"Dept {dept_id}")
        for person in rows:
            try:
                name  = clean_text(person.get("full_name", ""))
                title = clean_text(person.get("position", "") or "N/A")
                if not name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, dept_name))
            except Exception:
                continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  12. L CATTERTON  (JSON API)
# ═══════════════════════════════════════════════════════════════════════

def scrape_lcatterton(page) -> list:
    firm = "L Catterton"
    data = []
    seen = set()

    print(f"[{firm}] Loading page to fetch JSON API...")
    page.goto("https://www.lcatterton.com/People.html",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)

    try:
        resp   = page.evaluate(
            "async () => { const r = await fetch('/js/people.json'); return await r.text(); }"
        )
        people = json.loads(resp)
        print(f"[{firm}] JSON API returned {len(people)} records")
        for person in people:
            name  = clean_text(person.get("DisplayName", ""))
            title = clean_text(person.get("Title", "N/A")) or "N/A"
            team  = clean_text(person.get("Function", "N/A")) or "N/A"
            loc   = clean_text(person.get("Region", "N/A")) or "N/A"
            if not name or len(name) < 2:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, team, loc))
    except Exception as e:
        print(f"[{firm}] JSON API error: {e} — falling back to DOM")
        for person in page.query_selector_all(".person"):
            try:
                name = clean_text(person.inner_text())
                if not name or len(name) < 2:
                    continue
                key = f"{name}||N/A"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, "N/A"))
            except Exception:
                continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  13. BRIDGEPOINT
# ═══════════════════════════════════════════════════════════════════════

def scrape_bridgepoint(page) -> list:
    firm = "Bridgepoint"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.bridgepointgroup.com/about-us/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    page.evaluate("document.querySelectorAll('.tile-hidden').forEach(el => el.classList.remove('tile-hidden'))")
    page.wait_for_timeout(1_000)

    tiles = page.query_selector_all(".team-tile")
    print(f"[{firm}] Found {len(tiles)} .team-tile cards")

    for tile in tiles:
        try:
            name_el  = tile.query_selector(".grid-tile-text h4")
            title_el = tile.query_selector(".grid-tile-text p")
            name  = clean_text(name_el.inner_text()  if name_el  else "")
            title = clean_text(title_el.inner_text() if title_el else "N/A")
            if not name or len(name) < 2:
                continue
            if any(kw in name.lower() for kw in ["our people", "about", "contact", "jump to"]):
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  14. APAX PARTNERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_apax(page) -> list:
    firm     = "Apax Partners"
    base_url = "https://www.apax.com/people/our-team/"
    data     = []
    seen     = set()

    def extract_cards_from_page():
        count = 0
        for card in page.query_selector_all(".m-team-card"):
            try:
                name_el  = card.query_selector("h2.a-paragraph-type, .card-content h2")
                title_el = card.query_selector("p.a-paragraph-type,  .card-content p")
                link_el  = card.query_selector("a.card-container")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")
                team  = clean_text(
                    link_el.get_attribute("data-team") if link_el else "N/A"
                ) or "N/A"
                if not name or len(name) < 2:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, team))
                count += 1
            except Exception:
                continue
        return count

    def load_page(url):
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        try:
            page.wait_for_selector(".m-team-card", timeout=10_000)
        except Exception:
            pass
        page.wait_for_timeout(2_000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)

    def cards_on_page():
        try:
            return [el.inner_text().strip()
                    for el in page.query_selector_all(".m-team-card h2")]
        except Exception:
            return []

    print(f"[{firm}] Loading page 1...")
    load_page(base_url)
    dismiss_cookies(page)
    page.wait_for_timeout(1_500)

    c  = extract_cards_from_page()
    pg = 1
    print(f"[{firm}] Page {pg}: {c} cards ({len(data)} total)")

    prev_names = set(cards_on_page())

    for pg in range(2, 101):
        next_url = f"{base_url}?page={pg}"
        load_page(next_url)
        current_names = set(cards_on_page())
        if not current_names:
            print(f"[{firm}] No cards on page {pg} — finished")
            break
        if current_names == prev_names:
            print(f"[{firm}] Page {pg} identical to previous — finished")
            break
        prev_count = len(data)
        c = extract_cards_from_page()
        print(f"[{firm}] Page {pg}: {c} new cards ({len(data)} total)")
        if len(data) == prev_count:
            print(f"[{firm}] No new employees on page {pg} — finished")
            break
        prev_names = current_names

    print(f"[{firm}] ✓ {len(data)} employees across {pg} pages")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  15. NORDIC CAPITAL
# ═══════════════════════════════════════════════════════════════════════

def scrape_nordic_capital(page) -> list:
    firm = "Nordic Capital"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.nordiccapital.com/our-people/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    prev_count = 0
    for attempt in range(30):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
        current = len(page.query_selector_all(".team-member-card--component"))
        if current == prev_count and attempt > 3:
            break
        if current != prev_count:
            print(f"[{firm}] Scroll {attempt+1}: {current} cards loaded")
        prev_count = current

    page.evaluate("window.scrollTo(0, 0)")
    cards = page.query_selector_all(".team-member-card--component")
    print(f"[{firm}] Total cards: {len(cards)}")

    for card in cards:
        try:
            name_el = card.query_selector(".card-content p a, .card-content p.v--bold a")
            if not name_el:
                name_el = card.query_selector(".card-content p")
            title_el = card.query_selector(".card-content .u--pt--xs p, .card-content div p")
            name  = clean_text(name_el.inner_text()  if name_el  else "")
            title = clean_text(title_el.inner_text() if title_el else "N/A")
            if not name or len(name) < 2:
                continue
            if name.lower() in {"our people", "culture", "careers"}:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  16. BLUE OWL
# ═══════════════════════════════════════════════════════════════════════

def scrape_blue_owl(page) -> list:
    """
    Blue Owl team page (verified 2026-03-10):
    URL: https://www.blueowl.com/our-team

    Section 1 — Executive Officers (18 cards in 'section article'):
      Name:  div.text-24-30 > div
      Title: div.text-16-22 (first div after the SVG divider)
      Link:  a[href*="our-team"] inside article

    Section 2 — Employee Directory (Drupal, ~540 rows, 10/page):
      Row selector: div.views-row
      Grid: div[class*="grid-cols-4"] with 4 direct children:
        col 0 = Name, col 1 = Title, col 2 = Team/Business Unit, col 3 = Location
      Pagination: nav.pager  →  li.pager__item a[href]
        URLs are ?page=0 (first) … ?page=N (last)
        "Last page" link href reveals the final page index.
    """
    firm     = "Blue Owl"
    base_url = "https://www.blueowl.com/our-team"
    data     = []
    seen     = set()

    def load_page(url):
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3_000)
        # Scroll gently to trigger any lazy-loading
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        page.evaluate("window.scrollTo(0, 0)")

    def extract_executives():
        """Section 1: 18 Executive Officer cards in 'section article'."""
        count = 0
        for art in page.query_selector_all("section article"):
            try:
                name_el  = art.query_selector(".text-24-30 div")
                title_el = art.query_selector(".text-16-22")
                link_el  = art.query_selector("a[href*='our-team']")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")
                # Strip any trailing "Read more" that bleeds into title
                title = re.sub(r"\s*Read\s*more\s*$", "", title, flags=re.I).strip() or "N/A"
                link  = link_el.get_attribute("href") if link_el else "N/A"
                if not name or len(name) < 2 or " " not in name:
                    continue
                key = f"{name}||exec"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, "Executive Officers"))
                count += 1
            except Exception:
                continue
        return count

    def extract_directory_rows():
        """Section 2: directory rows from div.views-row on current page."""
        count = 0
        for row in page.query_selector_all(".views-row"):
            try:
                # Each row has a 4-column grid: Name | Title | Team | Location
                cols = row.evaluate("""el => {
                    const grid = el.querySelector('[class*="grid-cols"]');
                    if (!grid) return [];
                    return Array.from(grid.children).map(c => (c.innerText || '').trim());
                }""")
                if not cols or len(cols) < 1:
                    continue
                name  = clean_text(cols[0]) if len(cols) > 0 else ""
                title = clean_text(cols[1]) if len(cols) > 1 else "N/A"
                team  = clean_text(cols[2]) if len(cols) > 2 else "N/A"
                loc   = clean_text(cols[3]) if len(cols) > 3 else "N/A"
                if not name or len(name) < 2 or " " not in name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, team, loc))
                count += 1
            except Exception:
                continue
        return count

    def get_last_page_index():
        """Read last page index from the 'Last page' pager link (href ends in ?page=N)."""
        try:
            last_link = page.query_selector(
                "li.pager__item--last a, .pager__items a[title*='last'], "
                ".pager__items a[title*='Last']"
            )
            if last_link:
                href = last_link.get_attribute("href") or ""
                m = re.search(r"[?&]page=(\d+)", href)
                if m:
                    return int(m.group(1))
        except Exception:
            pass
        return None

    # Page 0 (= page 1) — also has the Executive Officers section
    print(f"[{firm}] Loading page 1 (index 0)...")
    load_page(base_url)
    dismiss_cookies(page)
    page.wait_for_timeout(1_000)

    # Section 1: executives (only on the first page load)
    exec_count = extract_executives()
    print(f"[{firm}] Executive Officers: {exec_count}")

    # Section 2: directory rows on page 0
    dir_count = extract_directory_rows()
    print(f"[{firm}] Directory page 0: {dir_count} rows ({len(data)} total)")

    # Find the last page index
    last_page = get_last_page_index()
    if last_page is None:
        # Fall back: count visible numbered pager links
        try:
            links = page.query_selector_all(".pager__items a")
            nums  = [int(re.search(r"[?&]page=(\d+)", (a.get_attribute("href") or "")).group(1))
                     for a in links
                     if re.search(r"[?&]page=(\d+)", (a.get_attribute("href") or ""))]
            last_page = max(nums) if nums else 4
        except Exception:
            last_page = 4
    print(f"[{firm}] Directory has {last_page + 1} pages (index 0 … {last_page})")

    # Paginate through pages 1 … last_page
    for pg_idx in range(1, last_page + 1):
        try:
            url = f"{base_url}?page={pg_idx}"
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
            try:
                page.wait_for_selector(".views-row", timeout=8_000)
            except Exception:
                pass
            c = extract_directory_rows()
            if pg_idx % 10 == 0 or pg_idx == last_page:
                print(f"[{firm}] Directory page {pg_idx}/{last_page}: {c} rows ({len(data)} total)")
            if c == 0:
                print(f"[{firm}] No rows on page {pg_idx} — stopping early")
                break
        except Exception as e:
            print(f"[{firm}] Error on page {pg_idx}: {e}")
            break

    print(f"[{firm}] ✓ {len(data)} employees total "
          f"({exec_count} executives + {len(data) - exec_count} directory)")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  17. CVC CAPITAL
# ═══════════════════════════════════════════════════════════════════════

def scrape_cvc(page) -> list:
    firm = "CVC Capital"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.cvc.com/about/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    def extract_page():
        count = 0
        for card in page.query_selector_all(".people__box, .people__info"):
            try:
                name_el     = card.query_selector(".people__name")
                lastname_el = card.query_selector(".people__last-name")
                title_el    = card.query_selector(".people__job")
                first = clean_text(name_el.inner_text()     if name_el     else "")
                last  = clean_text(lastname_el.inner_text() if lastname_el else "")
                title = clean_text(title_el.inner_text()    if title_el    else "N/A")
                if first and last and first.endswith(last):
                    name = first
                elif first and last:
                    name = f"{first} {last}".strip()
                else:
                    name = (first or last).strip()
                if not name:
                    link = card.query_selector("a[href*='/about/our-people/']")
                    if link:
                        text  = clean_text(link.inner_text())
                        parts = text.split("|")
                        name  = parts[0].strip() if parts else ""
                        title = parts[1].strip() if len(parts) > 1 else "N/A"
                if not name or len(name) < 2:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title))
                count += 1
            except Exception:
                continue
        return count

    total_pages = 63
    try:
        pag = page.query_selector("[class*='pagination']")
        if pag:
            nums = re.findall(r"\d+", pag.inner_text())
            if nums:
                total_pages = max(int(n) for n in nums if int(n) < 200)
    except Exception:
        pass

    print(f"[{firm}] Scraping {total_pages} pages...")

    for pg in range(1, total_pages + 1):
        if pg > 1:
            try:
                btn = page.query_selector(
                    f"[class*='pagination__link']:has-text('{pg}'), "
                    f".pagination__item:has-text('{pg}') a"
                )
                if btn:
                    btn.click()
                    page.wait_for_timeout(2_000)
                else:
                    nxt = page.query_selector(".pagination__btn--next, [class*='next']")
                    if nxt and nxt.is_visible():
                        nxt.click()
                        page.wait_for_timeout(2_000)
                    else:
                        break
            except Exception as e:
                print(f"[{firm}] Pagination error at page {pg}: {e}")
                break
        c = extract_page()
        if pg % 10 == 0:
            print(f"[{firm}] Page {pg}/{total_pages}: {len(data)} total")

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  18. HELLMAN & FRIEDMAN
# ═══════════════════════════════════════════════════════════════════════

def scrape_hf(page) -> list:
    firm = "Hellman & Friedman"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://hf.com/people/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    for sel in [".bio-grid a", ".bio-grid > *", "[class*='bio']",
                "[class*='person']", "[class*='team']", "a[href*='people/']"]:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) < 3:
                continue
            print(f"[{firm}] Found {len(cards)} cards with: {sel}")
            for card in cards:
                try:
                    lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                    if not lines:
                        continue
                    name  = lines[0]
                    title = lines[1] if len(lines) > 1 else "N/A"
                    if not name or len(name) < 2:
                        continue
                    if any(kw in name.lower() for kw in
                           ["previous", "next", "close", "about", "sort by", "filter", "load more"]):
                        continue
                    if name.isupper():
                        continue
                    if name.lower().startswith("sort by") or name.lower().startswith("filter by"):
                        continue
                    if " " not in name and len(name) < 20:
                        continue
                    key = f"{name}||{title}"
                    if key in seen:
                        continue
                    seen.add(key)
                    data.append(make_row(firm, name, title))
                except Exception:
                    continue
            if data:
                break
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  19. ADVENT INTERNATIONAL
# ═══════════════════════════════════════════════════════════════════════

def scrape_advent(page) -> list:
    firm     = "Advent International"
    data     = []
    seen     = set()
    base_url = "https://www.adventinternational.com/our-team/"

    print(f"[{firm}] Loading page 1...")
    page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    def extract_cards():
        count = 0
        for card in page.query_selector_all(".c-card-people"):
            try:
                name_el = card.query_selector(".c-card__heading")
                body_el = card.query_selector(".c-card__body")
                name    = clean_text(name_el.inner_text() if name_el else "")
                body    = clean_text(body_el.inner_text() if body_el else "")
                if not name or len(name) < 2:
                    continue
                title = loc = "N/A"
                if body:
                    parts = [p.strip() for p in body.split(",")]
                    title = parts[0] if parts else "N/A"
                    loc   = ", ".join(parts[1:]) if len(parts) > 1 else "N/A"
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, "N/A", loc))
                count += 1
            except Exception:
                continue
        return count

    total_pages = 17
    try:
        pag_text = page.query_selector("[class*='pagination']")
        if pag_text:
            match = re.search(r"of\s+(\d+)", pag_text.inner_text())
            if match:
                total = int(match.group(1))
                total_pages = (total + 23) // 24
    except Exception:
        pass

    print(f"[{firm}] ~{total_pages} pages to scrape")
    c = extract_cards()
    print(f"[{firm}] Page 1: {c} people")

    for pg in range(2, total_pages + 1):
        try:
            page.goto(f"{base_url}?sf_paged={pg}",
                      wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
            c = extract_cards()
            print(f"[{firm}] Page {pg}: {c} people (total: {len(data)})")
            if c == 0:
                break
        except Exception as e:
            print(f"[{firm}] Error on page {pg}: {e}")
            break

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  20. ALTOR EQUITY
# ═══════════════════════════════════════════════════════════════════════

def scrape_altor(page) -> list:
    firm = "Altor Equity"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://altor.com/our-team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    cards = page.query_selector_all(".g-content-card--coworker")
    print(f"[{firm}] Found {len(cards)} cards")

    for card in cards:
        try:
            name_el  = card.query_selector(".g-content-card__header")
            title_el = card.query_selector(".g-content-card__sub-header")
            name  = clean_text(name_el.inner_text()  if name_el  else "")
            title = clean_text(title_el.inner_text() if title_el else "N/A")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  21. PARTNERS GROUP
# ═══════════════════════════════════════════════════════════════════════

def scrape_partners_group(page) -> list:
    firm = "Partners Group"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.partnersgroup.com/about-us/our-team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    # Scroll to trigger lazy-load for all sections
    for _ in range(6):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    def add(name, title, team_label="N/A", location="N/A"):
        name  = clean_text(name)
        title = clean_text(title) if title else "N/A"
        if not name or len(name) < 2:
            return
        if is_garbage_name(name):
            return
        key = name.lower()
        if key in seen:
            return
        seen.add(key)
        data.append(make_row(firm, name, title, team_label, location))

    def extract_card_panel(panel_el, section_label):
        """Extract name + title from article/profile-card elements within a panel."""
        count = 0
        for article in panel_el.query_selector_all("article"):
            try:
                name_el  = article.query_selector(".profile-detail__name")
                # Title is in the first <p> inside the detail area (NOT the wrapper div
                # which has class *title* but contains both name + title text combined)
                title_el = article.query_selector(".profile-detail__content p, "
                                                  ".profile-detail p")
                name  = name_el.inner_text().strip()  if name_el  else ""
                title = title_el.inner_text().strip() if title_el else "N/A"
                if not name:
                    continue
                add(name, title, section_label)
                count += 1
            except Exception:
                continue
        return count

    # ── Section 1: Executive Team (Panel 0, visible on load) ─────────────────
    panels = page.query_selector_all(".tabs__panel")
    if panels:
        exec_panel = panels[0]
        c = extract_card_panel(exec_panel, "Executive Team")
        print(f"[{firm}] Executive Team: {c} people")
    else:
        # Fallback: extract from any visible articles
        for article in page.query_selector_all("article"):
            name_el  = article.query_selector(".profile-detail__name")
            title_el = article.query_selector(".profile-detail__content p")
            name  = name_el.inner_text().strip()  if name_el  else ""
            title = title_el.inner_text().strip() if title_el else "N/A"
            add(name, title, "Executive Team")

    # ── Section 2: Senior Management (table rows — already in DOM) ────────────
    # Rows alternate: tr.table__row (summary) and tr.table__content (expanded detail).
    # We only need table__row which has cells: Name(0), Title(1), Business Unit(2), Location(3).
    sm_count = 0
    for row in page.query_selector_all("tr.table__row"):
        try:
            cells = row.query_selector_all("td")
            if len(cells) < 2:
                continue
            name     = clean_text(cells[0].inner_text())
            title    = clean_text(cells[1].inner_text())
            biz_unit = clean_text(cells[2].inner_text()) if len(cells) > 2 else "N/A"
            location = clean_text(cells[3].inner_text()) if len(cells) > 3 else "N/A"
            # Skip icon/expand cells and empty rows
            if not name or name.lower() in {"name", "expand/close icon"}:
                continue
            add(name, title, biz_unit or "Senior Management", location)
            sm_count += 1
        except Exception:
            continue
    print(f"[{firm}] Senior Management: {sm_count} people")

    # ── Section 3: Board of Directors (Panel 2, hidden — click tab first) ─────
    board_tab = None
    for tab in page.query_selector_all(".tabs__link"):
        if "board" in tab.inner_text().lower():
            board_tab = tab
            break

    if board_tab:
        try:
            board_tab.evaluate("el => el.click()")
            page.wait_for_timeout(1_500)
            # Panel 2 is now visible
            if len(panels) >= 3:
                c = extract_card_panel(panels[2], "Board of Directors")
            else:
                # Re-query visible articles after tab click
                c = 0
                for article in page.query_selector_all("article"):
                    if article.evaluate("el => el.offsetParent !== null"):
                        name_el  = article.query_selector(".profile-detail__name")
                        title_el = article.query_selector(".profile-detail__content p")
                        name  = name_el.inner_text().strip()  if name_el  else ""
                        title = title_el.inner_text().strip() if title_el else "N/A"
                        add(name, title, "Board of Directors")
                        c += 1
            print(f"[{firm}] Board of Directors: {c} people")
        except Exception as e:
            print(f"[{firm}] Board tab error: {e}")
    else:
        print(f"[{firm}] Board of Directors tab not found")

    print(f"[{firm}] ✓ {len(data)} total employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  22. NAXICAP
# ═══════════════════════════════════════════════════════════════════════

def scrape_naxicap(page) -> list:
    firm = "Naxicap"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://naxicap.com/en/team/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    cards = page.query_selector_all(".item")
    print(f"[{firm}] Found {len(cards)} items")

    for card in cards:
        try:
            lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
            if not lines:
                continue
            name  = lines[0]
            title = lines[1] if len(lines) > 1 else "N/A"
            if not name or len(name) < 2:
                continue
            if name.upper() == name and len(name) < 4:
                continue
            if any(kw in name.lower() for kw in ["extranet", "contact", "about", "team", "news"]):
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  23. IK PARTNERS
# ═══════════════════════════════════════════════════════════════════════

def scrape_ik_partners(page) -> list:
    firm = "IK Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://ikpartners.com/our-people/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        try:
            btn = page.query_selector("button:has-text('Load More'), a:has-text('Load More')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(1_500)
        except Exception:
            pass
    page.evaluate("window.scrollTo(0, 0)")

    for sel in ["[class*='person']", "[class*='team-member']", "[class*='people']",
                "[class*='member']", "[class*='staff']", "article", "a[href*='our-people/']"]:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) < 3:
                continue
            print(f"[{firm}] Found {len(cards)} cards with: {sel}")
            for card in cards:
                try:
                    lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                    if not lines:
                        continue
                    name  = lines[0]
                    title = lines[1] if len(lines) > 1 else "N/A"
                    if not name or len(name) < 2:
                        continue
                    if name.lower() in {"our people", "contact us", "investor login"}:
                        continue
                    key = f"{name}||{title}"
                    if key in seen:
                        continue
                    seen.add(key)
                    data.append(make_row(firm, name, title))
                except Exception:
                    continue
            if data:
                break
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ═══════════════════════════════════════════════════════════════════════
#  24. GENERAL ATLANTIC  (custom — replaces generic)
# ═══════════════════════════════════════════════════════════════════════

def scrape_general_atlantic(page) -> list:
    firm = "General Atlantic"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.generalatlantic.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page, firm)

    # Scroll to trigger lazy-loading of all person cards
    for _ in range(12):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    # .team-item cards — exact class to avoid matching nested .team-item__* elements
    cards = page.query_selector_all(".team-item")
    print(f"[{firm}] Found {len(cards)} .team-item cards")

    for card in cards:
        try:
            name_el = card.query_selector("h2.team-item__title, h3.team-item__title, "
                                          ".team-item__title")
            role_el = card.query_selector(".team-item__role")
            cat_el  = card.query_selector("[class*='team-item__category'], "
                                          "[class*='team-item__tag'], "
                                          "[class*='team-item__label']")

            name = clean_text(name_el.inner_text()) if name_el else ""
            role = clean_text(role_el.inner_text()) if role_el else "N/A"
            cat  = clean_text(cat_el.inner_text())  if cat_el  else "N/A"

            if not name or len(name) < 2:
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, role, cat))
        except Exception:
            continue

    print(f"[{firm}] Extracted {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  25. THOMA BRAVO
# ═══════════════════════════════════════════════════════════════════════

def scrape_thoma_bravo(page) -> list:
    """
    Thoma Bravo team page (verified 2026-03-10):
      URL: https://www.thomabravo.com/team
      Three sections: LEADERSHIP, ALL STAFF, OPERATING TEAM — all pre-loaded.
      Cards: <a href="/team/SLUG"> each containing name + title as text lines.
      Section labels extracted per card by walking up to the nearest h2 ancestor.
      Lazy-load images require full scroll before extraction.
    """
    firm = "Thoma Bravo"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.thomabravo.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    # Scroll fully so lazy-loaded cards render
    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(600)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    # Use JS to collect all person links with their section label in one pass.
    # Walk up each <a href="/team/..."> to find the nearest h2 sibling/ancestor.
    raw = page.evaluate("""() => {
        const results = [];
        document.querySelectorAll('a[href*="/team/"]').forEach(a => {
            const href = a.getAttribute('href') || '';
            // Skip the /team landing page link
            if (href.replace(/\\/$/, '').endsWith('/team')) return;

            // Determine section label: walk up until we find a container
            // whose first h2 child matches a team section name
            let section = 'N/A';
            let el = a.parentElement;
            for (let i = 0; i < 12 && el; i++) {
                const h2 = el.querySelector(':scope > h2, :scope > div > h2,\
 :scope > div > div > h2');
                if (h2) {
                    const t = h2.innerText.trim().toUpperCase();
                    if (t.includes('LEADERSHIP') || t.includes('ALL STAFF') ||
                        t.includes('OPERATING')) {
                        section = h2.innerText.trim();
                        break;
                    }
                }
                el = el.parentElement;
            }

            const lines = a.innerText.trim().split('\\n')
                           .map(l => l.trim()).filter(Boolean);
            results.push({
                href,
                name:    lines[0] || '',
                title:   lines[1] || 'N/A',
                section,
            });
        });
        return results;
    }""")

    for item in raw:
        name    = clean_text(item.get("name", ""))
        title   = clean_text(item.get("title", "N/A"))
        section = clean_text(item.get("section", "N/A"))
        if not name or len(name) < 2 or " " not in name:
            continue
        key = f"{name}||{title}"
        if key in seen:
            continue
        seen.add(key)
        data.append(make_row(firm, name, title, section))

    # Print section breakdown
    from collections import Counter
    breakdown = Counter(r["team"] for r in data)
    for sec, cnt in breakdown.items():
        print(f"[{firm}] Section '{sec}': {cnt} people")

    print(f"[{firm}] ✓ {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  25. TPG
# ═══════════════════════════════════════════════════════════════════════

def scrape_tpg(page) -> list:
    """
    TPG team page (verified 2026-03-10):
      URL: https://www.tpg.com/about-us/who-we-are
      Cards: div.team-member  (15 initially visible)
      Name:     h3 inside card
      Platform: first <small> inside card
      Region:   second <small> or remaining lines
      Pagination: 'Load More' button — click repeatedly until gone
                  or card count stops increasing.
    """
    firm = "TPG"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.tpg.com/about-us/who-we-are",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    def extract_cards():
        count = 0
        for card in page.query_selector_all(".team-member"):
            try:
                # Use textContent (not innerText) — innerText returns "" for
                # elements that are off-screen / use CSS visibility tricks
                raw = card.evaluate("""el => {
                    const h3 = el.querySelector('h3');
                    const ps = Array.from(el.querySelectorAll('p'));
                    return {
                        name:     (h3   ? h3.textContent    : '').trim(),
                        platform: (ps[0] ? ps[0].textContent : '').trim(),
                        region:   (ps[1] ? ps[1].textContent : '').trim(),
                    };
                }""")
                name     = clean_text(raw.get("name", ""))
                platform = clean_text(raw.get("platform", "")) or "N/A"
                region   = clean_text(raw.get("region", ""))   or "N/A"
                if not name or len(name) < 2:
                    continue
                key = f"{name}||{platform}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, platform, platform, region))
                count += 1
            except Exception:
                continue
        return count

    c = extract_cards()
    print(f"[{firm}] Initial cards: {c}")

    # Click Load More until it disappears or no new cards appear
    no_change = 0
    click_num = 0
    while no_change < 3:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)

        btn = page.query_selector(
            "button:has-text('Load More'), button:has-text('load more')"
        )
        if not btn or not btn.is_visible():
            print(f"[{firm}] Load More button gone — all loaded.")
            break

        prev_count = len(data)
        try:
            btn.scroll_into_view_if_needed()
            page.wait_for_timeout(300)
            btn.click()
            click_num += 1
            # Wait for new cards to render (up to 6 seconds)
            for _ in range(12):
                page.wait_for_timeout(500)
                if len(page.query_selector_all(".team-member")) > prev_count:
                    break
            c = extract_cards()
            new = len(data) - prev_count
            if new > 0:
                no_change = 0
                if click_num % 5 == 0:
                    print(f"[{firm}] Click {click_num}: {len(data)} total (+{new})")
            else:
                no_change += 1
        except Exception as e:
            print(f"[{firm}] Load More error: {e}")
            no_change += 1

    print(f"[{firm}] ✓ {len(data)} employees after {click_num} Load More clicks")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  26. HG CAPITAL
# ═══════════════════════════════════════════════════════════════════════

def scrape_hg(page) -> list:
    """
    Hg Capital team page (verified 2026-03-10):
      URL: https://hgcapital.com/team
      Cards: div[class*="MemberInfo"] (styled-components, class hashes change)
      Name:  h3[class*="MemberName"] inside card
      Title: p[class*="LabelStyled"] inside card  (or first <p> inside card)
      Pagination: 'LOAD MORE' button — click until gone or no new cards.
                  Button uses styled-components class, not text (unreliable);
                  use :has-text('LOAD MORE') which is stable.
    """
    firm = "Hg Capital"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://hgcapital.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    def extract_cards():
        count = 0
        for card in page.query_selector_all("[class*='MemberInfo']"):
            try:
                name_el  = card.query_selector("h3")
                title_el = card.query_selector("p")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")
                if not name or len(name) < 2:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title))
                count += 1
            except Exception:
                continue
        return count

    c = extract_cards()
    print(f"[{firm}] Initial cards: {c}")

    no_change = 0
    click_num = 0
    while no_change < 3:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)

        btn = page.query_selector(
            "button:has-text('LOAD MORE'), button:has-text('Load More')"
        )
        if not btn or not btn.is_visible():
            print(f"[{firm}] Load More button gone — all loaded.")
            break

        prev_count = len(data)
        try:
            # Use JS click to bypass viewport/CSS visibility constraints
            btn.evaluate('el => el.click()')
            click_num += 1
            # Wait for new cards to appear — poll up to 8 seconds
            prev_dom_count = len(page.query_selector_all("[class*='MemberInfo']"))
            for _ in range(16):
                page.wait_for_timeout(500)
                if len(page.query_selector_all("[class*='MemberInfo']")) > prev_dom_count:
                    break
            page.wait_for_timeout(500)
            c = extract_cards()
            new = len(data) - prev_count
            if new > 0:
                no_change = 0
                if click_num % 5 == 0:
                    print(f"[{firm}] Click {click_num}: {len(data)} total (+{new})")
            else:
                no_change += 1
        except Exception as e:
            print(f"[{firm}] Load More error: {e}")
            no_change += 1

    print(f"[{firm}] ✓ {len(data)} employees after {click_num} Load More clicks")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  27. BAIN CAPITAL
# ═══════════════════════════════════════════════════════════════════════

def scrape_bain_capital(page) -> list:
    """
    Bain Capital people page (verified 2026-03-10):
      URL: https://www.baincapital.com/people
      Cards: div.col.staff  (40 per page, up to 21 pages)
      Card text lines: Name | Title | Business/Platform | Location
      Pagination: <a class="pagination-link" data-page="N"> (JS-driven AJAX)
                  Click by data-page attribute; wait for cards to reload.
    """
    firm = "Bain Capital"
    data = []
    seen = set()

    def extract_cards():
        count = 0
        for card in page.query_selector_all(".col.staff"):
            try:
                lines = [l.strip() for l in card.inner_text().split("\n")
                         if l.strip() and l.strip().lower() not in
                         {"read more", "view more", "image", "photo"}]
                if not lines:
                    continue
                name     = clean_text(lines[0])
                title    = clean_text(lines[1]) if len(lines) > 1 else "N/A"
                business = clean_text(lines[2]) if len(lines) > 2 else "N/A"
                location = clean_text(lines[3]) if len(lines) > 3 else "N/A"
                if not name or len(name) < 2 or " " not in name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, business, location))
                count += 1
            except Exception:
                continue
        return count

    def get_total_pages():
        try:
            links = page.query_selector_all("a.pagination-link[data-page]")
            nums  = [int(a.get_attribute("data-page"))
                     for a in links
                     if (a.get_attribute("data-page") or "").isdigit()]
            return max(nums) if nums else 1
        except Exception:
            return 1

    def wait_for_page_load(expected_pg: int):
        """Wait until the active pagination link matches expected_pg."""
        for _ in range(20):
            page.wait_for_timeout(500)
            try:
                active = page.query_selector("a.pagination-link.current, "
                                             "a.pagination-link.active")
                if active:
                    val = active.get_attribute("data-page") or ""
                    if val == str(expected_pg):
                        return
            except Exception:
                pass

    print(f"[{firm}] Loading page...")
    page.goto("https://www.baincapital.com/people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    # Scroll to let lazy images load
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(1_500)
    page.evaluate("window.scrollTo(0, 0)")

    c = extract_cards()
    print(f"[{firm}] Page 1: {c} cards")

    total_pages = get_total_pages()
    print(f"[{firm}] Total pages: {total_pages}")

    for pg in range(2, total_pages + 1):
        try:
            # Capture first card name before click to detect DOM change
            first_before = page.evaluate(
                "() => { const c = document.querySelector('.col.staff'); "
                "return c ? c.innerText.trim().split('\\n')[0] : ''; }"
            )

            # Try numbered link first; fall back to next-arrow
            clicked = page.evaluate(f"""() => {{
                const btn = document.querySelector("a.pagination-link[data-page='{pg}']");
                if (btn) {{ btn.click(); return true; }}
                const nxt = document.querySelector(
                    "a.pagination-link[aria-label*='Next'], a.pagination-next"
                );
                if (nxt) {{ nxt.click(); return true; }}
                return false;
            }}""")

            if not clicked:
                print(f"[{firm}] No link for page {pg} — stopping")
                break

            # Wait up to 10 s for first card to change
            for _ in range(20):
                page.wait_for_timeout(500)
                first_after = page.evaluate(
                    "() => { const c = document.querySelector('.col.staff'); "
                    "return c ? c.innerText.trim().split('\\n')[0] : ''; }"
                )
                if first_after != first_before:
                    break
            page.wait_for_timeout(300)

            c = extract_cards()
            if pg % 5 == 0 or pg == total_pages:
                print(f"[{firm}] Page {pg}/{total_pages}: {c} new ({len(data)} total)")
        except Exception as e:
            print(f"[{firm}] Error on page {pg}: {e}")
            break

    print(f"[{firm}] ✓ {len(data)} employees across {total_pages} pages")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  28. ARDIAN
# ═══════════════════════════════════════════════════════════════════════

def scrape_ardian(page) -> list:
    """
    Ardian team pages (verified 2026-03-10):
      5 section URLs — each ~24 people:
        /expertise/private-equity/buyout/team
        /expertise/private-equity/secondaries-primaries/team
        /expertise/private-equity/co-investment/team
        /expertise/real-assets/infrastructure/team
        /expertise/credit/private-credit/team
      Cards: li.team-list-item
      Name:    p.name a  (link text)
      Title:   p.employment
      Profile: p.name a[href]  (relative → prepend https://www.ardian.com)
      Image:   img[src]
    """
    firm = "Ardian"
    data = []
    seen = set()
    BASE = "https://www.ardian.com"

    SECTION_URLS = [
        ("Private Equity — Buyout",        f"{BASE}/expertise/private-equity/buyout/team"),
        ("Private Equity — Secondaries",   f"{BASE}/expertise/private-equity/secondaries-primaries/team"),
        ("Private Equity — Co-investment", f"{BASE}/expertise/private-equity/co-investment/team"),
        ("Real Assets — Infrastructure",   f"{BASE}/expertise/real-assets/infrastructure/team"),
        ("Credit — Private Credit",        f"{BASE}/expertise/credit/private-credit/team"),
    ]

    cookie_dismissed = False

    for section_label, url in SECTION_URLS:
        print(f"[{firm}] Loading {section_label}...")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(3_000)
            if not cookie_dismissed:
                dismiss_cookies(page, firm)
                cookie_dismissed = True
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1_500)
        except Exception as e:
            print(f"[{firm}] Load error on {url}: {e}")
            continue

        entries = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('.team-list-item').forEach(el => {
                const nameLink = el.querySelector('p.name a');
                const titleEl  = el.querySelector('p.employment');
                const imgEl    = el.querySelector('img');
                results.push({
                    name:  (nameLink ? nameLink.textContent : '').trim(),
                    title: (titleEl  ? titleEl.textContent  : '').trim(),
                    href:  nameLink ? (nameLink.getAttribute('href') || '') : '',
                    img:   imgEl ? (imgEl.getAttribute('src') || '') : '',
                });
            });
            return results;
        }""")

        count = 0
        for e in entries:
            name  = clean_text(e.get("name",  ""))
            title = clean_text(e.get("title", "")) or "N/A"
            href  = e.get("href", "")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{href}"
            if key in seen:
                continue
            seen.add(key)
            profile_url = BASE + href if href.startswith("/") else href
            row = make_row(firm, name, title, section_label)
            row["profile_url"] = profile_url
            row["image_url"]   = e.get("img", "")
            data.append(row)
            count += 1

        print(f"[{firm}]   {section_label}: {count} people ({len(data)} total)")

    print(f"[{firm}] ✓ {len(data)} employees across {len(SECTION_URLS)} sections")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  29. WATERLAND
# ═══════════════════════════════════════════════════════════════════════

def scrape_waterland(page) -> list:
    """
    Waterland PE team directory (verified 2026-03-10):
      URL:   https://www.waterlandpe.com/about-us/people/
      Cards: article.c-card  (20 per page, 11 pages)
      Name:     h3.c-card__title
      Title:    p.c-card__text
      Location: span.c-card__category
      Profile:  a[href] inside card
      Image:    img src
      Pagination: direct URL navigation to /about-us/people/page/N/
    """
    firm  = "Waterland"
    data  = []
    seen  = set()
    BASE  = "https://www.waterlandpe.com/about-us/people"

    def extract_cards():
        count = 0
        entries = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll('article.c-card').forEach(card => {
                const nameEl  = card.querySelector('h3.c-card__title');
                const titleEl = card.querySelector('p.c-card__text');
                const locEl   = card.querySelector('span.c-card__category');
                const linkEl  = card.querySelector('a[href]');
                const imgEl   = card.querySelector('img');
                results.push({
                    name:  (nameEl  ? nameEl.textContent  : '').trim(),
                    title: (titleEl ? titleEl.textContent : '').trim(),
                    loc:   (locEl   ? locEl.textContent   : '').trim(),
                    href:  linkEl ? (linkEl.getAttribute('href') || '') : '',
                    img:   imgEl  ? (imgEl.getAttribute('src')  || '') : '',
                });
            });
            return results;
        }""")
        for e in entries:
            name  = clean_text(e.get("name",  ""))
            title = clean_text(e.get("title", "")) or "N/A"
            loc   = clean_text(e.get("loc",   "")) or "N/A"
            href  = e.get("href", "")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{href}"
            if key in seen:
                continue
            seen.add(key)
            row = make_row(firm, name, title, "N/A", loc)
            row["profile_url"] = href
            row["image_url"]   = e.get("img", "")
            data.append(row)
            count += 1
        return count

    print(f"[{firm}] Loading page 1...")
    page.goto(f"{BASE}/", wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm)

    c = extract_cards()
    print(f"[{firm}] Page 1: {c} cards")

    # Determine last page number from pagination links
    last_page = page.evaluate("""() => {
        let max = 1;
        document.querySelectorAll('a.page-numbers').forEach(a => {
            const txt = a.textContent.trim();
            const n   = parseInt(txt);
            if (!isNaN(n) && n > max) max = n;
        });
        return max;
    }""")
    print(f"[{firm}] Total pages: {last_page}")

    for pg in range(2, last_page + 1):
        try:
            url = f"{BASE}/page/{pg}/#search_results"
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
            c = extract_cards()
            print(f"[{firm}] Page {pg}/{last_page}: {c} new ({len(data)} total)")
        except Exception as e:
            print(f"[{firm}] Error on page {pg}: {e}")
            break

    print(f"[{firm}] ✓ {len(data)} employees across {last_page} pages")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  29. GIC
# ═══════════════════════════════════════════════════════════════════════

def scrape_gic(page) -> list:
    """
    GIC organisational structure (verified 2026-03-10):
      URL: https://www.gic.com.sg/who-we-are/organisational-structure/
      Sections: Group Executive Committee | Investment Groups | Corporate HQ |
                Global Offices | Global Leaders (letter accordion)
      All 105 desktop profiles are already visible without clicking.
      Person: div.eachProfile  — name=first p, title=p.s
      Section: walk up DOM to parent eachRow h4
      Data appears twice (desktop + mobile copy); filter visible only (offsetParent).
    """
    firm = "GIC"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.gic.com.sg/who-we-are/organisational-structure/",
              wait_until="networkidle", timeout=90_000)
    page.wait_for_timeout(5_000)
    dismiss_cookies(page, firm)
    page.wait_for_timeout(2_000)

    # Scroll to trigger any lazy loading
    for _ in range(4):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_500)
    page.evaluate("window.scrollTo(0, 0)")

    # Extract all visible eachProfile elements with their section labels
    results = page.evaluate("""() => {
        const entries = [];
        document.querySelectorAll('.eachProfile').forEach(el => {
            // Only take the desktop (visible) copy
            if (!el.offsetParent) return;

            const nameEl  = el.querySelector('p:first-child');
            const titleEl = el.querySelector('p.s');

            // Walk up to find meaningful parent eachRow section label.
            // Skip single-letter rows (alphabetical accordion inside Global Leaders).
            let section = 'N/A';
            let cur = el.parentElement;
            for (let i = 0; i < 12 && cur; i++) {
                if (cur.classList && cur.classList.contains('eachRow')) {
                    const h4 = cur.querySelector(':scope > h4');
                    if (h4) {
                        const t = h4.textContent.trim();
                        if (t.length > 1) { section = t; break; }
                        // single letter — keep walking up to find parent section
                    }
                }
                cur = cur.parentElement;
            }

            entries.push({
                name:    (nameEl  ? nameEl.textContent  : '').trim(),
                title:   (titleEl ? titleEl.textContent : '').trim(),
                section,
            });
        });
        return entries;
    }""")

    print(f"[{firm}] Extracted {len(results)} visible profiles")

    for r in results:
        name  = clean_text(r.get("name",  ""))
        title = clean_text(r.get("title", "")) or "N/A"
        sec   = clean_text(r.get("section","")) or "N/A"
        if not name or len(name) < 2:
            continue
        key = f"{name}||{title}"
        if key in seen:
            continue
        seen.add(key)
        data.append(make_row(firm, name, title, sec))

    print(f"[{firm}] ✓ {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  30. EURAZEO
# ═══════════════════════════════════════════════════════════════════════

def scrape_eurazeo(page) -> list:
    """
    Eurazeo team directory (verified 2026-03-10):
      URL:    https://www.eurazeo.com/en/group/teams
      ~443 profiles, infinite scroll
      Cookie: button:has-text('Allow all')
      Links:  a[href*='/en/group/teams/profile/'] — each person has 2 links
              (image link + text link); text links contain p.name + h3.post
      Name:   p.name  inside link
      Title:  h3.post inside link
    """
    firm = "Eurazeo"
    data = []
    seen_hrefs = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.eurazeo.com/en/group/teams",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(5_000)

    # Cookie consent — "Allow all" button
    for sel in [
        "button:has-text('Allow all')",
        "button:has-text('Allow all cookies')",
        "button:has-text('Tout accepter')",
        "button:has-text('Accept all')",
        "#CybotCookiebotDialogBodyButtonAccept",
        "#onetrust-accept-btn-handler",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2_000)
                print(f"[{firm}] Cookie dismissed: {sel}")
                break
        except Exception:
            pass

    page.wait_for_timeout(2_000)

    def extract_profile_links():
        """Extract from text links that have p.name inside them."""
        count = 0
        entries = page.evaluate("""() => {
            const results = [];
            document.querySelectorAll("a[href*='/en/group/teams/profile/']").forEach(a => {
                const href  = a.getAttribute('href') || '';
                const nameEl  = a.querySelector('p.name');
                const titleEl = a.querySelector('h3.post');
                if (!nameEl) return;  // skip image-only links
                const img = a.querySelector('img');
                results.push({
                    href,
                    name:  nameEl.textContent.trim(),
                    title: titleEl ? titleEl.textContent.trim() : '',
                    img:   img ? (img.getAttribute('src') || '') : '',
                });
            });
            return results;
        }""")
        for e in entries:
            href  = e.get("href", "")
            if not href or href in seen_hrefs:
                continue
            name  = clean_text(e.get("name", ""))
            title = clean_text(e.get("title", "")) or "N/A"
            if not name or len(name) < 2:
                continue
            seen_hrefs.add(href)
            row = make_row(firm, name, title)
            row["profile_url"] = "https://www.eurazeo.com" + href if href.startswith("/") else href
            row["image_url"]   = e.get("img", "")
            data.append(row)
            count += 1
        return count

    c = extract_profile_links()
    print(f"[{firm}] Initial profiles: {c}")

    # Infinite scroll — keep scrolling until no new profiles appear (5 consecutive)
    no_change  = 0
    scroll_num = 0
    while no_change < 5:
        prev_count = len(data)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2_500)
        c = extract_profile_links()
        new = len(data) - prev_count
        scroll_num += 1
        if new > 0:
            no_change = 0
            if scroll_num % 5 == 0:
                print(f"[{firm}] Scroll {scroll_num}: {len(data)} total (+{new})")
        else:
            no_change += 1

    print(f"[{firm}] ✓ {len(data)} employees after {scroll_num} scrolls")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  29. BROOKFIELD
# ═══════════════════════════════════════════════════════════════════════

def scrape_brookfield(page) -> list:
    """
    Brookfield leadership page (verified 2026-03-10):
      URL: https://www.brookfield.com/about-us/leadership
      Section 1: Leadership carousel — cards are a.person-teaser (duplicate div inside; dedup by href)
                 Name:  h3 span
                 Title: div.positions div (first text-div inside .positions)
                 Arrow: button[aria-label='Next'] with class containing 'next' (not 'prev')
      Section 2: Employee directory table
                 Rows: tr.leadership-directory-row
                 Name:  td.views-field-title a.row-link (strip ' - view full profile')
                 Title: td.views-field-nothing
                 Biz:   td.views-field-field-business
                 Region:td.views-field-nothing-2
                 Pagination: a[rel='next'] → ?page=1, ?page=2, ...
    """
    firm = "Brookfield"
    data = []
    seen = set()
    BASE = "https://www.brookfield.com"

    print(f"[{firm}] Loading page...")
    page.goto("https://www.brookfield.com/about-us/leadership",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page, firm)

    # ── Section 1: Leadership carousel ───────────────────────────────────
    def extract_leadership():
        count = 0
        entries = page.evaluate("""() => {
            const results = [];
            const seen = new Set();
            document.querySelectorAll('a.person-teaser').forEach(a => {
                const href = a.getAttribute('href') || '';
                if (seen.has(href)) return;
                seen.add(href);
                const nameEl  = a.querySelector('h3 span, h3');
                const titleEl = a.querySelector('.positions div');
                const img     = a.querySelector('img');
                results.push({
                    name:  (nameEl  ? nameEl.textContent  : '').trim(),
                    title: (titleEl ? titleEl.textContent : '').trim(),
                    href,
                    img:   img ? (img.getAttribute('src') || '') : '',
                });
            });
            return results;
        }""")
        for e in entries:
            name  = clean_text(e.get("name", ""))
            title = clean_text(e.get("title", "")) or "N/A"
            href  = e.get("href", "")
            if not name or len(name) < 2:
                continue
            key = f"{name}||{href}"
            if key in seen:
                continue
            seen.add(key)
            row = make_row(firm, name, title, "Leadership")
            row["profile_url"] = BASE + href if href.startswith("/") else href
            row["image_url"]   = e.get("img", "")
            data.append(row)
            count += 1
        return count

    c = extract_leadership()
    print(f"[{firm}] Initial leadership cards: {c}")

    # Click right arrow to reveal more leadership cards
    no_change    = 0
    arrow_clicks = 0
    while no_change < 3:
        prev = len(data)
        # Find enabled Next button (class contains 'next' but NOT 'disabled')
        clicked = page.evaluate("""() => {
            const btns = Array.from(document.querySelectorAll('button[aria-label="Next"]'));
            const btn  = btns.find(b => !b.classList.contains('disabled') &&
                                        !b.hasAttribute('disabled'));
            if (btn) { btn.click(); return true; }
            return false;
        }""")
        if not clicked:
            print(f"[{firm}] No more Next arrow clicks available.")
            break
        arrow_clicks += 1
        page.wait_for_timeout(1_200)
        c = extract_leadership()
        new = len(data) - prev
        if new > 0:
            no_change = 0
        else:
            no_change += 1

    print(f"[{firm}] Leadership done: {len(data)} after {arrow_clicks} arrow clicks")

    # ── Section 2: Employee directory table ──────────────────────────────
    def extract_directory():
        count = 0
        rows = page.query_selector_all("tr.leadership-directory-row")
        for row in rows:
            info = row.evaluate("""el => {
                const nameTd  = el.querySelector('td.views-field-title');
                const titleTd = el.querySelector('td.views-field-nothing');
                const bizTd   = el.querySelector('td.views-field-field-business');
                const regTd   = el.querySelector('td.views-field-nothing-2');
                const nameA   = nameTd ? nameTd.querySelector('a.row-link') : null;
                return {
                    name:   (nameA   ? nameA.firstChild.textContent : (nameTd ? nameTd.textContent : '')).trim(),
                    title:  (titleTd ? titleTd.textContent : '').trim(),
                    biz:    (bizTd   ? bizTd.textContent   : '').trim(),
                    region: (regTd   ? regTd.textContent   : '').trim(),
                    href:   nameA ? (nameA.getAttribute('href') || '') : '',
                };
            }""")
            name  = clean_text(info.get("name", "").replace(" - view full profile", ""))
            title = clean_text(info.get("title", "")) or "N/A"
            biz   = clean_text(info.get("biz",   "")) or "N/A"
            region= clean_text(info.get("region","")) or "N/A"
            if not name or len(name) < 2 or " " not in name:
                continue
            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            row_data = make_row(firm, name, title, biz, region)
            href = info.get("href", "")
            row_data["profile_url"] = BASE + href if href.startswith("/") else href
            data.append(row_data)
            count += 1
        return count

    c = extract_directory()
    print(f"[{firm}] Directory page 0: {c} rows")

    # Paginate via ?page=N
    pg = 1
    while True:
        # Find rel='next' link
        next_href = page.evaluate(
            "() => { const a = document.querySelector('a[rel=\"next\"]'); "
            "return a ? a.getAttribute('href') : null; }"
        )
        if not next_href:
            break
        # Handle both absolute paths (/about-us/leadership?page=1) and bare (?page=1)
        if next_href.startswith("http"):
            next_url = next_href
        elif next_href.startswith("/"):
            next_url = BASE + next_href
        else:
            next_url = BASE + "/about-us/leadership" + next_href
        prev = len(data)
        try:
            page.goto(next_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2_000)
        except Exception as e:
            print(f"[{firm}] Pagination error page {pg}: {e}")
            break
        c = extract_directory()
        new = len(data) - prev
        print(f"[{firm}] Directory page {pg}: +{new} ({len(data)} total)")
        pg += 1
        if new == 0:
            break

    print(f"[{firm}] ✓ {len(data)} total employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  30. CARLYLE
# ═══════════════════════════════════════════════════════════════════════

def scrape_carlyle(page) -> list:
    """
    Carlyle — blocked by Cloudflare (returns 'Attention Required!' challenge page).
    Kept as a stub; returns empty list.
    """
    print("[Carlyle] Blocked by Cloudflare — skipping.")
    return []


# ═══════════════════════════════════════════════════════════════════════
#  31. TRITON
# ═══════════════════════════════════════════════════════════════════════

def scrape_triton(page) -> list:
    """
    Triton Partners team page (verified 2026-03-10):
      URL:   https://www.triton-partners.com/team/?team=triton&language=en
      Cards: a.person-thumb  (each card is a link, ~60 per filter)
             Name:  div.heading-thumb span
             href:  /team/kevin-albery/?team=triton&language=en
      Profile page title: [class*='job']  (e.g., 'Partner, Investor Relations Professional')
    """
    firm    = "Triton"
    data    = []
    seen    = set()
    BASE    = "https://www.triton-partners.com"
    TEAM_URL = "https://www.triton-partners.com/team/?team=triton&language=en"

    print(f"[{firm}] Loading team grid...")
    page.goto(TEAM_URL, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page, firm)

    # Scroll to ensure all cards are rendered
    for _ in range(4):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_200)
    page.evaluate("window.scrollTo(0, 0)")

    # Collect a.person-thumb cards — name from div.heading-thumb span, href from <a>
    card_entries = page.evaluate("""() => {
        const results = [];
        const seen    = new Set();
        document.querySelectorAll('a.person-thumb').forEach(a => {
            const href = a.getAttribute('href') || '';
            if (seen.has(href)) return;
            seen.add(href);
            const spanEl = a.querySelector('div.heading-thumb span');
            const imgEl  = a.querySelector('img');
            const name   = spanEl ? spanEl.textContent.trim() : '';
            results.push({
                href,
                name,
                img: imgEl ? (imgEl.getAttribute('src') || '') : '',
            });
        });
        return results;
    }""")

    print(f"[{firm}] Found {len(card_entries)} profile cards in grid")

    for i, entry in enumerate(card_entries):
        name = clean_text(entry.get("name", ""))
        href = entry.get("href", "")
        if not name or len(name) < 2:
            continue
        if name in seen:
            continue

        profile_url = href if href.startswith("http") else BASE + href

        try:
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(1_500)

            # Title confirmed at: [class*='job']
            title = page.evaluate("""() => {
                const el = document.querySelector('[class*="job"]');
                if (el) {
                    const t = el.textContent.trim();
                    if (t.length > 0 && t.length < 150) return t;
                }
                // Fallbacks
                for (const sel of ['[class*="title"]', '[class*="role"]', 'h4', '.subtitle']) {
                    const e2 = document.querySelector(sel);
                    if (e2) {
                        const t = e2.textContent.trim();
                        if (t.length > 0 && t.length < 120) return t;
                    }
                }
                return 'N/A';
            }""")
            title = clean_text(title) or "N/A"

            seen.add(name)
            row = make_row(firm, name, title)
            row["profile_url"] = profile_url
            row["image_url"]   = entry.get("img", "")
            data.append(row)

            if (i + 1) % 10 == 0:
                print(f"[{firm}] {i+1}/{len(card_entries)} profiles done ({len(data)} collected)")

        except Exception as e:
            print(f"[{firm}] Error on {profile_url}: {e}")
            seen.add(name)
            data.append(make_row(firm, name, "N/A"))

    print(f"[{firm}] ✓ {len(data)} employees")
    return post_process(data, firm)


# ═══════════════════════════════════════════════════════════════════════
#  GENERIC DOM SCRAPER  (for firms with no custom scraper)
# ═══════════════════════════════════════════════════════════════════════

def scrape_generic(page, firm_name: str, url: str) -> list:
    data      = []
    seen_keys = set()

    print(f"[{firm_name}] Loading page (generic)...")
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        print(f"[{firm_name}] Page load error: {e}")
        return data
    page.wait_for_timeout(3_000)
    dismiss_cookies(page, firm_name)

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

    card_selectors = [
        "[class*='people'] [class*='card']", "[class*='team'] [class*='card']",
        "[class*='member'] [class*='card']", "[class*='person']",
        "[class*='people-card']", "[class*='team-member']",
        "[class*='staff-member']", "[class*='profile-card']",
        "[class*='people'] [class*='item']", "[class*='team'] [class*='item']",
        "[class*='people-grid'] > div", "[class*='team-grid'] > div",
        "[class*='people-list'] > div", "[class*='team-list'] > li",
        "a[href*='bio']", "a[href*='people/']", "a[href*='team/']",
    ]
    cards, used_selector = extract_cards(page, card_selectors)

    if not cards:
        try:
            all_links = page.query_selector_all("a:has(h2), a:has(h3), a:has(h4)")
            filtered  = [c for c in all_links if any(
                kw in (c.get_attribute("href") or "").lower()
                for kw in ["people", "team", "bio", "staff"]
            )]
            if len(filtered) >= 3:
                cards = filtered
                used_selector = "fallback: links with headings"
        except Exception:
            pass

    if not cards:
        print(f"[{firm_name}] No people cards found.")
        return data

    print(f"[{firm_name}] Found {len(cards)} cards using: {used_selector}")
    for card in cards:
        result = parse_card(card, firm_name, seen_keys)
        if result:
            data.append(result)

    # Arrow pagination
    next_btn_selectors = [
        "button[aria-label*='Next']", "a[aria-label*='Next']",
        "[class*='pagination'] [class*='next']",
        "[class*='pagination'] button:last-child",
        ".pagination__next", "button:has-text('Next')", "a:has-text('Next')",
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
                        result = parse_card(card, firm_name, seen_keys)
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

    return post_process(data, firm_name)


# ═══════════════════════════════════════════════════════════════════════
#  SCRAPERS LIST — all firms with dedicated custom scrapers
# ═══════════════════════════════════════════════════════════════════════

CUSTOM_SCRAPERS = [
    # API-based
    ("KKR",                   scrape_kkr),
    ("Permira",               scrape_permira),
    ("EQT",                   scrape_eqt),
    ("CD&R",                  scrape_cdr),
    ("Warburg Pincus",        scrape_warburg),
    # Custom DOM
    ("Apollo",                scrape_apollo),
    ("PAI Partners",          scrape_pai_partners),
    ("Francisco Partners",    scrape_francisco_partners),
    ("GTCR",                  scrape_gtcr),
    ("Summit Partners",       scrape_summit_partners),
    ("Insight Partners",      scrape_insight_partners),
    ("L Catterton",           scrape_lcatterton),
    ("Bridgepoint",           scrape_bridgepoint),
    ("Apax Partners",         scrape_apax),
    ("Nordic Capital",        scrape_nordic_capital),
    ("Blue Owl",              scrape_blue_owl),
    ("CVC Capital",           scrape_cvc),
    ("Hellman & Friedman",    scrape_hf),
    ("Advent International",  scrape_advent),
    ("Altor Equity",          scrape_altor),
    ("Partners Group",        scrape_partners_group),
    ("Naxicap",               scrape_naxicap),
    ("IK Partners",           scrape_ik_partners),
    ("General Atlantic",       scrape_general_atlantic),
    ("Thoma Bravo",           scrape_thoma_bravo),
    ("TPG",                   scrape_tpg),
    ("Hg Capital",            scrape_hg),
    ("Bain Capital",          scrape_bain_capital),
    ("Ardian",                scrape_ardian),
    ("Waterland",             scrape_waterland),
    ("GIC",                   scrape_gic),
    ("Eurazeo",               scrape_eurazeo),
    ("Brookfield",            scrape_brookfield),
    ("Triton",                scrape_triton),
]

# Firms using the generic DOM scraper
GENERIC_SITES = [
    {"name": "Blackstone",    "url": "https://www.blackstone.com/people/"},
    {"name": "Silver Lake",   "url": "https://www.silverlake.com/team"},
    {"name": "Montagu PE",    "url": "https://www.montagu.com/team"},
    {"name": "BC Partners",   "url": "https://www.bcpartners.com/people"},
    {"name": "Charterhouse",  "url": "https://www.charterhouse.co.uk/team"},
    {"name": "Leonard Green", "url": "https://www.leonardgreen.com/team"},
    {"name": "Cinven",        "url": "https://www.cinven.com/team"},
    {"name": "Vista Equity",  "url": "https://www.vistaequitypartners.com/team"},
    {"name": "Astorg",        "url": "https://www.astorg.com/team"},
    {"name": "Accel-KKR",     "url": "https://www.accel-kkr.com/team"},
]

# Blocked firms — no public team page or fundamentally inaccessible
SKIPPED = [
    ("Carlyle",           "Cloudflare block — Attention Required challenge page"),
    ("Genstar Capital",   "Blank redirect — no public team page"),
    ("Neuberger Berman",  "Wrong region redirect, correct URL unknown"),
    ("CapVest",           "404 — no public team page"),
    ("Ares Management",   "Names only in <option> dropdowns, titles on 3027 individual pages — infeasible"),
]


# ═══════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════

def make_browser(p):
    b   = p.chromium.launch(headless=True, slow_mo=50)
    ctx = b.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    )
    return b, ctx, ctx.new_page()


if __name__ == "__main__":
    all_data = []
    results  = {}
    failed   = []

    total_firms = len(CUSTOM_SCRAPERS) + len(GENERIC_SITES)
    print("=" * 60)
    print(f"  PE Firm Scraper — {total_firms} firms")
    print("=" * 60)

    print(f"\n  Skipping {len(SKIPPED)} blocked/inaccessible firms:")
    for firm, reason in SKIPPED:
        print(f"  • {firm}: {reason}")
    print()

    RESTART_EVERY = 5   # restart browser every N firms to prevent JS heap OOM

    with sync_playwright() as p:
        browser, context, page = make_browser(p)
        firm_index = 0

        # ── Custom scrapers ──────────────────────────────────────────
        for firm, scraper_func in CUSTOM_SCRAPERS:
            firm_index += 1

            if firm_index > 1 and (firm_index - 1) % RESTART_EVERY == 0:
                print(f"\n  Restarting browser to free memory (after {firm_index-1} firms)...")
                try:
                    browser.close()
                except Exception:
                    pass
                browser, context, page = make_browser(p)

            print(f"\n{'─'*60}")
            print(f"  [{firm_index}/{total_firms}] {firm}")
            print(f"{'─'*60}")
            try:
                site_data = scraper_func(page)
                all_data.extend(site_data)
                results[firm] = len(site_data)
            except Exception as e:
                print(f"[{firm}] FAILED: {e}")
                failed.append((firm, str(e)))
                results[firm] = 0

        # ── Generic DOM scrapers ─────────────────────────────────────
        for site in GENERIC_SITES:
            firm = site["name"]
            url  = site["url"]
            firm_index += 1

            if firm_index > 1 and (firm_index - 1) % RESTART_EVERY == 0:
                print(f"\n  Restarting browser to free memory...")
                try:
                    browser.close()
                except Exception:
                    pass
                browser, context, page = make_browser(p)

            print(f"\n{'─'*60}")
            print(f"  [{firm_index}/{total_firms}] {firm}  (generic)")
            print(f"{'─'*60}")
            try:
                site_data = scrape_generic(page, firm, url)
                all_data.extend(site_data)
                results[firm] = len(site_data)
            except Exception as e:
                print(f"[{firm}] FAILED: {e}")
                failed.append((firm, str(e)))
                results[firm] = 0

        try:
            browser.close()
        except Exception:
            pass

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

        print("\nResults by firm:")
        for firm, count in sorted(results.items(), key=lambda x: -x[1]):
            s = "✓" if count >= 50 else ("~" if count >= 10 else "✗")
            print(f"  {s}  {firm}: {count}")
        print(f"\nTotal employees: {len(all_data)}")

    if failed:
        print(f"\n  Failed ({len(failed)}):")
        for firm, err in failed:
            print(f"  • {firm}: {err[:100]}")
