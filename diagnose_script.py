"""
Custom Scrapers — Broken & Missing Firms
=========================================
Written based on diagnosis_report.txt findings.

STRATEGY PER FIRM:
  Apollo           → Click through 55 pages of .pagignated-people-container__info
  PAI Partners     → .individual-profile cards with .name + .position
  Francisco Partners → a[href*='/team/'] links (name+title in link text)
  GTCR             → WordPress JSON API /wp-json/wp/v2/team/ (paginated)
  Summit Partners  → .team-member-item cards with .member-name
  Insight Partners → WordPress JSON API /wp-json/insight/v1/get-users per department
  Advent Intl      → .c-card-people cards, paginated via ?sf_paged=N (386 total)
  Altor Equity     → .g-content-card--coworker cards (correct URL: /our-team)
  Partners Group   → article cards, click tabs (Executive/Senior/Board)
  CVC Capital      → a[href*='/about/our-people/'] links, 63 pages
  Naxicap          → .item cards with .title (correct URL: naxicap.com/en/team/)
  Hellman & Friedman → correct URL /people/ with .bio-grid
  IK Partners      → correct URL /our-people/
  L Catterton      → correct URL /People.html
  Bridgepoint      → correct URL /about-us/our-people
  Apax Partners    → correct URL /people/our-team/
  Ares Management  → correct URL /about-ares-management-corporation/our-team
  Nordic Capital   → correct URL /our-people/
  Blue Owl         → correct URL /our-team
  
BLOCKED / NO PUBLIC PAGE (skipped with note):
  Carlyle          → Cloudflare blocking — needs residential proxy or manual
  Bain Capital     → Blank page, no CSS — likely SPA that needs different approach
  TPG              → 404 blank — no public team page found
  Hg               → 404 — no public team page found
  Triton           → triton.com is wrong company (ATM vendor) — need correct PE firm URL
  GIC              → 404 — no public team page
  Genstar Capital  → Blank redirect — no public team page
  Waterland        → Redirects to portfolio company page — no public team page
  Neuberger Berman → Wrong region URL, correct URL unknown
  Eurazeo          → 404 — correct URL unknown
  CapVest          → 404 — no public team page
"""

from playwright.sync_api import sync_playwright
import pandas as pd
import json
import re
from datetime import datetime
from urllib.parse import urlparse

OUTPUT_FILE = f"broken_firms_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
TODAY = datetime.today().strftime("%Y-%m-%d")


# ── Helpers ──────────────────────────────────────────────────────────

def dismiss_cookies(page):
    for sel in [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Accept All')", "button:has-text('Accept')",
        "button:has-text('Allow All')", "button:has-text('I Accept')",
        "button[id*='accept']", "button[class*='accept']",
        ".cc-allow", "button:has-text('Agree')",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(800)
                return
        except Exception:
            continue


def clean_text(t):
    return re.sub(r'\s+', ' ', (t or "").strip())


def make_row(firm, name, position="N/A", team="N/A", location="N/A"):
    return {
        "firm_name": firm,
        "person_name": clean_text(name),
        "person_position": clean_text(position),
        "team": clean_text(team),
        "location": clean_text(location),
        "date_scraped": TODAY,
    }


# ── 1. APOLLO ────────────────────────────────────────────────────────
def scrape_apollo(page):
    """
    550 employees across 55 pages.
    Each page shows 10 rows in an ag-grid table.
    Selector: .pagignated-people-container__info (12 per page visible)
    Pagination: numbered buttons 1-55 visible.
    Strategy: click each page number, extract cards.
    """
    firm = "Apollo"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.apollo.com/aboutus/leadership-and-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)
    page.wait_for_timeout(1_000)

    def extract_page():
        cards = page.query_selector_all(".pagignated-people-container__info")
        count = 0
        for card in cards:
            try:
                name_el = card.query_selector(".pagignated-people-container__details h2, "
                                              ".pagignated-people-container__details h3, "
                                              ".pagignated-people-container__details strong, "
                                              ".data-cell-name-col-wrapper")
                title_el = card.query_selector(".pagignated-people-container__description, "
                                               ".ag-employee-data-cell")
                name = clean_text(name_el.inner_text() if name_el else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")

                if not name:
                    # Try reading all text from card
                    lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                    if lines:
                        name = lines[0]
                    if len(lines) > 1:
                        title = lines[1]

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

    # Also try ag-grid row extraction
    def extract_ag_grid():
        count = 0
        rows = page.query_selector_all(".ag-row")
        for row in rows:
            try:
                name_el = row.query_selector(".ag-employee-name-data-cell, .data-cell-name-col-wrapper")
                cells = row.query_selector_all(".ag-employee-data-cell")
                name = clean_text(name_el.inner_text() if name_el else "")
                title = clean_text(cells[0].inner_text() if cells else "N/A")
                dept = clean_text(cells[1].inner_text() if len(cells) > 1 else "N/A")
                loc = clean_text(cells[2].inner_text() if len(cells) > 2 else "N/A")
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

    # Get total pages from pagination
    total_pages = 55  # known from diagnosis
    try:
        pag = page.query_selector(".pagination, [class*='pagination']")
        if pag:
            text = pag.inner_text()
            nums = re.findall(r'\d+', text)
            if nums:
                total_pages = max(int(n) for n in nums if int(n) < 200)
    except Exception:
        pass

    print(f"[{firm}] Scraping {total_pages} pages...")

    for pg in range(1, total_pages + 1):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)

        c1 = extract_page()
        c2 = extract_ag_grid()
        new = c1 + c2

        if pg % 5 == 0:
            print(f"[{firm}] Page {pg}/{total_pages}: {len(data)} total so far")

        if pg < total_pages:
            # Click next page button
            try:
                # Try clicking numbered page button
                next_sel = (f"[class*='pagination'] button:has-text('{pg+1}'), "
                           f"[class*='pagination'] a:has-text('{pg+1}')")
                btn = page.query_selector(next_sel)
                if btn and btn.is_visible():
                    btn.click()
                    page.wait_for_timeout(2_000)
                else:
                    # Try Next arrow
                    next_btn = page.query_selector("button[aria-label*='Next'], a[aria-label*='Next'], "
                                                   "[class*='next']:not([disabled])")
                    if next_btn and next_btn.is_visible():
                        next_btn.click()
                        page.wait_for_timeout(2_000)
                    else:
                        print(f"[{firm}] No next button at page {pg}, stopping")
                        break
            except Exception as e:
                print(f"[{firm}] Pagination error at page {pg}: {e}")
                break

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 2. PAI PARTNERS ──────────────────────────────────────────────────
def scrape_pai_partners(page):
    """
    166 .individual-profile cards visible.
    Each card has .name and .position children.
    No pagination needed — all loaded on one page.
    """
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
            name_el  = card.query_selector(".name")
            title_el = card.query_selector(".position")
            name  = clean_text(name_el.inner_text()  if name_el  else "")
            title = clean_text(title_el.inner_text() if title_el else "N/A")

            if not name or len(name) < 2:
                continue

            # Get position level from CSS class e.g. position-partner
            pos_class = ""
            try:
                cls = card.get_attribute("class") or ""
                match = re.search(r'position-([\w-]+)', cls)
                if match:
                    pos_class = match.group(1).replace("-", " ").title()
            except Exception:
                pass

            if title == "N/A" and pos_class:
                title = pos_class

            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 3. FRANCISCO PARTNERS ─────────────────────────────────────────────
def scrape_francisco_partners(page):
    """
    ~196 people links: a[href*='/team/person-name']
    Each link text contains 'Name | Title'.
    """
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
            # Skip the main /team page link
            if href.rstrip("/") == "/team":
                continue

            text = clean_text(link.inner_text())
            if not text or len(text) < 3:
                continue

            # Text format: "Name \n Title" or "Name | Title"
            parts = re.split(r'\n|\|', text)
            parts = [p.strip() for p in parts if p.strip()]

            name  = parts[0] if parts else ""
            title = parts[1] if len(parts) > 1 else "N/A"

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


# ── 4. GTCR ──────────────────────────────────────────────────────────
def scrape_gtcr(page):
    """
    WordPress REST API: GET /wp-json/wp/v2/team/
    Returns paginated JSON. Each item has title.rendered and acf fields.
    151 team members confirmed.
    """
    firm = "GTCR"
    data = []
    seen = set()

    print(f"[{firm}] Using WordPress REST API...")
    base = "https://www.gtcr.com/wp-json/wp/v2/team/"
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
                title = clean_text(acf.get("title") or acf.get("job_title") or acf.get("position") or "N/A")
                team  = clean_text(acf.get("team") or acf.get("group") or "N/A")
                loc   = clean_text(acf.get("location") or acf.get("office") or "N/A")

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

    # Fallback: DOM scraping if API gave nothing
    if not data:
        print(f"[{firm}] API empty, trying DOM...")
        cards = page.query_selector_all(".team-member")
        for card in cards:
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


# ── 5. SUMMIT PARTNERS ───────────────────────────────────────────────
def scrape_summit_partners(page):
    """
    224 .team-member-item cards each with .member-name and title/location.
    All loaded on one page (Webflow CMS).
    """
    firm = "Summit Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.summitpartners.com/team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    # Scroll to load all Webflow CMS items
    for _ in range(10):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
    page.evaluate("window.scrollTo(0, 0)")

    cards = page.query_selector_all(".team-member-item")
    print(f"[{firm}] Found {len(cards)} cards")

    for card in cards:
        try:
            name_el  = card.query_selector(".member-name")
            # Title is usually the second text element after name
            all_text = [l.strip() for l in card.inner_text().split("\n") if l.strip()]

            name = clean_text(name_el.inner_text() if name_el else "")
            if not name and all_text:
                # member name may be the second item (first is last name initial)
                for t in all_text:
                    if len(t) > 3 and not t.isupper() and " " in t:
                        name = t
                        break

            if not name or len(name) < 2:
                continue

            # Find title — usually appears after the name in the text
            title = "N/A"
            team_val = "N/A"
            loc = "N/A"
            found_name = False
            for t in all_text:
                if t == name:
                    found_name = True
                    continue
                if found_name:
                    if title == "N/A":
                        title = t
                    elif team_val == "N/A" and t not in {"1"}:
                        team_val = t
                    break

            # Also try href for extra info
            href = ""
            try:
                a = card.query_selector("a[href*='/team/']")
                if a:
                    href = a.get_attribute("href") or ""
                    # href text format: LastnameFirstname Last TitleTeamOffice1
                    # Not reliable, skip
            except Exception:
                pass

            key = f"{name}||{title}"
            if key in seen:
                continue
            seen.add(key)
            data.append(make_row(firm, name, title, team_val, loc))
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 6. INSIGHT PARTNERS ──────────────────────────────────────────────
def scrape_insight_partners(page):
    """
    WordPress JSON API: /wp-json/insight/v1/get-users?department=ID
    From diagnosis: department=12 → 151 Investors
    Need to discover other department IDs by clicking the filter tabs.
    """
    firm = "Insight Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page to discover departments...")
    page.goto("https://www.insightpartners.com/team/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    # Get all department labels and their IDs from the filter sidebar
    dept_ids = []
    try:
        labels = page.query_selector_all(".department[tabindex]")
        for label in labels:
            try:
                # Each label click triggers API call with department param
                text = clean_text(label.inner_text())
                print(f"[{firm}] Clicking department: {text}")
                label.click()
                page.wait_for_timeout(2_000)

                # Extract current URL which has ?department=X
                url = page.url
                match = re.search(r'\?department=(\d+)', url)
                if match:
                    dept_ids.append((text, int(match.group(1))))
                else:
                    # Try to get it from the API call URL pattern
                    dept_ids.append((text, None))
            except Exception:
                continue
    except Exception as e:
        print(f"[{firm}] Dept discovery error: {e}")

    # Known from diagnosis: department 12 = Investors (151 people)
    # Try sequential IDs if discovery failed
    if not dept_ids:
        print(f"[{firm}] Using known department IDs...")
        dept_ids = [
            ("Investors", 12),
            ("Onsite Experts", 13),
            ("Firm Operations", 14),
            ("Advisors", 15),
            ("IPPE", 16),
        ]

    print(f"[{firm}] Scraping {len(dept_ids)} departments via API...")

    for dept_name, dept_id in dept_ids:
        if not dept_id:
            continue
        try:
            api_url = f"https://www.insightpartners.com/wp-json/insight/v1/get-users?department={dept_id}&search="
            resp = page.evaluate(
                "async (url) => { const r = await fetch(url); return await r.text(); }",
                api_url
            )
            result = json.loads(resp)
            rows = result.get("rows", [])
            print(f"[{firm}] Dept '{dept_name}' (ID {dept_id}): {len(rows)} people")

            for person in rows:
                name  = clean_text(person.get("full_name", ""))
                title = clean_text(person.get("position", "N/A"))
                if not name:
                    continue
                key = f"{name}||{title}"
                if key in seen:
                    continue
                seen.add(key)
                data.append(make_row(firm, name, title, dept_name))
        except Exception as e:
            print(f"[{firm}] API error for dept {dept_id}: {e}")
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 7. ADVENT INTERNATIONAL ──────────────────────────────────────────
def scrape_advent(page):
    """
    386 people across multiple pages.
    URL pattern: /our-team/?sf_paged=N
    Cards: .c-card-people with .c-card__heading (name) and .c-card__body (title)
    24 per page → ceil(386/24) = 17 pages
    """
    firm = "Advent International"
    data = []
    seen = set()
    base_url = "https://www.adventinternational.com/our-team/"

    print(f"[{firm}] Loading page 1...")
    page.goto(base_url, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    def extract_cards():
        count = 0
        cards = page.query_selector_all(".c-card-people")
        for card in cards:
            try:
                name_el  = card.query_selector(".c-card__heading")
                body_el  = card.query_selector(".c-card__body")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                body  = clean_text(body_el.inner_text()  if body_el  else "")

                if not name or len(name) < 2:
                    continue

                # body is usually "Title, Location"
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

    # Detect total pages
    total_pages = 17
    try:
        pag_text = page.query_selector("[class*='pagination']")
        if pag_text:
            t = pag_text.inner_text()
            # "Results 1-24 of 386"
            match = re.search(r'of\s+(\d+)', t)
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


# ── 8. ALTOR EQUITY ──────────────────────────────────────────────────
def scrape_altor(page):
    """
    123 .g-content-card--coworker cards all on one page.
    Name: .g-content-card__header
    Title: .g-content-card__sub-header
    Correct URL: /our-team (redirects from /team)
    """
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


# ── 9. PARTNERS GROUP ────────────────────────────────────────────────
def scrape_partners_group(page):
    """
    Has tabs: Executive Team, Senior Management, Board of Directors.
    Cards: article elements with profile-detail__name and profile-detail__title.
    17 visible on first tab — need to click each tab.
    """
    firm = "Partners Group"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.partnersgroup.com/about-us/our-team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    def extract_current_tab():
        count = 0
        cards = page.query_selector_all("article, .profile-card")
        for card in cards:
            try:
                name_el  = card.query_selector(".profile-detail__name, h3, h4")
                title_el = card.query_selector(".profile-detail__title, [class*='title'], p")
                name  = clean_text(name_el.inner_text()  if name_el  else "")
                title = clean_text(title_el.inner_text() if title_el else "N/A")

                if not name or len(name) < 2 or name.lower() in {"read more"}:
                    continue
                # Skip if name looks like a nav item
                if any(kw in name.lower() for kw in ["linkedin", "press", "pdf", "news"]):
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

    # Click each tab
    tab_selectors = [
        ".tabs__link", "[role='tab']", "[class*='tab'] a",
        "button[class*='tab']",
    ]
    tabs_clicked = []
    for sel in tab_selectors:
        try:
            tabs = page.query_selector_all(sel)
            if tabs:
                for tab in tabs:
                    try:
                        label = clean_text(tab.inner_text())
                        if any(kw in label.lower() for kw in
                               ["executive", "senior", "board", "management", "team"]):
                            if label not in tabs_clicked:
                                tab.click()
                                page.wait_for_timeout(1_500)
                                c = extract_current_tab()
                                print(f"[{firm}] Tab '{label}': {c} people")
                                tabs_clicked.append(label)
                    except Exception:
                        continue
                if tabs_clicked:
                    break
        except Exception:
            continue

    # If no tabs clicked, just extract whatever's visible
    if not tabs_clicked:
        c = extract_current_tab()
        print(f"[{firm}] No tabs found, extracted {c} people from default view")

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 10. CVC CAPITAL ──────────────────────────────────────────────────
def scrape_cvc(page):
    """
    63 pages of pagination.
    Each page shows ~8 people cards: .people__box
    Name: .people__name + .people__last-name
    Title: .people__job
    """
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
        cards = page.query_selector_all(".people__box, .people__info")
        for card in cards:
            try:
                name_el      = card.query_selector(".people__name")
                lastname_el  = card.query_selector(".people__last-name")
                title_el     = card.query_selector(".people__job")

                first = clean_text(name_el.inner_text()     if name_el     else "")
                last  = clean_text(lastname_el.inner_text() if lastname_el else "")
                name  = f"{first} {last}".strip() if first or last else ""
                title = clean_text(title_el.inner_text()    if title_el    else "N/A")

                if not name:
                    # Try from link text
                    link = card.query_selector("a[href*='/about/our-people/']")
                    if link:
                        text = clean_text(link.inner_text())
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
            nums = re.findall(r'\d+', pag.inner_text())
            if nums:
                total_pages = max(int(n) for n in nums if int(n) < 200)
    except Exception:
        pass

    print(f"[{firm}] Scraping {total_pages} pages...")

    for pg in range(1, total_pages + 1):
        if pg > 1:
            try:
                # CVC uses numbered pagination buttons
                btn = page.query_selector(f"[class*='pagination__link']:has-text('{pg}'), "
                                          f".pagination__item:has-text('{pg}') a")
                if btn:
                    btn.click()
                    page.wait_for_timeout(2_000)
                else:
                    # Try next button
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


# ── 11. NAXICAP ──────────────────────────────────────────────────────
def scrape_naxicap(page):
    """
    111 .item elements each with .title inside.
    All on one page at naxicap.com/en/team/
    """
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
            # Skip nav items
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


# ── 12. HELLMAN & FRIEDMAN ───────────────────────────────────────────
def scrape_hf(page):
    """
    Correct URL: /people/ (not /team which redirects to portfolio)
    Has .bio-grid class — each bio is a clickable card.
    """
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

    # Try various selectors
    selectors_to_try = [
        ".bio-grid a", ".bio-grid > *",
        "[class*='bio']", "[class*='person']", "[class*='team']",
        "a[href*='people/']",
    ]

    for sel in selectors_to_try:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
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
                        if any(kw in name.lower() for kw in ["previous", "next", "close", "about"]):
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


# ── 13. IK PARTNERS ──────────────────────────────────────────────────
def scrape_ik_partners(page):
    """
    Correct URL: /our-people/ (not /team which was empty)
    """
    firm = "IK Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://ikpartners.com/our-people/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    # Scroll + Load More
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

    selectors = [
        "[class*='person']", "[class*='team-member']", "[class*='people']",
        "[class*='member']", "[class*='staff']", "article",
        "a[href*='our-people/']",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
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


# ── 14. L CATTERTON ──────────────────────────────────────────────────
def scrape_lcatterton(page):
    """
    Correct URL: /People.html
    """
    firm = "L Catterton"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.lcatterton.com/People.html",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    selectors = [
        "[class*='person']", "[class*='people']", "[class*='team']",
        "[class*='member']", "[class*='bio']", "[class*='profile']",
        "article", ".card",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
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


# ── 15. BRIDGEPOINT ──────────────────────────────────────────────────
def scrape_bridgepoint(page):
    """
    Correct URL: /about-us/our-people (not /team which redirects to jobs)
    """
    firm = "Bridgepoint"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.bridgepointgroup.com/about-us/our-people",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2_000)
    page.evaluate("window.scrollTo(0, 0)")

    selectors = [
        "[class*='person']", "[class*='people']", "[class*='team']",
        "[class*='profile']", "[class*='bio']", "article",
        "a[href*='our-people/']",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
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
                        if name.lower() in {"our people", "about us", "skip to main content"}:
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


# ── 16. APAX PARTNERS ────────────────────────────────────────────────
def scrape_apax(page):
    """
    Correct URL: /people/our-team/ (not /en/people which is 404)
    """
    firm = "Apax Partners"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    try:
        page.goto("https://www.apax.com/people/our-team/",
                  wait_until="domcontentloaded", timeout=60_000)
    except Exception as e:
        print(f"[{firm}] Load error: {e}")
        return data

    page.wait_for_timeout(4_000)
    dismiss_cookies(page)

    # Scroll + Load More
    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        try:
            btn = page.query_selector("button:has-text('Load More'), a:has-text('Load More')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(1_500)
            else:
                break
        except Exception:
            break

    page.evaluate("window.scrollTo(0, 0)")

    selectors = [
        "[class*='person']", "[class*='people']", "[class*='profile']",
        "[class*='team-member']", "[class*='bio']", "article",
        "a[href*='our-team/']",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
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
                        if name.lower() in {"people", "our team", "all people"}:
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


# ── 17. ARES MANAGEMENT ──────────────────────────────────────────────
def scrape_ares(page):
    """
    Correct URL: /about-ares-management-corporation/our-team
    """
    firm = "Ares Management"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.aresmgmt.com/about-ares-management-corporation/our-team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    # Click tabs for each business unit
    tabs_to_try = [
        "button[role='tab']", "[class*='tab'] button",
        "[class*='tab'] a", ".nav-item a",
    ]

    clicked_tabs = set()

    def extract_visible():
        count = 0
        for sel in ["[class*='person']", "[class*='member']", "[class*='team']",
                    "[class*='bio']", "[class*='profile']", "article"]:
            try:
                cards = page.query_selector_all(sel)
                if len(cards) >= 3:
                    for card in cards:
                        try:
                            lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                            if not lines:
                                continue
                            name = lines[0]
                            title = lines[1] if len(lines) > 1 else "N/A"
                            if not name or len(name) < 2:
                                continue
                            if any(kw in name.lower() for kw in
                                   ["our team", "about", "contact", "news", "investor"]):
                                continue
                            key = f"{name}||{title}"
                            if key in seen:
                                continue
                            seen.add(key)
                            data.append(make_row(firm, name, title))
                            count += 1
                        except Exception:
                            continue
                    if count > 0:
                        return count
            except Exception:
                continue
        return count

    # Scroll and extract
    for _ in range(10):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(800)

    c = extract_visible()
    print(f"[{firm}] Default view: {c} people")

    # Try clicking each tab
    for sel in tabs_to_try:
        try:
            tabs = page.query_selector_all(sel)
            for tab in tabs:
                try:
                    label = clean_text(tab.inner_text())
                    if label in clicked_tabs or not label or len(label) > 30:
                        continue
                    tab.click()
                    page.wait_for_timeout(1_500)
                    c = extract_visible()
                    if c > 0:
                        print(f"[{firm}] Tab '{label}': {c} people")
                    clicked_tabs.add(label)
                except Exception:
                    continue
        except Exception:
            continue

    print(f"[{firm}] ✓ {len(data)} employees")
    return data


# ── 18. NORDIC CAPITAL ───────────────────────────────────────────────
def scrape_nordic_capital(page):
    """
    Correct URL: /our-people/ (not /team which is 404)
    """
    firm = "Nordic Capital"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.nordiccapital.com/our-people/",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    # Scroll + Load More
    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
        try:
            btn = page.query_selector("button:has-text('Load more'), a:has-text('Load more')")
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(2_000)
            else:
                break
        except Exception:
            break

    page.evaluate("window.scrollTo(0, 0)")

    selectors = [
        "[class*='person']", "[class*='people']", "[class*='team']",
        "[class*='profile']", "[class*='member']", "article",
        "a[href*='our-people/']",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
                print(f"[{firm}] Found {len(cards)} cards with: {sel}")
                for card in cards:
                    try:
                        lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                        if not lines:
                            continue
                        name = lines[0]
                        title = lines[1] if len(lines) > 1 else "N/A"
                        if not name or len(name) < 2:
                            continue
                        if name.lower() in {"our people", "people", "culture"}:
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


# ── 19. BLUE OWL ─────────────────────────────────────────────────────
def scrape_blue_owl(page):
    """
    Correct URL: /our-team (not /team which is 404)
    """
    firm = "Blue Owl"
    data = []
    seen = set()

    print(f"[{firm}] Loading page...")
    page.goto("https://www.blueowl.com/our-team",
              wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3_000)
    dismiss_cookies(page)

    for _ in range(10):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)

    page.evaluate("window.scrollTo(0, 0)")

    selectors = [
        "[class*='person']", "[class*='people']", "[class*='team']",
        "[class*='profile']", "[class*='member']", "[class*='bio']",
        "article",
    ]

    for sel in selectors:
        try:
            cards = page.query_selector_all(sel)
            if len(cards) >= 3:
                print(f"[{firm}] Found {len(cards)} cards with: {sel}")
                for card in cards:
                    try:
                        lines = [l.strip() for l in card.inner_text().split("\n") if l.strip()]
                        if not lines:
                            continue
                        name = lines[0]
                        title = lines[1] if len(lines) > 1 else "N/A"
                        if not name or len(name) < 2:
                            continue
                        if any(kw in name.lower() for kw in
                               ["what we", "who we", "our team", "credit", "real estate"]):
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


# ── MAIN ─────────────────────────────────────────────────────────────

SCRAPERS = [
    ("Apollo",              scrape_apollo),
    ("PAI Partners",        scrape_pai_partners),
    ("Francisco Partners",  scrape_francisco_partners),
    ("GTCR",                scrape_gtcr),
    ("Summit Partners",     scrape_summit_partners),
    ("Insight Partners",    scrape_insight_partners),
    ("Advent International",scrape_advent),
    ("Altor Equity",        scrape_altor),
    ("Partners Group",      scrape_partners_group),
    ("CVC Capital",         scrape_cvc),
    ("Naxicap",             scrape_naxicap),
    ("Hellman & Friedman",  scrape_hf),
    ("IK Partners",         scrape_ik_partners),
    ("L Catterton",         scrape_lcatterton),
    ("Bridgepoint",         scrape_bridgepoint),
    ("Apax Partners",       scrape_apax),
    ("Ares Management",     scrape_ares),
    ("Nordic Capital",      scrape_nordic_capital),
    ("Blue Owl",            scrape_blue_owl),
]

SKIPPED = [
    ("Carlyle",          "Cloudflare block — needs residential proxy"),
    ("Bain Capital",     "Blank SPA page — no team page found"),
    ("TPG",              "404 — no public team page found"),
    ("Hg",               "404 — no public team page found"),
    ("Triton",           "triton.com is an ATM company, not PE firm — need correct URL"),
    ("GIC",              "No public team page"),
    ("Genstar Capital",  "Blank redirect — no public team page"),
    ("Waterland",        "Redirects to portfolio company page — no public team page"),
    ("Neuberger Berman", "Wrong region redirect, correct URL unknown"),
    ("Eurazeo",          "404 — correct URL unknown"),
    ("CapVest",          "404 — no public team page"),
    ("Thoma Bravo",      "ERR_ABORTED on load — site blocking"),
]


if __name__ == "__main__":
    all_data = []
    results = {}
    failed = []

    print("=" * 60)
    print(f"  Broken Firms Scraper — {len(SCRAPERS)} firms")
    print("=" * 60)

    print(f"\n⚠️  Skipping {len(SKIPPED)} firms (no public page / blocked):")
    for firm, reason in SKIPPED:
        print(f"  • {firm}: {reason}")

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

        for i, (firm, scraper_func) in enumerate(SCRAPERS, 1):
            print(f"\n{'─'*60}")
            print(f"  [{i}/{len(SCRAPERS)}] {firm}")
            print(f"{'─'*60}")
            try:
                site_data = scraper_func(page)
                all_data.extend(site_data)
                results[firm] = len(site_data)
            except Exception as e:
                print(f"[{firm}] ✗ FAILED: {e}")
                failed.append((firm, str(e)))
                results[firm] = 0

        browser.close()

    print(f"\n{'='*60}  DONE  {'='*60}")

    if all_data:
        df = pd.DataFrame(all_data)
        try:
            df.to_excel(OUTPUT_FILE, index=False)
            print(f"\nSaved: {OUTPUT_FILE}")
        except PermissionError:
            backup = "broken_firms_backup.xlsx"
            df.to_excel(backup, index=False)
            print(f"Saved (backup): {backup}")

    print("\nResults:")
    for firm, count in sorted(results.items(), key=lambda x: -x[1]):
        s = "✓" if count >= 50 else ("~" if count >= 10 else "✗")
        print(f"  {s} {firm}: {count}")

    print(f"\nTotal: {len(all_data)}")

    if failed:
        print(f"\n⚠️  Failed ({len(failed)}):")
        for firm, err in failed:
            print(f"  • {firm}: {err[:80]}")