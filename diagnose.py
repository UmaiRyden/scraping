"""
Diagnosis Script — Broken & Missing Firms Only
================================================
Runs ONLY on the firms that returned 0, garbage, or errors.
Opens each site, captures:
  - All CSS classes related to people/team
  - Any JSON API calls made during page load
  - Sample text from likely card elements
  - Pagination/filter controls
  - Raw HTML structure sample

Output: diagnosis_report.txt
Share this file and custom scrapers will be written for each firm.

Firms diagnosed (32 total):
  Garbage data:  Apollo, Francisco Partners, Ardian, Ares, Brookfield,
                 CVC, Naxicap, Nordic Capital, Thoma Bravo, Bridgepoint,
                 GTCR, Summit Partners, IK Partners, Blue Owl, PAI Partners
  Zero data:     TPG, Hg, Hellman & Friedman, Bain Capital, Carlyle,
                 Insight Partners, Genstar, Neuberger Berman, Eurazeo,
                 Altor, Waterland, GIC, CapVest, L Catterton, Partners Group
  Failed:        Apax Partners, Triton, Advent International
"""

from playwright.sync_api import sync_playwright
import json
import re
from collections import Counter
from datetime import datetime

OUTPUT_FILE = "diagnosis_report.txt"

BROKEN_SITES = [
    # Garbage data — wrong selector
    {"name": "Apollo",            "url": "https://www.apollo.com/about-apollo/our-people"},
    {"name": "PAI Partners",      "url": "https://www.paipartners.com/team"},
    {"name": "Francisco Partners","url": "https://www.franciscopartners.com/team"},
    {"name": "Ardian",            "url": "https://www.ardian.com/team"},
    {"name": "Ares Management",   "url": "https://www.aresmgmt.com/team"},
    {"name": "Brookfield",        "url": "https://www.brookfield.com/team"},
    {"name": "CVC Capital",       "url": "https://www.cvc.com/about/our-people"},
    {"name": "Nordic Capital",    "url": "https://www.nordiccapital.com/team"},
    {"name": "Thoma Bravo",       "url": "https://www.thomabravo.com/team"},
    {"name": "Bridgepoint",       "url": "https://www.bridgepoint.eu/team"},
    {"name": "GTCR",              "url": "https://www.gtcr.com/team"},
    {"name": "Summit Partners",   "url": "https://www.summitpartners.com/team"},
    {"name": "IK Partners",       "url": "https://www.ikpartners.com/team"},
    {"name": "Blue Owl",          "url": "https://www.blueowl.com/team"},
    {"name": "Naxicap",           "url": "https://www.naxicap.fr/en/team"},
    # Zero data — no selector worked
    {"name": "TPG",               "url": "https://www.tpg.com/team"},
    {"name": "Hg",                "url": "https://hg.co/team"},
    {"name": "Hellman & Friedman","url": "https://www.hf.com/team"},
    {"name": "Bain Capital",      "url": "https://www.baincapital.com/team"},
    {"name": "Carlyle",           "url": "https://www.carlyle.com/about/team"},
    {"name": "Insight Partners",  "url": "https://www.insightpartners.com/team"},
    {"name": "Genstar Capital",   "url": "https://www.genstarcapital.com/team"},
    {"name": "Neuberger Berman",  "url": "https://www.nb.com/en/us/about-us/our-team"},
    {"name": "Eurazeo",           "url": "https://www.eurazeo.com/en/our-team"},
    {"name": "Altor Equity",      "url": "https://www.altor.com/team"},
    {"name": "Waterland",         "url": "https://www.waterland.nu/team"},
    {"name": "GIC",               "url": "https://www.gic.com.sg/our-people"},
    {"name": "CapVest",           "url": "https://www.capvest.com/team"},
    {"name": "L Catterton",       "url": "https://www.lcatterton.com/team"},
    {"name": "Partners Group",    "url": "https://www.partnersgroup.com/about-us/our-team"},
    # Failed — errors
    {"name": "Apax Partners",     "url": "https://www.apax.com/en/people"},
    {"name": "Triton",            "url": "https://www.triton.com/people"},
    {"name": "Advent International","url": "https://www.adventinternational.com/team"},
]


def dismiss_cookies(page):
    for sel in [
        "#onetrust-accept-btn-handler",
        "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
        "button:has-text('Accept All')", "button:has-text('Accept')",
        "button:has-text('Allow All')", "button:has-text('Agree')",
        "button[id*='accept']", "button[class*='accept']",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(800)
                return
        except Exception:
            continue


def diagnose(page, name, url):
    lines = []
    api_calls = []

    def on_response(response):
        ct = response.headers.get("content-type", "")
        if "json" in ct:
            try:
                body = response.json()
                preview = str(body)[:400]
                api_calls.append({
                    "url": response.url,
                    "status": response.status,
                    "preview": preview,
                })
            except Exception:
                pass

    page.on("response", on_response)

    # Load page
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3_000)
    except Exception as e:
        lines.append(f"❌ PAGE LOAD ERROR: {e}")
        print(f"  [{name}] Load error: {e}")
        return "\n".join(lines), api_calls

    dismiss_cookies(page)

    # Scroll to trigger lazy loading + network calls
    for _ in range(6):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1_000)
    page.evaluate("window.scrollTo(0, 0)")
    page.wait_for_timeout(500)

    lines.append(f"Page title: {page.title()}")
    lines.append(f"Final URL:  {page.url}")

    # ── Relevant CSS classes ─────────────────────────────────────────
    all_classes = page.evaluate("""
        () => {
            const c = [];
            document.querySelectorAll('*').forEach(el => el.classList.forEach(x => c.push(x)));
            return c;
        }
    """)
    counts = Counter(all_classes)
    keywords = ["person","people","team","member","bio","profile","card","staff",
                "employee","name","title","role","position","grid","list","item",
                "filter","tab","pagination","load","more","show","next","prev"]
    relevant = {c: n for c, n in counts.items()
                if any(k in c.lower() for k in keywords)}

    lines.append(f"\n--- RELEVANT CSS CLASSES ---")
    if relevant:
        for c, n in sorted(relevant.items(), key=lambda x: -x[1])[:30]:
            lines.append(f"  .{c}  ({n}x)")
    else:
        lines.append("  None found")

    # ── Sample text from card elements ───────────────────────────────
    lines.append(f"\n--- SAMPLE TEXT FROM CARD ELEMENTS ---")
    test_sels = [
        "article", "[class*='card']", "[class*='person']",
        "[class*='member']", "[class*='team']", "[class*='bio']",
        "[class*='profile']", "[class*='people']", "[class*='staff']",
        "a[href*='people']", "a[href*='team']", "a[href*='bio']",
        "li", "[class*='item']",
    ]
    found_any = False
    for sel in test_sels:
        try:
            els = page.query_selector_all(sel)
            if len(els) >= 3:
                found_any = True
                lines.append(f"\n  '{sel}' → {len(els)} elements")
                for i, el in enumerate(els[:4]):
                    try:
                        text = el.inner_text().strip()[:150].replace("\n", " | ")
                        href = el.get_attribute("href") or ""
                        lines.append(f"    [{i+1}] text={text!r}  href={href[:60]!r}")
                    except Exception:
                        pass
        except Exception:
            continue
    if not found_any:
        lines.append("  No matching elements found")

    # ── Buttons and pagination ───────────────────────────────────────
    lines.append(f"\n--- VISIBLE BUTTONS & PAGINATION ---")
    for sel in ["button", "[class*='pagination']", "[class*='next']",
                "[class*='load-more']", "[role='button']", "a[class*='next']"]:
        try:
            els = page.query_selector_all(sel)
            texts = []
            for el in els[:10]:
                try:
                    if el.is_visible():
                        t = el.inner_text().strip()[:40]
                        if t:
                            texts.append(t)
                except Exception:
                    pass
            if texts:
                lines.append(f"  '{sel}': {texts[:8]}")
        except Exception:
            continue

    # ── Filter/tab elements ──────────────────────────────────────────
    lines.append(f"\n--- FILTER / TAB ELEMENTS ---")
    for sel in ["[class*='filter']", "[class*='tab']", "[role='tab']",
                "select", "[class*='dropdown']", "[class*='category']"]:
        try:
            els = page.query_selector_all(sel)
            if els:
                texts = []
                for el in els[:8]:
                    try:
                        t = el.inner_text().strip()[:40]
                        if t:
                            texts.append(t)
                    except Exception:
                        pass
                if texts:
                    lines.append(f"  '{sel}': {texts}")
        except Exception:
            continue

    # ── JSON API calls ───────────────────────────────────────────────
    lines.append(f"\n--- JSON API CALLS ({len(api_calls)} detected) ---")
    if api_calls:
        for call in api_calls[:8]:
            lines.append(f"  ✅ URL: {call['url']}")
            lines.append(f"     Status: {call['status']}")
            lines.append(f"     Preview: {call['preview'][:300]}")
            lines.append("")
    else:
        lines.append("  None — no JSON API detected")

    # ── HTML snippet ─────────────────────────────────────────────────
    lines.append(f"\n--- HTML STRUCTURE (first 1500 chars of main content) ---")
    try:
        html = page.evaluate("""
            () => {
                const m = document.querySelector('main, [role="main"], .main, #main') || document.body;
                return m ? m.innerHTML.substring(0, 1500) : '';
            }
        """)
        lines.append(re.sub(r'\s+', ' ', html).strip()[:1500])
    except Exception as e:
        lines.append(f"  Error: {e}")

    print(f"  [{name}] Done — {len(api_calls)} API calls, {len(relevant)} relevant classes")
    return "\n".join(lines), api_calls


if __name__ == "__main__":
    all_reports = [
        f"DIAGNOSIS REPORT — BROKEN & MISSING FIRMS",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total firms diagnosed: {len(BROKEN_SITES)}",
        "=" * 70,
    ]

    api_summary = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=30)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        for i, site in enumerate(BROKEN_SITES, 1):
            name = site["name"]
            url  = site["url"]
            print(f"\n[{i}/{len(BROKEN_SITES)}] Diagnosing: {name}")
            print(f"  URL: {url}")

            report, api_calls = diagnose(page, name, url)

            all_reports.append(f"\n{'#'*70}")
            all_reports.append(f"# [{i}/{len(BROKEN_SITES)}] {name.upper()}")
            all_reports.append(f"# URL: {url}")
            all_reports.append(f"{'#'*70}")
            all_reports.append(report)

            if api_calls:
                api_summary.append(f"✅ {name}: {len(api_calls)} API calls found")
            else:
                api_summary.append(f"❌ {name}: No API calls — HTML only")

        browser.close()

    # Summary at top
    all_reports.insert(4, "\n--- QUICK SUMMARY (API vs HTML) ---")
    all_reports.insert(5, "\n".join(api_summary))
    all_reports.insert(6, "\n" + "=" * 70)

    full_text = "\n".join(all_reports)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(full_text)

    print(f"\n{'='*60}")
    print(f"✓ Diagnosis complete!")
    print(f"  Report saved to: {OUTPUT_FILE}")
    print(f"\nAPI Summary:")
    for s in api_summary:
        print(f"  {s}")
    print(f"\nNext step: share '{OUTPUT_FILE}' to get custom scrapers written.")