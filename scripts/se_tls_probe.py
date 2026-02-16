#!/usr/bin/env python3
"""
StreetEasy TLS-Client Prototype

Tests whether we can fetch SE building pages using:
1. TLS fingerprint impersonation (chrome_120 profile)
2. PX cookies extracted from a real Chrome session
3. Optional RSC headers for structured data

Usage:
  # First: extract cookies from Chrome
  python3 scripts/se_tls_probe.py --extract-cookies

  # Then: test a building page
  python3 scripts/se_tls_probe.py --test 740-park-avenue-new_york

  # Test RSC format
  python3 scripts/se_tls_probe.py --test 740-park-avenue-new_york --rsc

  # Test multiple buildings
  python3 scripts/se_tls_probe.py --test-batch --limit 10
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

try:
    import tls_client
except ImportError:
    print("pip3 install tls-client")
    sys.exit(1)

COOKIE_FILE = Path(__file__).parent.parent / "se_cache" / "chrome_cookies.json"


# ── Cookie extraction via AppleScript ────────────────────────

def extract_cookies_from_chrome():
    """Use AppleScript to grab cookies from Chrome's active SE session."""
    # Navigate to SE first to ensure cookies are fresh
    nav_script = 'tell application "Google Chrome" to set URL of active tab of first window to "https://streeteasy.com/"'
    subprocess.run(["osascript", "-e", nav_script], capture_output=True, text=True, timeout=10)
    time.sleep(4)

    js = "document.cookie"
    escaped = js.replace("\\", "\\\\").replace('"', '\\"')
    script = f'tell application "Google Chrome" to execute active tab of first window javascript "{escaped}"'
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=10)

    if r.returncode != 0:
        print(f"Error: {r.stderr}")
        return None

    raw = r.stdout.strip()
    if not raw:
        print("No cookies returned. Is Chrome open with SE loaded?")
        return None

    # Parse "key=value; key2=value2" format
    cookies = {}
    for pair in raw.split("; "):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cookies[k.strip()] = v.strip()

    # Also grab user-agent
    js_ua = "navigator.userAgent"
    escaped_ua = js_ua.replace("\\", "\\\\").replace('"', '\\"')
    script_ua = f'tell application "Google Chrome" to execute active tab of first window javascript "{escaped_ua}"'
    r_ua = subprocess.run(["osascript", "-e", script_ua], capture_output=True, text=True, timeout=10)
    user_agent = r_ua.stdout.strip() if r_ua.returncode == 0 else None

    result = {"cookies": cookies, "user_agent": user_agent, "extracted_at": time.strftime("%Y-%m-%d %H:%M:%S")}

    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(COOKIE_FILE, "w") as f:
        json.dump(result, f, indent=2)

    # Report PX cookies
    px_keys = [k for k in cookies if k.startswith("_px")]
    print(f"Extracted {len(cookies)} cookies ({len(px_keys)} PX cookies)")
    for k in px_keys:
        print(f"  {k} = {cookies[k][:40]}...")
    if user_agent:
        print(f"  User-Agent: {user_agent[:80]}...")
    print(f"Saved to {COOKIE_FILE}")

    return result


def load_cookies():
    """Load saved cookies."""
    if not COOKIE_FILE.exists():
        print(f"No cookies found. Run: python3 {sys.argv[0]} --extract-cookies")
        return None
    with open(COOKIE_FILE) as f:
        data = json.load(f)

    age_secs = time.time() - time.mktime(time.strptime(data["extracted_at"], "%Y-%m-%d %H:%M:%S"))
    age_mins = int(age_secs / 60)
    print(f"Cookies age: {age_mins} minutes", end="")
    if age_mins > 50:
        print(" ⚠ STALE (>50min, PX session may have expired)")
    else:
        print(" ✓")
    return data


# ── TLS-Client session ───────────────────────────────────────

def create_session(cookie_data):
    """Create a tls_client session impersonating Chrome."""
    session = tls_client.Session(
        client_identifier="chrome_120",
        random_tls_extension_order=True,
    )

    # Set cookies
    for k, v in cookie_data["cookies"].items():
        session.cookies.set(k, v, domain=".streeteasy.com")

    # Set headers to match real Chrome
    ua = cookie_data.get("user_agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    session.headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
    }

    return session


def fetch_building(session, slug, use_rsc=False):
    """Fetch a building page. Returns (status_code, content_length, content_or_error)."""
    url = f"https://streeteasy.com/building/{slug}"

    headers = {}
    if use_rsc:
        headers = {
            "RSC": "1",
            "Next-Url": f"/building/{slug}",
            "Accept": "text/x-component",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }

    t0 = time.time()
    try:
        resp = session.get(url, headers=headers)
        elapsed = time.time() - t0
        return resp.status_code, len(resp.text), elapsed, resp.text
    except Exception as e:
        elapsed = time.time() - t0
        return 0, 0, elapsed, str(e)


def analyze_response(status, length, elapsed, content, slug):
    """Analyze what we got back."""
    result = {
        "slug": slug,
        "status": status,
        "length": length,
        "elapsed_ms": int(elapsed * 1000),
    }

    if status == 403:
        if "blocked" in content.lower() or "px" in content.lower():
            result["verdict"] = "PX_BLOCKED"
        else:
            result["verdict"] = "FORBIDDEN"
    elif status == 200:
        # Check for real PX block: a challenge-only page is short and has no real content
        is_px_only = ("px-captcha" in content and len(content) < 5000)
        m = re.search(r'<title>([^<]+)</title>', content)
        title = m.group(1) if m else ""

        if is_px_only:
            result["verdict"] = "PX_CHALLENGE"
        elif "StreetEasy" in title:
            result["verdict"] = "SUCCESS"
            result["title"] = title
            result["has_rsc"] = "self.__next_f" in content
            result["rsc_chunks"] = content.count("self.__next_f")
        else:
            result["verdict"] = "UNKNOWN_200"
    elif status == 0:
        result["verdict"] = "ERROR"
        result["error"] = content[:200]
    else:
        result["verdict"] = f"HTTP_{status}"

    return result


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SE TLS-Client Probe")
    parser.add_argument("--extract-cookies", action="store_true", help="Extract cookies from Chrome")
    parser.add_argument("--test", help="Test a single building slug")
    parser.add_argument("--rsc", action="store_true", help="Use RSC headers")
    parser.add_argument("--test-batch", action="store_true", help="Test a batch of buildings")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--raw", action="store_true", help="Print raw response body")
    args = parser.parse_args()

    if args.extract_cookies:
        extract_cookies_from_chrome()
        return

    cookie_data = load_cookies()
    if not cookie_data:
        return

    session = create_session(cookie_data)

    slugs = []
    if args.test:
        slugs = [args.test]
    elif args.test_batch:
        # Use known-good slugs from our previous scrape
        slugs = [
            "740-park-avenue-new_york",
            "834-5-avenue-new_york",
            "5-east-17-street-new_york",
            "383-west-broadway-new_york",
            "33-east-74-street-new_york",
            "the-gramercy-new_york",
            "one-manhattan-square-new_york",
            "15-central-park-west-new_york",
            "432-park-avenue-new_york",
            "56-leonard-street-new_york",
        ][:args.limit]
    else:
        parser.print_help()
        return

    print(f"\n{'='*60}")
    print(f"Testing {len(slugs)} building(s) — RSC: {'yes' if args.rsc else 'no'}")
    print(f"{'='*60}\n")

    results = []
    for slug in slugs:
        status, length, elapsed, content = fetch_building(session, slug, use_rsc=args.rsc)
        result = analyze_response(status, length, elapsed, content, slug)
        results.append(result)

        verdict = result["verdict"]
        title = result.get("title", "")
        print(f"  {slug}")
        print(f"    HTTP {status} | {length:,} bytes | {int(elapsed*1000)}ms | {verdict}")
        if title:
            print(f"    Title: {title}")
        if args.raw and status == 200:
            print(f"    Body (first 500 chars):")
            print(f"    {content[:500]}")
        print()

        time.sleep(0.5)  # be gentle

    # Summary
    verdicts = [r["verdict"] for r in results]
    print(f"{'='*60}")
    print(f"Results: {len([v for v in verdicts if v == 'SUCCESS'])} success, "
          f"{len([v for v in verdicts if 'BLOCK' in v or 'CHALLENGE' in v])} blocked, "
          f"{len([v for v in verdicts if v not in ('SUCCESS',) and 'BLOCK' not in v and 'CHALLENGE' not in v])} other")


if __name__ == "__main__":
    main()
