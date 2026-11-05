#!/usr/bin/env python3
"""
================================================================================
ouo.io 24/7 Automation & Telegram Control Center (Standalone All-in-One Engine)
================================================================================
Combines:
  1. ouo_login.py     - Headless / proxy login and session cookie generator
  2. ouo_shorten.py   - Authenticated link shortener using CSRF token
  3. ouo_visitor.py   - Human ad-view simulator with Cloudflare bypass & timer
  4. ouo_stats.py     - Thread-safe persistent stats and dashboard cache
  5. ouo_campaign.py  - End-to-end automated campaign loop
  6. ouo_daemon.py    - 24/7 Multi-worker concurrency engine (US rotating IPs)
  7. ouo_bot.py       - Telegram bot control center & live status card

Usage:
  python ouo_standalone.py daemon               # Run 24/7 multi-worker background engine
  python ouo_standalone.py bot                  # Run Telegram bot interface
  python ouo_standalone.py all                  # Run BOTH Daemon & Telegram Bot concurrently
  python ouo_standalone.py campaign [views]     # Run campaign for N views (e.g. 5)
  python ouo_standalone.py login                # Test login & generate session cookies
  python ouo_standalone.py shorten <target_url> # Shorten target URL
  python ouo_standalone.py visit <short_url>    # Visit and bypass a shortlink
  python ouo_standalone.py status               # Display current stats in terminal
================================================================================
"""

import os
import sys
import time
import json
import random
import string
import signal
import argparse
import threading
import subprocess
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    sync_playwright = None
    PLAYWRIGHT_AVAILABLE = False

# Optional Telegram bot support
try:
    import telebot
    from telebot import types
    from telebot.apihelper import ApiTelegramException
    TELEBOT_AVAILABLE = True
except ImportError:
    telebot = None
    types = None
    ApiTelegramException = Exception
    TELEBOT_AVAILABLE = False

# Force unbuffered output for live logging
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# ------------------------------------------------------------------------------
# 1. PATH RESOLUTION & CONFIGURATION
# ------------------------------------------------------------------------------
BASE_DIR = os.environ.get("OUO_DATA_DIR", os.path.dirname(os.path.abspath(__file__)))

def resolve_path(filename, legacy_path=None):
    local_p = os.path.join(BASE_DIR, filename)
    if not os.path.exists(local_p) and legacy_path and os.path.exists(legacy_path):
        return legacy_path
    return local_p

SESSION_FILE = resolve_path("ouo_session.json", "/home/ipx/ouo_session.json")
STATS_FILE = resolve_path("campaign_stats.json", "/home/ipx/campaign_stats.json")
AUTH_FILE = resolve_path("bot_auth.json", "/home/ipx/bot_auth.json")

# Dedicated Rotating Proxies for visitor traffic (Webshare)
VISITOR_PROXY_SERVER = os.environ.get('OUO_VISITOR_PROXY_SERVER', 'http://p.webshare.io:80')
VISITOR_PROXY_USER = os.environ.get('OUO_VISITOR_PROXY_USER', 'yjzdynuw-rotate')
VISITOR_PROXY_PASS = os.environ.get('OUO_VISITOR_PROXY_PASS', 'z8aeyhoyjcz2')

def get_rotating_visitor_proxy():
    cfg = {
        'server': VISITOR_PROXY_SERVER,
        'username': VISITOR_PROXY_USER,
        'password': VISITOR_PROXY_PASS
    }
    return cfg, "🇺🇸 US (Webshare Rotating)"

VISITOR_PROXY_CONFIG = {
    'server': VISITOR_PROXY_SERVER,
    'username': VISITOR_PROXY_USER,
    'password': VISITOR_PROXY_PASS
}

# Proxy for login, dashboard & shortener
PROXY_CONFIG = VISITOR_PROXY_CONFIG
if os.environ.get('OUO_PROXY_SERVER'):
    PROXY_CONFIG = {
        'server': os.environ.get('OUO_PROXY_SERVER'),
        'username': os.environ.get('OUO_PROXY_USER', VISITOR_PROXY_USER),
        'password': os.environ.get('OUO_PROXY_PASS', VISITOR_PROXY_PASS)
    }

OUO_USERNAME = os.environ.get('OUO_USERNAME', "nxtzenmaker@gmail.com")
OUO_PASSWORD = os.environ.get('OUO_PASSWORD', "Zeno@123")
MASTER_PASSWORD = os.environ.get('MASTER_PASSWORD', "Raj Nandini is the heart of Rudra/Zeno")
BOT_TOKEN = os.environ.get('BOT_TOKEN', "8943552749:AAEq10g6coSpFo39pQHzJO0ja9K15VlVm6Y")
DEFAULT_VERCEL_APP = os.environ.get('VERCEL_APP_URL', "https://ouo-portal.vercel.app")
ADMIN_CHAT_ID = os.environ.get('ADMIN_CHAT_ID', "8321630022")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

DEFAULT_CHROMIUM_ARGS = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-blink-features=AutomationControlled',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--single-process',
    '--no-zygote',
    '--disable-software-rasterizer',
    '--disable-extensions',
    '--js-flags=--max-old-space-size=128',
    '--window-size=1280,720'
]

HOURLY_CHECK_INTERVAL = 300  # Check every 5 minutes
NUM_WORKERS = int(os.environ.get("OUO_WORKERS", 40))
SESSION_LIFETIME = 86400      # 24 Hours

# Concurrency Locks & Flags
stats_lock = threading.Lock()
shortener_lock = threading.Lock()
daemon_running = False

# ------------------------------------------------------------------------------
# 2. STATS ENGINE (ouo_stats.py)
# ------------------------------------------------------------------------------
DEFAULT_STATS = {
    "total_links_created": 0,
    "total_views_completed": 0,
    "failed_views": 0,
    "success_rate": "100%",
    "daemon_status": "⏸️ STOPPED",
    "daemon_started_at": 0,
    "daemon_started_str": "",
    "last_activity_time": 0,
    "last_activity_str": "None yet",
    "last_shortlink": "None yet",
    "last_target_url": "None yet",
    "last_visitor_country": "🇺🇸 US",
    "dashboard": {
        "views": "0",
        "earnings": "$0.00",
        "ecpm": "$0.00",
        "available": "$0.00",
        "last_checked_at": 0,
        "last_checked_str": "Never",
        "next_check_at": 0,
        "next_check_str": "Pending"
    },
    "history": []
}

def get_current_time_str(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def _load_stats_unlocked():
    if not os.path.exists(STATS_FILE):
        return DEFAULT_STATS.copy()
    try:
        with open(STATS_FILE, 'r') as f:
            data = json.load(f)
            for k, v in DEFAULT_STATS.items():
                if k not in data:
                    data[k] = v
            return data
    except Exception:
        return DEFAULT_STATS.copy()

def _save_stats_unlocked(stats):
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        tmp_file = STATS_FILE + ".tmp"
        with open(tmp_file, 'w') as f:
            json.dump(stats, f, indent=2)
        os.replace(tmp_file, STATS_FILE)
    except Exception as e:
        print(f"[!] Error saving stats: {e}")

def load_stats():
    with stats_lock:
        if not os.path.exists(STATS_FILE):
            stats = DEFAULT_STATS.copy()
            stats["daemon_started_at"] = time.time()
            stats["daemon_started_str"] = get_current_time_str(stats["daemon_started_at"])
            _save_stats_unlocked(stats)
            return stats
        return _load_stats_unlocked()

def save_stats(stats):
    with stats_lock:
        _save_stats_unlocked(stats)

def record_link_created(short_url, target_url):
    with stats_lock:
        stats = _load_stats_unlocked()
        stats["total_links_created"] += 1
        stats["last_shortlink"] = short_url
        stats["last_target_url"] = target_url
        stats["last_activity_time"] = time.time()
        stats["last_activity_str"] = get_current_time_str(stats["last_activity_time"])
        _save_stats_unlocked(stats)

def record_view_result(success, short_url, dest_url, country="🇺🇸 US"):
    with stats_lock:
        stats = _load_stats_unlocked()
        if success:
            stats["total_views_completed"] += 1
        else:
            stats["failed_views"] += 1
            
        total_attempts = stats["total_views_completed"] + stats["failed_views"]
        if total_attempts > 0:
            pct = (stats["total_views_completed"] / total_attempts) * 100.0
            stats["success_rate"] = f"{pct:.1f}%"
        else:
            stats["success_rate"] = "100%"
            
        stats["last_activity_time"] = time.time()
        stats["last_activity_str"] = get_current_time_str(stats["last_activity_time"])
        stats["last_visitor_country"] = country
        
        entry = {
            "time": get_current_time_str(),
            "shortlink": short_url,
            "country": country,
            "status": "SUCCESS" if success else "FAILED",
            "dest": dest_url
        }
        history = stats.get("history", [])
        history.insert(0, entry)
        stats["history"] = history[:10]
        _save_stats_unlocked(stats)

def record_dashboard_stats(dashboard_data):
    with stats_lock:
        stats = _load_stats_unlocked()
        now = time.time()
        next_check = now + HOURLY_CHECK_INTERVAL
        stats["dashboard"] = {
            "views": dashboard_data.get("views", "0"),
            "earnings": dashboard_data.get("earnings", "$0.00"),
            "ecpm": dashboard_data.get("ecpm", "$0.00"),
            "available": dashboard_data.get("available", "$0.00"),
            "last_checked_at": now,
            "last_checked_str": get_current_time_str(now),
            "next_check_at": next_check,
            "next_check_str": get_current_time_str(next_check)
        }
        _save_stats_unlocked(stats)

# ------------------------------------------------------------------------------
# 3. AUTHENTICATION & SESSION MANAGEMENT (ouo_login.py)
# ------------------------------------------------------------------------------

def check_session_validity(headless=True):
    """Verifies whether saved session cookies can access the dashboard"""
    if not PLAYWRIGHT_AVAILABLE:
        print("[-] Error: 'playwright' is not installed. Run: pip install -r requirements.txt && playwright install chromium")
        return False
    if not os.path.exists(SESSION_FILE):
        return False
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=DEFAULT_CHROMIUM_ARGS,
                ignore_default_args=['--enable-automation']
            )
            context = browser.new_context(
                proxy=PROXY_CONFIG,
                user_agent=USER_AGENT,
                viewport={'width': 1280, 'height': 720}
            )
            with open(SESSION_FILE) as f:
                cookie_dict = json.load(f)
            context.add_cookies([{'name': k, 'value': v, 'domain': '.ouo.io', 'path': '/'} for k, v in cookie_dict.items()])
            page = context.new_page()
            page.goto('https://ouo.io/manage/home', wait_until='domcontentloaded', timeout=30000)
            
            for _ in range(15):
                time.sleep(1)
                if 'manage/home' in page.url or 'Dashboard' in page.title():
                    break
                    
            is_valid = ('manage/home' in page.url or 'Dashboard' in page.title()) and 'auth/signin' not in page.url
            browser.close()
            return is_valid
    except Exception as e:
        print(f"[!] Session validity check error: {e}")
        return False

def do_login(headless=True):
    """Executes fresh login and saves persistent cookies"""
    if not PLAYWRIGHT_AVAILABLE:
        print("[-] Error: 'playwright' is not installed. Run: pip install -r requirements.txt && playwright install chromium")
        return False
    print("[*] Logging in to ouo.io with proxy...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=DEFAULT_CHROMIUM_ARGS,
                ignore_default_args=['--enable-automation']
            )
            context = browser.new_context(
                proxy=PROXY_CONFIG,
                user_agent=USER_AGENT,
                viewport={'width': 1280, 'height': 720}
            )
            context.add_init_script('''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            ''')
            page = context.new_page()
            print("[*] Opening signin page with residential proxy...")
            try:
                page.goto('https://ouo.io/auth/signin', wait_until='domcontentloaded', timeout=35000)
            except Exception as nav_e:
                print(f"[!] Navigation notice: {nav_e}")
            
            for i in range(15):
                time.sleep(1)
                try:
                    title = page.title()
                except Exception:
                    title = ""
                if 'Sign in' in title or ('Just a moment' not in title and title != ''):
                    break
                    
            try:
                cur_title = page.title()
            except Exception:
                cur_title = ""
                
            if 'Sign in' not in cur_title and 'Just a moment' in cur_title:
                print(f"[-] Could not reach sign in page (Challenge page active). Current title: {cur_title}")
                browser.close()
                return False
                
            print("[*] Submitting credentials...")
            page.fill('input[name="username"]', OUO_USERNAME)
            page.fill('input[name="password"]', OUO_PASSWORD)
            
            remember = page.query_selector('input[name="remember-me"]')
            if remember and not remember.is_checked():
                remember.check()
                
            page.click('button[type="submit"]')
            
            for _ in range(15):
                time.sleep(1)
                if '/manage/home' in page.url or 'Dashboard' in page.title():
                    break
                    
            if '/manage/home' in page.url or 'Dashboard' in page.title():
                cookies = context.cookies()
                auth_cookies = {c['name']: c['value'] for c in cookies}
                os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
                with open(SESSION_FILE, 'w') as f:
                    json.dump(auth_cookies, f, indent=2)
                browser.close()
                print(f"[🎉 SUCCESS] Login successful! Session saved to: {SESSION_FILE}")
                return True
            else:
                print(f"[-] Login failed. Redirected to: {page.url}")
                browser.close()
                return False
    except Exception as e:
        print(f"[!] Login exception: {e}")
        return False

def ensure_valid_session():
    """Smart Guard: Skips login if cookies are already valid"""
    print("[*] Checking session health...")
    if check_session_validity():
        print("[+] Active session cookie found (skipping login).")
        return True
    else:
        print("[!] Session expired or not found. Performing fresh login...")
        return do_login()

# ------------------------------------------------------------------------------
# 4. HOURLY DASHBOARD CHECKER
# ------------------------------------------------------------------------------

def check_ouo_dashboard_hourly(force=False):
    """Fetches account views, earnings, and eCPM from ouo.io dashboard"""
    if not PLAYWRIGHT_AVAILABLE:
        return
    stats = load_stats()
    last_check = stats.get("dashboard", {}).get("last_checked_at", 0)
    now = time.time()
    
    if not force and (now - last_check) < HOURLY_CHECK_INTERVAL and last_check > 0:
        return
        
    print("[*] Scraping hourly ouo.io dashboard snapshot...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=DEFAULT_CHROMIUM_ARGS,
                ignore_default_args=['--enable-automation']
            )
            context = browser.new_context(proxy=PROXY_CONFIG, user_agent=USER_AGENT, viewport={'width': 1280, 'height': 720})
            context.add_init_script('''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            ''')
            if os.path.exists(SESSION_FILE):
                with open(SESSION_FILE) as f:
                    cookie_dict = json.load(f)
                context.add_cookies([{'name': k, 'value': v, 'domain': '.ouo.io', 'path': '/'} for k, v in cookie_dict.items()])
                
            page = context.new_page()
            page.goto('https://ouo.io/manage/home', wait_until='domcontentloaded', timeout=35000)
            
            for _ in range(15):
                time.sleep(1)
                if 'manage/home' in page.url or 'Dashboard' in page.title():
                    break

            if 'auth/signin' in page.url or 'sign in' in page.title().lower():
                print("[*] Dashboard check detected expired session. Logging in directly on page...")
                try:
                    page.fill('input[name="username"]', OUO_USERNAME)
                    page.fill('input[name="password"]', OUO_PASSWORD)
                    remember = page.query_selector('input[name="remember-me"]')
                    if remember and not remember.is_checked():
                        remember.check()
                    page.click('button[type="submit"]')
                    for _ in range(15):
                        time.sleep(1)
                        if '/manage/home' in page.url or 'Dashboard' in page.title():
                            break
                    if '/manage/home' in page.url or 'Dashboard' in page.title():
                        cookies = context.cookies()
                        auth_cookies = {c['name']: c['value'] for c in cookies}
                        with open(SESSION_FILE, 'w') as f:
                            json.dump(auth_cookies, f, indent=2)
                        print("[+] Relogin successful during dashboard check!")
                except Exception as login_err:
                    print(f"[!] Relogin attempt error: {login_err}")

            dash_data = {'views': '0', 'earnings': '$0.00', 'ecpm': '$0.00', 'available': '$0.00'}
            cards = page.query_selector_all('.col-sm-3, .col-xs-6, .panel')
            for c in cards:
                lines = [l.strip() for l in c.inner_text().split('\n') if l.strip()]
                if len(lines) >= 2:
                    val, label = lines[0], lines[1]
                    if label.lower() == 'views':
                        dash_data['views'] = val
                    elif label.lower() == 'earnings':
                        dash_data['earnings'] = val
                    elif label.lower() == 'ecpm':
                        dash_data['ecpm'] = val
                    elif 'total available' in label.lower():
                        dash_data['available'] = val
            browser.close()
            
            # Don't overwrite with 0 if previous valid stats existed and scrape was empty
            old_dash = stats.get("dashboard", {})
            if dash_data['views'] == '0' and old_dash.get('views', '0') != '0':
                dash_data['views'] = old_dash.get('views')
                dash_data['earnings'] = old_dash.get('earnings')
                dash_data['ecpm'] = old_dash.get('ecpm')
                dash_data['available'] = old_dash.get('available')
                
            record_dashboard_stats(dash_data)
            print(f"[+] Hourly Check Complete: Views={dash_data['views']}, Earnings={dash_data['earnings']}, Available={dash_data['available']}")
    except Exception as e:
        print(f"[!] Hourly dashboard check notice: {e}")

# ------------------------------------------------------------------------------
# 5. DYNAMIC TARGET URL GENERATOR & LINK SHORTENER (ouo_shorten.py)
# ------------------------------------------------------------------------------

def get_dynamic_url(vercel_base_url=None):
    base = (vercel_base_url or DEFAULT_VERCEL_APP).rstrip('/')
    if REQUESTS_AVAILABLE:
        try:
            res = requests.get(f"{base}/api/link", params={'auth': MASTER_PASSWORD}, timeout=8)
            if res.status_code == 200:
                data = res.json()
                if data.get('url'):
                    return data['url']
        except Exception:
            pass
    rand_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    rand_ts = int(time.time() * 1000)
    cat = random.choice(['tech', 'cloud', 'dev', 'ai', 'crypto', 'quantum', 'fintech'])
    return f"{base}/article?id={rand_id}&cat={cat}&t={rand_ts}&auth={urllib.parse.quote(MASTER_PASSWORD)}&ref=standalone"

def shorten_target(target_url, cookies_path=None):
    """Shortens target URL using stored authenticated cookies and CSRF token"""
    if not PLAYWRIGHT_AVAILABLE:
        print("[-] Error: 'playwright' is not installed. Run: pip install -r requirements.txt && playwright install chromium")
        return None
    c_path = cookies_path or SESSION_FILE
    with shortener_lock:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=DEFAULT_CHROMIUM_ARGS,
                    ignore_default_args=['--enable-automation']
                )
                context = browser.new_context(
                    proxy=PROXY_CONFIG,
                    user_agent=USER_AGENT,
                    viewport={'width': 1280, 'height': 720}
                )
                context.add_init_script('''
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    window.chrome = { runtime: {} };
                ''')
                if os.path.exists(c_path):
                    with open(c_path) as f:
                        cookie_dict = json.load(f)
                    context.add_cookies([{'name': k, 'value': v, 'domain': '.ouo.io', 'path': '/'} for k, v in cookie_dict.items()])
                    
                page = context.new_page()
                page.goto('https://ouo.io/manage/home', wait_until='domcontentloaded', timeout=30000)
                
                for _ in range(15):
                    time.sleep(1)
                    if 'Just a moment' not in page.title() and page.title() != '':
                        break
                        
                token_el = page.query_selector('input[name="_token"]')
                csrf_token = token_el.get_attribute('value') if token_el else ""
                
                result = page.evaluate('''async ({url, token}) => {
                    const formData = new URLSearchParams();
                    formData.append('_token', token);
                    formData.append('url', url);
                    
                    const res = await fetch('https://ouo.io/shorten', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                            'X-Requested-With': 'XMLHttpRequest',
                            'Accept': 'application/json, text/javascript, */*; q=0.01'
                        },
                        body: formData.toString()
                    });
                    const raw = await res.text();
                    let data = null;
                    try { data = JSON.parse(raw); } catch(e) { data = raw; }
                    return { status: res.status, data };
                }''', {'url': target_url, 'token': csrf_token})
                
                browser.close()
                status = result.get('status')
                data = result.get('data')
                if status == 200 and isinstance(data, dict) and not data.get('error'):
                    slug = data.get('slug')
                    return f"https://ouo.io/{slug}"
                else:
                    return None
        except Exception as e:
            print(f"[!] Shorten error: {e}")
            return None

# ------------------------------------------------------------------------------
# 6. NATURAL AD-ENABLED VISITOR ENGINE (ouo_visitor.py)
# ------------------------------------------------------------------------------

def visit_shortlink(short_url, worker_id=1, proxy=None):
    """Simulates realistic human visit with natural ad scrolling & timer bypass"""
    if not PLAYWRIGHT_AVAILABLE:
        print("[-] Error: 'playwright' is not installed. Run: pip install -r requirements.txt && playwright install chromium")
        return False, "Playwright not installed"
    use_proxy = proxy or VISITOR_PROXY_CONFIG
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=DEFAULT_CHROMIUM_ARGS,
                ignore_default_args=['--enable-automation']
            )
            context = browser.new_context(
                proxy=use_proxy,
                user_agent=USER_AGENT,
                viewport={'width': 1280, 'height': 720}
            )
            context.add_init_script('''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
            ''')
            
            page = context.new_page()
            # Ultra Bandwidth Saver 1: Close ad popups immediately before they download video/banners
            def handle_ad_popup(pop):
                try:
                    pop.close()
                except Exception:
                    pass
            page.on("popup", handle_ad_popup)
            context.on("page", lambda p: handle_ad_popup(p) if p != page else None)

            # Ultra Bandwidth Saver 2: Block images, media, fonts, and heavy ad video networks
            def ultra_bandwidth_filter(route):
                try:
                    req = route.request
                    if 'ouo.io' in req.url or 'ouo.press' in req.url or 'cloudflare.com' in req.url:
                        return route.continue_()
                    if req.resource_type in ["image", "media", "font"]:
                        return route.abort()
                    url_l = req.url.lower()
                    heavy_ad_hosts = [
                        "doubleclick", "googlesyndication", "popads", "adcash",
                        "exoclick", "propellerads", "adsterra", "trafficjunky",
                        "adnxs", "creativecdn", "adservice", "analytics", ".mp4", ".webm",
                        "video", "vast", "banner", "cpmstar"
                    ]
                    if any(h in url_l for h in heavy_ad_hosts):
                        return route.abort()
                    return route.continue_()
                except Exception:
                    pass

            try:
                page.route("**/*", ultra_bandwidth_filter)
            except Exception:
                pass
            
            try:
                page.goto(short_url, wait_until='domcontentloaded', timeout=25000)
            except Exception:
                page.goto(short_url, wait_until='commit', timeout=25000)
            
            for _ in range(15):
                try:
                    t = page.title()
                    if 'Just a moment' not in t and t != '':
                        break
                except Exception:
                    break
                time.sleep(1)
                    
            # Natural scrolling
            try:
                scroll_y = random.randint(200, 450)
                page.mouse.wheel(0, scroll_y)
                time.sleep(random.uniform(0.8, 1.6))
            except Exception:
                pass
                
            # Step 1: Click "I'M A HUMAN" and ensure transition to /go/
            for _ in range(3):
                try:
                    cur_u = page.url
                    if '/go/' in cur_u or ('ouo.io' not in cur_u and 'ouo.press' not in cur_u):
                        break
                except Exception:
                    break
                try:
                    btn1 = page.wait_for_selector('#btn-main', timeout=6000)
                    if btn1:
                        time.sleep(random.uniform(0.6, 1.2))
                        btn1.click(force=True, timeout=4000)
                except Exception:
                    pass
                    
                try:
                    page.wait_for_url(lambda u: '/go/' in u or ('ouo.io' not in u and 'ouo.press' not in u), timeout=6000)
                except Exception:
                    # Fallback to direct form submit if ad intercepted click
                    try:
                        page.evaluate('() => { const f = document.querySelector("#form-captcha") || document.querySelector("form"); if (f) f.submit(); }')
                        page.wait_for_url(lambda u: '/go/' in u or ('ouo.io' not in u and 'ouo.press' not in u), timeout=5000)
                    except Exception:
                        pass
                try:
                    page.bring_to_front()
                except Exception:
                    pass
            
            # Step 2: On /go/ page, wait for countdown and click GET LINK
            try:
                is_go_page = '/go/' in page.url
            except Exception:
                is_go_page = False
                
            if is_go_page:
                for _ in range(15):
                    time.sleep(1)
                    try:
                        if 'ouo.io' not in page.url and 'ouo.press' not in page.url:
                            break
                        btn2 = page.query_selector('#btn-main')
                        if btn2:
                            cls = btn2.get_attribute('class') or ''
                            if 'disabled' not in cls:
                                time.sleep(random.uniform(0.6, 1.2))
                                try:
                                    btn2.click(force=True, timeout=5000)
                                except Exception:
                                    pass
                                break
                    except Exception:
                        continue
            
            # Wait for destination navigation with fallback
            try:
                final_url = page.url
            except Exception:
                final_url = ""
            for i in range(15):
                time.sleep(1)
                try:
                    cur_u = page.url
                    if 'ouo.io' not in cur_u and 'ouo.press' not in cur_u:
                        final_url = cur_u
                        time.sleep(random.uniform(1.2, 2.0))
                        break
                    if i >= 3 and ('/go/' in cur_u):
                        try:
                            page.evaluate('() => { const f = document.querySelector("#form-go"); if (f) f.submit(); }')
                        except Exception:
                            pass
                except Exception:
                    continue
                    
            try:
                page.unroute("**/*")
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
            is_valid = 'ouo.io' not in final_url and 'ouo.press' not in final_url and 'chrome-error' not in final_url
            return is_valid, final_url
    except Exception as e:
        return False, str(e)

# ------------------------------------------------------------------------------
# 7. CAMPAIGN LOOP (ouo_campaign.py)
# ------------------------------------------------------------------------------

def run_campaign_loop(total_views=1, vercel_url=None):
    print("\n=======================================================")
    print("🚀 STARTING COMPLETE CAMPAIGN AUTOMATION")
    print(f"🎯 Total Views Goal: {total_views}")
    print(f"🌐 Dynamic Portal: {vercel_url or DEFAULT_VERCEL_APP}")
    print("=======================================================\n")
    
    if not ensure_valid_session():
        print("[-] Failed to establish valid session. Aborting.")
        return False
        
    successful = 0
    for cycle in range(1, total_views + 1):
        print(f"\n>>> [CYCLE {cycle}/{total_views}] Generating Dynamic Target URL...")
        dynamic_url = get_dynamic_url(vercel_url)
        print(f"[+] Unique Target: {dynamic_url}")
        
        print(f">>> [CYCLE {cycle}/{total_views}] Shortening with ouo.io...")
        short_link = shorten_target(dynamic_url)
        if not short_link:
            if not check_session_validity():
                do_login()
                short_link = shorten_target(dynamic_url)
                
        if not short_link:
            print("[-] Skipping cycle due to shorten failure.")
            continue
            
        print(f"[+] Generated Shortlink: {short_link}")
        record_link_created(short_link, dynamic_url)
        
        print(f">>> [CYCLE {cycle}/{total_views}] Visiting shortlink with ads...")
        ok, dest = visit_shortlink(short_link)
        record_view_result(ok, short_link, dest)
        if ok:
            successful += 1
            print(f"[🎉 CYCLE {cycle} SUCCESS] Landed on: {dest}")
        else:
            print(f"[!] Cycle {cycle} failed: {dest}")
            
        if cycle < total_views:
            w = random.randint(5, 10)
            print(f"[*] Sleeping {w}s before next cycle...")
            time.sleep(w)
            
    print("\n=======================================================")
    print(f"🏁 CAMPAIGN COMPLETED: {successful}/{total_views} Views Delivered!")
    print("=======================================================\n")
    return successful

# ------------------------------------------------------------------------------
# 8. 24/7 MULTI-WORKER DAEMON (ouo_daemon.py)
# ------------------------------------------------------------------------------

# Hardcoded 3 Fixed Links requested by user: Workers rotate exclusively across these 3 links
HARDCODED_LINKS = os.environ.get(
    "OUO_HARDCODED_LINKS",
    "https://ouo.io/TixC7D,https://ouo.io/q232K7,https://ouo.io/90phq1"
).split(",")
HARDCODED_LINKS = [l.strip() for l in HARDCODED_LINKS if l.strip()]

def get_shortlink_from_pool():
    """Returns one of the hardcoded 3 links randomly without generating new ones"""
    if not HARDCODED_LINKS:
        return None
    return random.choice(HARDCODED_LINKS)

def worker_routine(worker_id):
    print(f"[Worker #{worker_id}] Initialized and active.")
    while daemon_running:
        try:
            short_url = get_shortlink_from_pool()
            if not short_url:
                time.sleep(4)
                continue

            proxy_cfg, country_label = get_rotating_visitor_proxy()
            ok, dest = visit_shortlink(short_url, worker_id=worker_id, proxy=proxy_cfg)
            record_view_result(ok, short_url, dest, country=f"{country_label} (Rotating Residential)")

            if ok:
                print(f"[Worker #{worker_id}] ✅ Paid View Verified ({country_label}) on {short_url}!")
            else:
                print(f"[Worker #{worker_id}] ⚠️ View ended on {short_url}: {dest[:45]}...")

            worker_sleep = random.uniform(3.0, 10.0)
            time.sleep(worker_sleep)
        except Exception as e:
            print(f"[Worker #{worker_id}] Error: {e}")
            time.sleep(6)

def run_multi_worker_daemon(num_workers=NUM_WORKERS, blocking=True):
    global daemon_running
    daemon_running = True
    
    print("=======================================================")
    print("🚀 OUO.IO 24/7 MULTI-WORKER ENGINE (STANDALONE)")
    print(f"👥 Concurrent Workers: {num_workers} Parallel Playwright Instances")
    print("📡 Visitor Proxy: US Residential Rotating (res-us-rotate)")
    print(f"🔗 Fixed Hardcoded Links: {', '.join(HARDCODED_LINKS)}")
    print("⏱️ Dashboard Check: Hourly Cached Snapshot")
    print("=======================================================\n")
    
    stats = load_stats()
    stats["daemon_status"] = f"🟢 RUNNING 24/7 ({num_workers} US Workers)"
    if not stats.get("daemon_started_at"):
        stats["daemon_started_at"] = time.time()
        stats["daemon_started_str"] = get_current_time_str(stats["daemon_started_at"])
    save_stats(stats)
    
    def session_and_hourly_worker():
        time.sleep(15)
        try:
            if not check_session_validity():
                do_login()
        except Exception as e:
            print(f"[!] Session initialization error: {e}")
        try:
            check_ouo_dashboard_hourly()
        except Exception as e:
            print(f"[!] Initial dashboard check error: {e}")
            
        while daemon_running:
            time.sleep(60)
            try:
                check_ouo_dashboard_hourly()
            except Exception:
                pass
    threading.Thread(target=session_and_hourly_worker, daemon=True).start()
    
    threads = []
    for w_id in range(1, num_workers + 1):
        t = threading.Thread(target=worker_routine, args=(w_id,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(random.uniform(2.0, 3.5))
        
    print(f"[+] All {num_workers} Workers successfully dispatched! Running 24/7.")
    
    if blocking:
        def handle_exit(sig, frame):
            global daemon_running
            print("\n[!] Stopping daemon workers...")
            daemon_running = False
            s = load_stats()
            s["daemon_status"] = "⏸️ STOPPED"
            save_stats(s)
            sys.exit(0)
            
        signal.signal(signal.SIGINT, handle_exit)
        signal.signal(signal.SIGTERM, handle_exit)
        
        while daemon_running:
            time.sleep(1)

# ------------------------------------------------------------------------------
# 9. TELEGRAM BOT ENGINE (ouo_bot.py)
# ------------------------------------------------------------------------------

def run_telegram_bot():
    if not TELEBOT_AVAILABLE:
        print("[-] Error: 'pyTelegramBotAPI' package not installed. Run: pip install pyTelegramBotAPI")
        sys.exit(1)
        
    bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
    active_status_messages = {}
    
    def is_user_authenticated(chat_id):
        if not os.path.exists(AUTH_FILE):
            return False
        try:
            with open(AUTH_FILE, 'r') as f:
                data = json.load(f)
            auth_time = data.get(str(chat_id), 0)
            return (time.time() - auth_time) < SESSION_LIFETIME
        except Exception:
            return False

    def set_user_authenticated(chat_id):
        data = {}
        if os.path.exists(AUTH_FILE):
            try:
                with open(AUTH_FILE, 'r') as f:
                    data = json.load(f)
            except Exception:
                pass
        data[str(chat_id)] = time.time()
        os.makedirs(os.path.dirname(AUTH_FILE), exist_ok=True)
        with open(AUTH_FILE, 'w') as f:
            json.dump(data, f, indent=2)

    def safe_delete(chat_id, message_id):
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass

    def safe_edit(chat_id, message_id, text, reply_markup=None):
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup, disable_web_page_preview=True)
            return True
        except ApiTelegramException as e:
            if "message is not modified" in str(e).lower():
                return True
            return False
        except Exception:
            return False

    def build_status_card_text():
        stats = load_stats()
        daemon_badge = stats.get("daemon_status", "⏸️ STOPPED")
        dash = stats.get("dashboard", {})
        dash_views = dash.get("views", "0")
        dash_earnings = dash.get("earnings", "$0.00")
        dash_ecpm = dash.get("ecpm", "$0.00")
        dash_available = dash.get("available", "$0.00")
        last_check_str = dash.get("last_checked_str", "Pending")
        next_check_str = dash.get("next_check_str", "In 1 Hour")
        
        links_created = stats.get("total_links_created", 0)
        views_completed = stats.get("total_views_completed", 0)
        failed_views = stats.get("failed_views", 0)
        success_rate = stats.get("success_rate", "100%")
        last_link = stats.get("last_shortlink", "None yet")
        last_act = stats.get("last_activity_str", "None yet")
        
        return (
            "📊 <b>ROX AI — 24/7 Earning & Automation Dashboard</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ <b>24/7 Engine Status:</b> <b>{daemon_badge}</b>\n"
            f"🔗 <b>Total Links Generated:</b> <code>{links_created}</code> links\n"
            f"✅ <b>Automated Views (Success):</b> <code>{views_completed}</code> paid views\n"
            f"⚠️ <b>Failed Attempts:</b> <code>{failed_views}</code>\n"
            f"🎯 <b>Success Delivery Rate:</b> <b>{success_rate}</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "💰 <b>ouo.io Live Account Stats:</b>\n"
            f"  • 👁️ <b>Account Views:</b> <b>{dash_views}</b>\n"
            f"  • 💵 <b>Total Earnings:</b> <b>{dash_earnings}</b>\n"
            f"  • 📈 <b>Current eCPM:</b> <b>{dash_ecpm}</b>\n"
            f"  • 💳 <b>Available Balance:</b> <b>{dash_available}</b>\n"
            f"  • ⏱️ <b>Last Check:</b> <code>{last_check_str}</code>\n"
            f"  • ⏳ <b>Next Check:</b> <code>{next_check_str}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🌐 <b>Dynamic Portal:</b> <a href=\"{DEFAULT_VERCEL_APP}\">ouo-portal.vercel.app</a>\n"
            "📡 <b>Visitor Network:</b> 🇺🇸 US Residential (Auto-Rotating IPs)\n"
            f"🔗 <b>Latest Shortlink:</b> <code>{last_link}</code>\n"
            f"🕒 <b>Last Activity:</b> <code>{last_act}</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<i>24/7 background earning active. All stats synced.</i>"
        )

    def get_status_inline_markup():
        markup = types.InlineKeyboardMarkup(row_width=2)
        b_refresh = types.InlineKeyboardButton(text="🔄 Refresh Status", callback_data="cb_refresh_status")
        b_portal = types.InlineKeyboardButton(text="🌐 Vercel Portal", url=DEFAULT_VERCEL_APP)
        b_force_check = types.InlineKeyboardButton(text="💰 Sync Earnings Now", callback_data="cb_force_check")
        b_health = types.InlineKeyboardButton(text="🔑 Session Health", callback_data="cb_health_check")
        markup.row(b_refresh, b_portal)
        markup.row(b_force_check, b_health)
        return markup

    def send_or_update_status_card(chat_id, edit_msg_id=None):
        text = build_status_card_text()
        markup = get_status_inline_markup()
        if edit_msg_id and safe_edit(chat_id, edit_msg_id, text, reply_markup=markup):
            return
        prev_msg_id = active_status_messages.get(chat_id)
        if prev_msg_id and safe_edit(chat_id, prev_msg_id, text, reply_markup=markup):
            return
        sent = bot.send_message(chat_id, text, reply_markup=markup, disable_web_page_preview=True)
        active_status_messages[chat_id] = sent.message_id

    def prompt_for_password(chat_id, original_msg_id=None):
        if original_msg_id:
            safe_delete(chat_id, original_msg_id)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(text="❌ Cancel", callback_data="cb_cancel_auth"))
        lock_msg = bot.send_message(
            chat_id,
            "🔒 <b>Security Verification Required (24-Hour Access)</b>\n\n"
            "Please enter the Master Security Password to access the control center.\n\n"
            "🔑 <b>Enter Password:</b>",
            reply_markup=markup
        )
        bot.register_next_step_handler(lock_msg, process_password_step)

    def process_password_step(message):
        chat_id = message.chat.id
        user_pass = (message.text or "").strip()
        safe_delete(chat_id, message.message_id)
        if user_pass == MASTER_PASSWORD:
            set_user_authenticated(chat_id)
            send_or_update_status_card(chat_id)
        else:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(text="🔄 Try Again", callback_data="cb_reauth"))
            bot.send_message(chat_id, "❌ <b>Access Denied: Incorrect Password!</b>", reply_markup=markup)

    @bot.message_handler(commands=['start', 'status', 'help'])
    def cmd_start(message):
        chat_id = message.chat.id
        safe_delete(chat_id, message.message_id)
        if not is_user_authenticated(chat_id):
            prompt_for_password(chat_id)
            return
        send_or_update_status_card(chat_id)

    @bot.message_handler(func=lambda m: True)
    def handle_all_messages(message):
        chat_id = message.chat.id
        safe_delete(chat_id, message.message_id)
        if not is_user_authenticated(chat_id):
            prompt_for_password(chat_id)
            return
        send_or_update_status_card(chat_id)

    @bot.callback_query_handler(func=lambda call: True)
    def handle_callbacks(call):
        chat_id = call.message.chat.id
        data = call.data
        if data == "cb_cancel_auth":
            bot.clear_step_handler_by_chat_id(chat_id)
            safe_edit(chat_id, call.message.message_id, "❌ <b>Verification Cancelled.</b>")
            bot.answer_callback_query(call.id, "Cancelled")
            return
        if data == "cb_reauth":
            bot.answer_callback_query(call.id)
            prompt_for_password(chat_id, original_msg_id=call.message.message_id)
            return
        if not is_user_authenticated(chat_id):
            bot.answer_callback_query(call.id, "Master Password required!")
            prompt_for_password(chat_id)
            return
        if data == "cb_refresh_status":
            bot.answer_callback_query(call.id, "Refreshing... ⚡")
            def bg_quick_refresh():
                check_ouo_dashboard_hourly()
                send_or_update_status_card(chat_id, edit_msg_id=call.message.message_id)
            threading.Thread(target=bg_quick_refresh, daemon=True).start()
        elif data == "cb_force_check":
            bot.answer_callback_query(call.id, "Syncing live ouo.io stats...")
            def bg_check():
                check_ouo_dashboard_hourly(force=True)
                send_or_update_status_card(chat_id, edit_msg_id=call.message.message_id)
            threading.Thread(target=bg_check, daemon=True).start()
        elif data == "cb_health_check":
            bot.answer_callback_query(call.id, "Session health checked!")
            send_or_update_status_card(chat_id, edit_msg_id=call.message.message_id)

    def auto_report_loop():
        print(f"[*] Hourly Auto-Report Daemon started for Admin: {ADMIN_CHAT_ID} (Every 1 Hour)")
        while True:
            time.sleep(3600)  # Exactly every 1 hour
            try:
                check_ouo_dashboard_hourly(force=True)
                text = (
                    "🔔 <b>ROX AI — Hourly Auto Earning Report</b>\n" +
                    build_status_card_text().replace("📊 <b>ROX AI — 24/7 Earning & Automation Dashboard</b>\n", "")
                )
                markup = get_status_inline_markup()
                for tid in {str(ADMIN_CHAT_ID), "8321630022"}:
                    try:
                        bot.send_message(tid, text, reply_markup=markup, disable_web_page_preview=True)
                        print(f"[+] Hourly Auto Earning Report delivered to {tid}!")
                    except Exception as err:
                        print(f"[!] Hourly Auto report delivery error to {tid}: {err}")
            except Exception as e:
                print(f"[!] Error in hourly auto-report thread: {e}")

    threading.Thread(target=auto_report_loop, daemon=True).start()

    print(f"[*] Starting Telegram Bot with token: {BOT_TOKEN[:10]}...")
    try:
        me = bot.get_me()
        print(f"[+] Bot Online: @{me.username} ({me.first_name})")
    except Exception as e:
        print(f"[!] Warning connecting to Telegram: {e}")

    while True:
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=15)
        except Exception as e:
            print(f"[!] Bot polling error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# ------------------------------------------------------------------------------
# 10. CLI ARGUMENT PARSER & MAIN ENTRYPOINT
# ------------------------------------------------------------------------------

def print_status_terminal():
    stats = load_stats()
    print("\n" + "="*50)
    print("📊 CURRENT OUO AUTOMATION STATS")
    print("="*50)
    print(f"Status:             {stats.get('daemon_status')}")
    print(f"Links Generated:    {stats.get('total_links_created')}")
    print(f"Completed Views:    {stats.get('total_views_completed')}")
    print(f"Failed Views:       {stats.get('failed_views')}")
    print(f"Success Rate:       {stats.get('success_rate')}")
    print(f"Last Shortlink:     {stats.get('last_shortlink')}")
    print(f"Last Activity:      {stats.get('last_activity_str')}")
    dash = stats.get("dashboard", {})
    print("-"*50)
    print(f"Dashboard Views:    {dash.get('views')}")
    print(f"Dashboard Earnings: {dash.get('earnings')}")
    print(f"Dashboard eCPM:     {dash.get('ecpm')}")
    print(f"Available Balance:  {dash.get('available')}")
    print(f"Last Checked:       {dash.get('last_checked_str')}")
    print("="*50 + "\n")

def main():
    parser = argparse.ArgumentParser(
        description="ouo.io 24/7 Automation & Telegram Control Center (Standalone Engine)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # daemon
    sub_daemon = subparsers.add_parser("daemon", help="Run 24/7 multi-worker earning daemon")
    sub_daemon.add_argument("-w", "--workers", type=int, default=NUM_WORKERS, help="Number of concurrent workers (default: 4)")
    
    # bot
    subparsers.add_parser("bot", help="Run Telegram control bot")
    
    # all (bot + daemon)
    sub_all = subparsers.add_parser("all", help="Run BOTH multi-worker daemon and Telegram bot concurrently")
    sub_all.add_argument("-w", "--workers", type=int, default=NUM_WORKERS, help="Number of concurrent workers (default: 4)")
    
    # campaign
    sub_campaign = subparsers.add_parser("campaign", help="Run end-to-end campaign loop for N views")
    sub_campaign.add_argument("views", type=int, nargs="?", default=1, help="Total views to deliver (default: 1)")
    sub_campaign.add_argument("--url", type=str, default=None, help="Vercel portal URL")
    
    # login
    subparsers.add_parser("login", help="Run login and save session cookies")
    
    # shorten
    sub_shorten = subparsers.add_parser("shorten", help="Shorten a specific URL")
    sub_shorten.add_argument("url", type=str, nargs="?", default="https://ouo-portal.vercel.app", help="URL to shorten")
    
    # visit
    sub_visit = subparsers.add_parser("visit", help="Visit and bypass a shortlink")
    sub_visit.add_argument("url", type=str, help="Shortlink URL to visit")
    
    # status
    subparsers.add_parser("status", help="Display current stats in terminal")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if args.command == "daemon":
        run_multi_worker_daemon(num_workers=args.workers, blocking=True)
    elif args.command == "bot":
        run_telegram_bot()
    elif args.command == "all":
        # Launch daemon non-blocking in background thread, then run bot in main thread
        d_thread = threading.Thread(target=run_multi_worker_daemon, kwargs={"num_workers": args.workers, "blocking": False}, daemon=True)
        d_thread.start()
        run_telegram_bot()
    elif args.command == "campaign":
        run_campaign_loop(total_views=args.views, vercel_url=args.url)
    elif args.command == "login":
        success = do_login(headless=True)
        print("[+] Login result:", "Success" if success else "Failed")
    elif args.command == "shorten":
        short = shorten_target(args.url)
        print(f"[+] Result: {short}")
    elif args.command == "visit":
        ok, dest = visit_shortlink(args.url)
        print(f"[+] Visit result: {'Success' if ok else 'Failed'} -> {dest}")
    elif args.command == "status":
        print_status_terminal()

# ------------------------------------------------------------------------------
# 11. RAILWAY / CLOUD HEALTH SERVER
# ------------------------------------------------------------------------------

def run_health_server():
    """Lightweight HTTP healthcheck for Railway, Render, Koyeb, Fly.io, etc."""
    port_str = os.environ.get("PORT")
    if not port_str:
        return
    try:
        port = int(port_str)
        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                stats = load_stats()
                resp = json.dumps({
                    "status": "online",
                    "workers": NUM_WORKERS,
                    "views": stats.get("total_views_completed", 0),
                    "earnings": stats.get("dashboard", {}).get("earnings", "$0.00"),
                    "service": "ouo-standalone",
                    "time": get_current_time_str()
                })
                self.wfile.write(resp.encode('utf-8'))
            def log_message(self, format, *args):
                pass
        server = HTTPServer(("0.0.0.0", port), HealthHandler)
        print(f"[*] Cloud Health Server active on 0.0.0.0:{port}")
        server.serve_forever()
    except Exception as e:
        print(f"[!] Health Server error: {e}")


if __name__ == '__main__':
    # Auto-detect Railway/Cloud: If no CLI args but PORT is set, run "all" mode automatically
    if len(sys.argv) == 1 and os.environ.get("PORT"):
        print("[*] Cloud environment detected (PORT set). Auto-launching: daemon + bot + health server")
        threading.Thread(target=run_health_server, daemon=True).start()
        d_thread = threading.Thread(target=run_multi_worker_daemon, kwargs={"num_workers": NUM_WORKERS, "blocking": False}, daemon=True)
        d_thread.start()
        run_telegram_bot()
    else:
        main()
