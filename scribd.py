#!/usr/bin/env python3
"""
Khan's Scribd Downloader — Telegram bot.
Production-grade, queue-based, cache-aware, zero-leak.
"""

import os
import re
import sys
import time
import html
import base64
import shutil
import sqlite3
import tempfile
import threading
import traceback
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from pypdf import PdfReader, PdfWriter
import telebot

# ==============================================================
# CONFIG
# ==============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID  = os.environ.get("ADMIN_ID", "").strip()
PORT      = int(os.environ.get("PORT", 8080))

CHROME_BIN        = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")

DB_PATH           = "/app/bot_data.db"
CACHE_DIR         = "/app/cache"
PROFILE_DIR       = "/app/chrome-profile"

MAX_TG_UPLOAD     = 49 * 1024 * 1024
EXPORT_BATCH_SIZE = 4
CDP_TIMEOUT       = 600
JOBS_BEFORE_CHROME_RESET = 30

SUPPORT_URL = "https://buymeacoffee.com/yourusername"  # <-- EDIT THIS

# ==============================================================
# TELEGRAM
# ==============================================================

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ==============================================================
# HEALTH CHECK SERVER
# ==============================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, *a): pass

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    server.serve_forever()

# ==============================================================
# DATABASE
# ==============================================================

class Database:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._init_schema()

    def _conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c

    def _init_schema(self):
        with self._conn() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_downloads INTEGER DEFAULT 0
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value INTEGER DEFAULT 0
                )
            """)
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_users', 0)")
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_jobs', 0)")
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('donation_goal', 7)")
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('donation_raised', 0)")

    def register_user(self, user_id, username, first_name):
        with self.lock:
            with self._conn() as c:
                row = c.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)).fetchone()
                if row:
                    c.execute("UPDATE users SET username = ?, first_name = ? WHERE user_id = ?", (username, first_name, user_id))
                    return False
                c.execute("INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)", (user_id, username, first_name))
                c.execute("UPDATE stats SET value = value + 1 WHERE key = 'total_users'")
                return True

    def increment_jobs(self):
        with self.lock:
            with self._conn() as c:
                c.execute("UPDATE stats SET value = value + 1 WHERE key = 'total_jobs'")

    def increment_user_downloads(self, user_id):
        with self.lock:
            with self._conn() as c:
                c.execute("UPDATE users SET total_downloads = total_downloads + 1 WHERE user_id = ?", (user_id,))

    def get_stats(self):
        with self.lock:
            with self._conn() as c:
                jobs  = c.execute("SELECT value FROM stats WHERE key = 'total_jobs'").fetchone()
                users = c.execute("SELECT value FROM stats WHERE key = 'total_users'").fetchone()
                return (jobs["value"] if jobs else 0), (users["value"] if users else 0)

    def get_user_downloads(self, user_id):
        with self.lock:
            with self._conn() as c:
                row = c.execute("SELECT total_downloads FROM users WHERE user_id = ?", (user_id,)).fetchone()
                return row["total_downloads"] if row else 0

    def get_donations(self):
        with self.lock:
            with self._conn() as c:
                goal = c.execute("SELECT value FROM stats WHERE key = 'donation_goal'").fetchone()
                raised = c.execute("SELECT value FROM stats WHERE key = 'donation_raised'").fetchone()
                return (goal["value"] if goal else 7), (raised["value"] if raised else 0)

    def set_donation(self, key, value):
        with self.lock:
            with self._conn() as c:
                c.execute("UPDATE stats SET value = ? WHERE key = ?", (value, key))

db = Database(DB_PATH)

# ==============================================================
# BROWSER
# ==============================================================

class BrowserManager:
    def __init__(self):
        self.driver = None
        self.lock = threading.Lock()
        self.jobs_since_reset = 0

    def get(self):
        with self.lock:
            if self.driver is None or not self._alive():
                self._launch()
            return self.driver

    def _alive(self):
        try:
            _ = self.driver.current_url
            return True
        except Exception:
            return False

    def _launch(self):
        os.makedirs(PROFILE_DIR, exist_ok=True)
        for attempt in range(3):
            try:
                opts = Options()
                opts.add_argument("--headless=new")
                opts.add_argument("--no-sandbox")
                opts.add_argument("--disable-dev-shm-usage")
                opts.add_argument("--disable-gpu")
                opts.add_argument("--window-size=1400,2000")
                opts.add_argument("--disable-software-rasterizer")
                opts.add_argument("--disable-blink-features=AutomationControlled")
                opts.add_argument("--ignore-certificate-errors")
                opts.add_argument("--force-color-profile=srgb")
                opts.add_argument("--hide-scrollbars")
                opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
                opts.add_experimental_option("excludeSwitches", ["enable-automation"])
                opts.add_experimental_option("useAutomationExtension", False)
                opts.binary_location = CHROME_BIN

                service = Service(executable_path=CHROMEDRIVER_PATH)
                self.driver = webdriver.Chrome(service=service, options=opts)
                self.jobs_since_reset = 0
                print("[browser] Chrome launched successfully")
                return
            except Exception as e:
                print(f"[browser] Launch failed (attempt {attempt+1}): {e}")
                self._kill()
                time.sleep(2)
        raise RuntimeError("Chrome failed to launch after 3 attempts")

    def reset_between_jobs(self):
        if self.driver is None:
            return
        try:
            self.driver.get("about:blank")
            self.driver.delete_all_cookies()
        except Exception:
            self._kill()
            self._launch()

    def maybe_restart(self):
        self.jobs_since_reset += 1
        if self.jobs_since_reset >= JOBS_BEFORE_CHROME_RESET:
            print("[browser] Periodic restart to free memory")
            self._kill()
            self._launch()

    def _kill(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

browser = BrowserManager()

# ==============================================================
# CACHE
# ==============================================================

def cache_path(doc_id):
    return os.path.join(CACHE_DIR, f"{doc_id}.pdf")

def cache_lookup(doc_id):
    p = cache_path(doc_id)
    if os.path.exists(p) and os.path.getsize(p) > 1024:
        return p
    return None

def cache_store(doc_id, src_path):
    os.makedirs(CACHE_DIR, exist_ok=True)
    shutil.copyfile(src_path, cache_path(doc_id))

# ==============================================================
# SCRIBD LOGIC
# ==============================================================

def is_scribd_url(url):
    return bool(re.search(r"scribd\.com/(?:document|doc|read)/\d+", url, re.I))

def extract_doc_id(url):
    m = re.search(r"scribd\.com/(?:document|doc|read)/(\d+)", url)
    if not m:
        raise ValueError("Invalid Scribd URL")
    return m.group(1)

def embed_url_for(url):
    return f"https://www.scribd.com/embeds/{extract_doc_id(url)}/content"

def configure_timeout(driver, seconds):
    try:
        exec_ = driver.command_executor
        cfg = getattr(exec_, "client_config", None) or getattr(exec_, "_client_config", None)
        if cfg:
            cfg.timeout = seconds
    except Exception:
        pass

def wait_for_pages(driver, timeout=60):
    driver.set_script_timeout(timeout + 5)
    return int(driver.execute_async_script("""
        const budget = arguments[0];
        const done   = arguments[arguments.length - 1];
        const start  = performance.now();
        function tick() {
            const n = document.querySelectorAll('.outer_page').length;
            if (n > 0) { done(n); return; }
            if (performance.now() - start >= budget) { done(0); return; }
            setTimeout(tick, 250);
        }
        tick();
    """, timeout * 1000) or 0)

def load_page_batch(driver, nums, timeout=120):
    configure_timeout(driver, timeout + 10)
    driver.set_script_timeout(timeout + 10)
    result = driver.execute_async_script("""
        const nums      = arguments[0];
        const timeoutMs = arguments[1];
        const done      = arguments[arguments.length - 1];
        const mgr       = window.docManager;
        if (!mgr || !mgr.pages) { done({supported: false}); return; }
        const states = nums.map(n => ({ pageNum: n, page: mgr.pages[n], error: null }));
        const start  = Date.now();
        for (const s of states) {
            if (!s.page) { s.error = 'missing'; continue; }
            try { if (!s.page.innerPageElem) s.page.load(); }
            catch (e) { s.error = String(e); }
        }
        const timer = setInterval(() => {
            let ready = 0;
            for (const s of states) {
                if (s.error) { ready++; continue; }
                const p = s.page;
                if (!p.innerPageElem) continue;
                try {
                    p.display();
                    if (!p._imagesTurnedOn) p.turnOnImages();
                } catch (e) { s.error = String(e); ready++; continue; }
                const imgs = Array.from(p.innerPageElem.querySelectorAll('img'));
                if (imgs.every(i => i.complete)) ready++;
            }
            if (ready === states.length) {
                clearInterval(timer);
                done({supported: true, failed: states.filter(s => s.error).map(s => ({n: s.pageNum, r: s.error}))});
                return;
            }
            if (Date.now() - start >= timeoutMs) {
                clearInterval(timer);
                done({supported: true, failed: states.filter(s => s.error).map(s => ({n: s.pageNum, r: s.error || 'timeout'}))});
            }
        }, 50);
    """, list(nums), timeout * 1000)
    if not result or not isinstance(result, dict) or not result.get("supported"):
        raise RuntimeError("Scribd docManager unavailable")

def release_batch(driver, nums):
    driver.execute_script("""
        const mgr = window.docManager;
        if (!mgr || !mgr.pages) return;
        for (const n of arguments[0]) {
            const p = mgr.pages[n];
            if (p) { try { p.remove(); } catch (e) {} }
        }
    """, list(nums))
    try:
        driver.execute_cdp_cmd("HeapProfiler.collectGarbage", {})
    except Exception:
        pass

def hide_cookies(driver):
    driver.execute_script("""
        const kill = [
            '[class*="cookie"]','[class*="Cookie"]','[class*="consent"]','[class*="Consent"]',
            '[class*="gdpr"]','[class*="GDPR"]','[id*="cookie"]','[id*="consent"]',
            '[class*="osano-cm"]','[id*="osano"]','#onetrust-consent-sdk'
        ];
        kill.forEach(s => { try { document.querySelectorAll(s).forEach(e => e.remove()); } catch (x) {} });
    """)

def prepare_for_print(driver):
    driver.execute_script("""
        const top = document.querySelector('.toolbar_top');    if (top) top.remove();
        const bot = document.querySelector('.toolbar_bottom'); if (bot) bot.remove();
        document.querySelectorAll('.document_scroller').forEach(el => {
            el.style.position  = 'static';
            el.style.overflow  = 'visible';
            el.style.height    = 'auto';
            el.style.maxHeight = 'none';
            el.style.margin    = '0';
            el.style.padding   = '0';
        });
    """)

def export_page_to_pdf(driver, index, page_count):
    info = driver.execute_script("""
        const i = arguments[0];
        const pages = Array.from(document.querySelectorAll('.outer_page'));
        const target = pages[i];
        if (!target) return null;
        const old = document.getElementById('isolated-print-css');
        if (old) old.remove();
        pages.forEach(p => {
            ['display','visibility','position','top','left','right','bottom','margin',
             'break-after','page-break-after','break-before','page-break-before']
                .forEach(prop => p.style.removeProperty(prop));
        });
        const r = target.getBoundingClientRect();
        const w = Math.ceil(r.width);
        const h = Math.ceil(r.height);
        pages.forEach(p => p.removeAttribute('data-export-target'));
        target.setAttribute('data-export-target','true');
        const s = document.createElement('style');
        s.id = 'isolated-print-css';
        s.textContent = `
            @page { size: ${w}px ${h}px; margin: 0; }
            @media print {
                html, body {
                    width: ${w}px !important; height: ${h}px !important;
                    min-width: ${w}px !important; min-height: ${h}px !important;
                    max-width: ${w}px !important; max-height: ${h}px !important;
                    margin: 0 !important; padding: 0 !important;
                    overflow: hidden !important;
                    -webkit-print-color-adjust: exact !important;
                    print-color-adjust: exact !important;
                }
                .outer_page { display: none !important; }
                .outer_page[data-export-target="true"] {
                    display: block !important; visibility: visible !important;
                    position: absolute !important;
                    top: 0 !important; left: 0 !important;
                    width: ${w}px !important; height: ${h}px !important;
                    margin: 0 !important; padding: 0 !important;
                    overflow: hidden !important;
                }
            }
        `;
        document.head.appendChild(s);
        return { width: w, height: h };
    """, index)
    if not info:
        raise RuntimeError(f"Page {index+1}/{page_count} missing")
    w_in = int(info["width"]) / 96.0
    h_in = int(info["height"]) / 96.0
    driver.execute_cdp_cmd("Emulation.setEmulatedMedia", {"media": "print"})
    result = driver.execute_cdp_cmd("Page.printToPDF", {
        "landscape": False,
        "displayHeaderFooter": False,
        "printBackground": True,
        "scale": 1,
        "paperWidth": w_in,
        "paperHeight": h_in,
        "marginTop": 0, "marginBottom": 0, "marginLeft": 0, "marginRight": 0,
        "preferCSSPageSize": True,
        "pageRanges": "1",
        "transferMode": "ReturnAsBase64",
    })
    return base64.b64decode(result["data"])

def fetch_scribd_pdf(url, output_path, progress_cb=None):
    embed = embed_url_for(url)
    driver = browser.get()

    try:
        driver.get(embed)
        time.sleep(1)
        hide_cookies(driver)

        pages = wait_for_pages(driver, 60)
        if pages <= 0:
            raise RuntimeError("No printable pages detected")
        if progress_cb:
            progress_cb(f"📄 Detected <b>{pages}</b> pages. Preparing...")

        prepare_for_print(driver)
        configure_timeout(driver, CDP_TIMEOUT)

        spool = tempfile.TemporaryDirectory(prefix="scribd-pages-")
        files = []
        try:
            for i in range(pages):
                if i % EXPORT_BATCH_SIZE == 0:
                    batch_end = min(pages, i + EXPORT_BATCH_SIZE)
                    batch = list(range(i+1, batch_end+1))
                    if progress_cb:
                        progress_cb(f"🖼️ Rendering pages <b>{i+1}–{batch_end}/{pages}</b>...")
                    load_page_batch(driver, batch)

                pdf_bytes = export_page_to_pdf(driver, i, pages)
                if len(PdfReader(BytesIO(pdf_bytes)).pages) != 1:
                    raise RuntimeError(f"Page {i+1} render failed")

                p = os.path.join(spool.name, f"p-{i+1:05d}.pdf")
                with open(p, "wb") as f:
                    f.write(pdf_bytes)
                files.append(p)

                if progress_cb and (i+1) % 5 == 0:
                    progress_cb(f"✅ Exported <b>{i+1}/{pages}</b> pages...")

                if (i+1) % EXPORT_BATCH_SIZE == 0 or (i+1) == pages:
                    release_batch(driver, batch)

            if progress_cb:
                progress_cb(f"🧩 Stitching <b>{len(files)}</b> pages...")

            writer = PdfWriter()
            try:
                for p in files:
                    writer.append(p)
                with open(output_path, "wb") as out:
                    writer.write(out)
            finally:
                writer.close()
        finally:
            spool.cleanup()
    finally:
        try:
            browser.reset_between_jobs()
            browser.maybe_restart()
        except Exception as ex:
            print(f"[browser restart error] {ex}")

    return pages

# ==============================================================
# QUEUE
# ==============================================================

class Job:
    def __init__(self, chat_id, user_id, url, status_msg_id, reply_to_id, user_name):
        self.chat_id       = chat_id
        self.user_id       = user_id
        self.url           = url
        self.status_msg_id = status_msg_id
        self.reply_to_id   = reply_to_id
        self.user_name     = user_name

class QueueManager:
    def __init__(self):
        self.pending = deque()
        self.active  = None
        self.lock    = threading.Lock()

    def add(self, job):
        with self.lock:
            if self.active is None:
                self.active = job
                threading.Thread(target=self._run, args=(job,), daemon=True).start()
                return 0
            self.pending.append(job)
            return len(self.pending)

    def _run(self, job):
        try:
            process_job(job)
        except Exception:
            traceback.print_exc()
        finally:
            with self.lock:
                self.active = None
                next_job = None
                if self.pending:
                    next_job = self.pending.popleft()
                    self.active = next_job
                    threading.Thread(target=self._run, args=(next_job,), daemon=True).start()

                remaining = list(self.pending)
                next_id   = next_job.status_msg_id if next_job else None
                chat_nex  = next_job.chat_id if next_job else None

            if next_job:
                try:
                    bot.edit_message_text(
                        "🚀 <b>It's your turn!</b>\n\nStarting download now...",
                        chat_id=chat_nex,
                        message_id=next_id,
                        parse_mode="HTML",
                    )
                except Exception:
                    pass

            for i, wj in enumerate(remaining):
                try:
                    bot.edit_message_text(
                        f"⏳ <b>Queue update</b>\n\n"
                        f"📍 You are now <b>#{i+1}</b> in line.\n"
                        f"<i>Thanks for your patience!</i>",
                        chat_id=wj.chat_id,
                        message_id=wj.status_msg_id,
                        parse_mode="HTML",
                    )
                    time.sleep(0.05)
                except Exception:
                    pass

queue = QueueManager()

# ==============================================================
# JOB PROCESSOR
# ==============================================================

def process_job(job):
    chat_id = job.chat_id
    msg_id  = job.status_msg_id

    def edit(text):
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
        except Exception:
            pass

    start = time.time()

    try:
        doc_id = extract_doc_id(job.url)

        # Cache hit?
        cached = cache_lookup(doc_id)
        if cached:
            try:
                edit("⚡ <b>Found in cache!</b>\n\nSending instantly...")
                with open(cached, "rb") as f:
                    bot.send_document(
                        chat_id, f,
                        caption=(
                            f"✅ <b>Delivered from cache</b>\n\n"
                            f"⚡ Fetched in under a second.\n\n"
                            f"☕ <a href='{SUPPORT_URL}'>Support the Dev</a>"
                        ),
                        reply_to_message_id=job.reply_to_id,
                        parse_mode="HTML",
                        timeout=300,
                        read_timeout=300,
                    )
                db.increment_jobs()
                db.increment_user_downloads(job.user_id)
                try: bot.delete_message(chat_id, msg_id)
                except Exception: pass
                return
            except Exception as e:
                print(f"[cache error] {e}")
                try: os.remove(cached)
                except Exception: pass

        # Fresh download
        edit("🚀 <b>Starting now...</b>\n\n⚙️ Booting engine...")

        workdir = tempfile.mkdtemp(prefix="scribd-job-")
        try:
            output_pdf = os.path.join(workdir, f"scribd_{doc_id}.pdf")
            pages = fetch_scribd_pdf(job.url, output_pdf, progress_cb=edit)

            if not os.path.exists(output_pdf):
                raise RuntimeError("PDF generation failed")

            size = os.path.getsize(output_pdf)
            if size > MAX_TG_UPLOAD:
                edit(
                    f"❌ <b>File too large</b>\n\n"
                    f"The PDF is {size/1024/1024:.1f} MB — Telegram's bot limit is 50 MB."
                )
                return

            try:
                cache_store(doc_id, output_pdf)
            except Exception:
                pass

            elapsed = time.time() - start
            edit("📤 <b>Uploading to Telegram...</b>")

            with open(output_pdf, "rb") as f:
                bot.send_document(
                    chat_id, f,
                    caption=(
                        f"✅ <b>Download complete!</b>\n\n"
                        f"📄 <b>Doc ID:</b> <code>{doc_id}</code>\n"
                        f"📑 <b>Pages:</b> {pages}\n"
                        f"📦 <b>Size:</b> {size/1024/1024:.1f} MB\n"
                        f"⏱ <b>Time:</b> {elapsed:.1f}s\n\n"
                        f"☕ <a href='{SUPPORT_URL}'>Support the Dev</a>"
                    ),
                    reply_to_message_id=job.reply_to_id,
                    parse_mode="HTML",
                    timeout=300,
                    read_timeout=300,
                )

            db.increment_jobs()
            db.increment_user_downloads(job.user_id)

            try: bot.delete_message(chat_id, msg_id)
            except Exception: pass

        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    except Exception as e:
        err = str(e).lower()
        if "invalid" in err or "no printable" in err or "could not find" in err:
            friendly = "❌ <b>Couldn't fetch that document.</b>\n\nIt may be private, region-locked, or Scribd is blocking us right now. Try again in a minute."
        elif "too large" in err:
            friendly = "❌ <b>File too large for Telegram.</b>"
        elif "timeout" in err or "connection" in err:
            friendly = "⏳ <b>Network timeout.</b>\n\nTelegram took too long to respond. Please try again."
        elif "devtools" in err or "chrome" in err or "launch" in err:
            friendly = "⚙️ <b>Browser engine crashed.</b>\n\nI'm restarting it. Try again in 10 seconds."
        else:
            friendly = "❌ <b>Something went wrong.</b>\n\nPlease try again in a moment."
        edit(friendly)
        print(f"[job error] {e}")
        traceback.print_exc()

# ==============================================================
# TELEGRAM HANDLERS
# ==============================================================

@bot.message_handler(commands=["start"])
def cmd_start(message):
    u = message.from_user
    name = html.escape(u.first_name or "friend")
    is_new = db.register_user(u.id, u.username or "", u.first_name or "")
    jobs, users = db.get_stats()
    my_downloads = db.get_user_downloads(u.id)
    goal, raised = db.get_donations()
    percent = min(100, int((raised / goal * 100)) if goal > 0 else 0)

    stats_block = (
        f"🔢 <b>Global Stats</b>\n"
        f"👥 Users served: <code>{users:,}</code>\n"
        f"✅ Documents delivered: <code>{jobs:,}</code>\n\n"
        f"💰 <b>Server Cost</b>\n"
        f"🎯 Goal: <code>${goal}</code>\n"
        f"💵 Raised: <code>${raised}</code>\n"
        f"📊 Progress: <code>{percent}%</code>\n"
    )

    help_block = (
        "📖 <b>How to use</b>\n"
        "1️⃣ Copy a Scribd link\n"
        "2️⃣ Paste it here\n"
        "3️⃣ Get your PDF!\n\n"
        "☕ /support — keep the server running"
    )

    if is_new:
        text = (
            f"⚡ <b>Welcome to Khan's Scribd Downloader!</b>\n\n"
            f"Hey <b>{name}</b>, I turn Scribd links into beautiful PDFs in seconds.\n\n"
            f"🚫 <b>100% Free.</b> No ads. No redirects. No spam.\n"
            f"🛠 Hosted personally by the Dev.\n\n"
            f"{stats_block}\n"
            f"{help_block}"
        )
    else:
        text = (
            f"👋 <b>Welcome back, {name}!</b>\n\n"
            f"📊 Your downloads: <code>{my_downloads:,}</code>\n\n"
            f"{stats_block}\n"
            f"{help_block}"
        )

    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=["setgoal"])
def cmd_setgoal(message):
    if not ADMIN_ID or str(message.from_user.id) != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /setgoal 10")
        return
    try:
        amount = int(parts[1])
        db.set_donation("donation_goal", amount)
        bot.reply_to(message, f"✅ Donation goal updated to <b>${amount}</b>.", parse_mode="HTML")
    except Exception:
        bot.reply_to(message, "Usage: /setgoal 10")

@bot.message_handler(commands=["setdonation"])
def cmd_setdonation(message):
    if not ADMIN_ID or str(message.from_user.id) != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /setdonation 5")
        return
    try:
        amount = int(parts[1])
        db.set_donation("donation_raised", amount)
        bot.reply_to(message, f"✅ Donations updated to <b>${amount}</b>.", parse_mode="HTML")
    except Exception:
        bot.reply_to(message, "Usage: /setdonation 5")

@bot.message_handler(commands=["help"])
def cmd_help(message):
    text = (
        "📖 <b>How to use Khan's Scribd Downloader</b>\n\n"
        "1️⃣ Open Scribd in your browser\n"
        "2️⃣ Copy the link of the document you want\n"
        "3️⃣ Paste it here\n\n"
        "I'll fetch the full PDF and send it back to you.\n\n"
        "🔗 <b>Supported formats:</b>\n"
        "<code>https://www.scribd.com/document/…</code>\n"
        "<code>https://www.scribd.com/doc/…</code>\n\n"
        "⚡ <b>Features</b>\n"
        "• Free forever\n"
        "• Zero ads\n"
        "• Fair queuing (you'll see your place in line)\n"
        "• Cache — instant for repeat documents\n\n"
        "☕ /support — keep the server running"
    )
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    jobs, users = db.get_stats()
    goal, raised = db.get_donations()
    percent = min(100, int((raised / goal * 100)) if goal > 0 else 0)
    bot.reply_to(
        message,
        f"🔢 <b>Live Stats</b>\n\n"
        f"👥 Users: <code>{users:,}</code>\n"
        f"✅ Documents delivered: <code>{jobs:,}</code>\n\n"
        f"💰 Server Cost\n"
        f"🎯 Goal: <code>${goal}</code> | 💵 Raised: <code>${raised}</code> ({percent}%)",
        parse_mode="HTML",
    )

@bot.message_handler(commands=["support"])
def cmd_support(message):
    text = (
        "☕ <b>Support the Dev</b>\n\n"
        "Khan's Scribd Downloader is completely free.\n"
        "No ads. No trackers. No hidden fees.\n\n"
        "Server costs come straight out of my pocket.\n"
        "If this bot saves you time, you can buy me a coffee:\n\n"
        f"☕ <a href='{SUPPORT_URL}'>Buy Me a Coffee</a>\n\n"
        "Or just share the bot with a friend — that helps too. 💙"
    )
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(content_types=["text"])
def cmd_text(message):
    text = (message.text or "").strip()
    if text.startswith("/"):
        return

    if not is_scribd_url(text):
        bot.reply_to(
            message,
            "❌ <b>Invalid link</b>\n\nPlease send a Scribd URL like:\n"
            "<code>https://www.scribd.com/document/123456789/Title</code>",
            parse_mode="HTML",
        )
        return

    u = message.from_user
    db.register_user(u.id, u.username or "", u.first_name or "")

    try:
        status = bot.reply_to(message, "⏳ <b>Received!</b>\n\nChecking queue...", parse_mode="HTML")
    except Exception:
        return

    job = Job(
        chat_id=message.chat.id,
        user_id=u.id,
        url=text,
        status_msg_id=status.message_id,
        reply_to_id=message.message_id,
        user_name=u.first_name or "friend",
    )

    pos = queue.add(job)

    if pos == 0:
        return

    try:
        bot.edit_message_text(
            f"⏳ <b>You're in line!</b>\n\n"
            f"📍 Position: <b>#{pos}</b>\n"
            f"⏱ Est. wait: ~{pos * 25}s\n\n"
            f"<i>I'll ping you when it's your turn.</i>",
            chat_id=message.chat.id,
            message_id=status.message_id,
            parse_mode="HTML",
        )
    except Exception:
        pass

# ==============================================================
# MAIN
# ==============================================================

def main():
    if not BOT_TOKEN:
        print("!! BOT_TOKEN not set")
        sys.exit(1)

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(PROFILE_DIR, exist_ok=True)

    threading.Thread(target=start_health_server, daemon=True).start()
    print(f"[health] listening on :{PORT}")

    def warm():
        try:
            browser.get()
            print("[warmup] Chrome ready")
        except Exception as e:
            print(f"[warmup] Chrome failed: {e}")
    threading.Thread(target=warm, daemon=True).start()

    jobs, users = db.get_stats()
    print(f"📚 Khan's Scribd Downloader online")
    print(f"   Total jobs delivered: {jobs:,}")
    print(f"   Total users served:   {users:,}")

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=50)
        except Exception as e:
            print(f"[polling] crashed: {e}")
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    main()
