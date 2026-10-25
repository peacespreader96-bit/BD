#!/usr/bin/env python3
"""
Heart Over Money | PDF Bot
Production-grade, queue-based, cache-aware, MAX SPEED, Loggable.
"""

import os
import re
import gc
import sys
import time
import html
import base64
import shutil
import sqlite3
import logging
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

MAX_JOB_SECONDS   = 900  # 15 minutes hard cap
MAX_CACHE_BYTES   = 400 * 1024 * 1024
LAST_JOB_DURATION = 30.0 

# TELEGRAM STARS CONFIG
STARS_AMOUNT      = 50 # Amount of stars to request

# ==============================================================
# LOGGING SETUP
# ==============================================================

log_buffer = deque(maxlen=150)

class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        log_buffer.append(self.format(record))

logger = logging.getLogger("heart_over_money")
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(console_handler)

memory_handler = MemoryLogHandler()
memory_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(memory_handler)

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
                    total_downloads INTEGER DEFAULT 0,
                    total_stars INTEGER DEFAULT 0
                )
            """)
            # Safe migration for existing DBs
            try:
                c.execute("ALTER TABLE users ADD COLUMN total_stars INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass # Column already exists
                
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
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('max_pages_limit', 80)")
            c.execute("INSERT OR IGNORE INTO stats (key, value) VALUES ('total_stars', 0)")

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
                
    def add_stars(self, user_id, amount):
        with self.lock:
            with self._conn() as c:
                c.execute("UPDATE users SET total_stars = total_stars + ? WHERE user_id = ?", (amount, user_id))
                c.execute("UPDATE stats SET value = value + ? WHERE key = 'total_stars'", (amount,))

    def get_stats(self):
        with self.lock:
            with self._conn() as c:
                jobs  = c.execute("SELECT value FROM stats WHERE key = 'total_jobs'").fetchone()
                users = c.execute("SELECT value FROM stats WHERE key = 'total_users'").fetchone()
                stars = c.execute("SELECT value FROM stats WHERE key = 'total_stars'").fetchone()
                return (jobs["value"] if jobs else 0), (users["value"] if users else 0), (stars["value"] if stars else 0)

    def get_user_downloads(self, user_id):
        with self.lock:
            with self._conn() as c:
                row = c.execute("SELECT total_downloads FROM users WHERE user_id = ?", (user_id,)).fetchone()
                return row["total_downloads"] if row else 0

    def set_stat(self, key, value):
        with self.lock:
            with self._conn() as c:
                c.execute("UPDATE stats SET value = ? WHERE key = ?", (value, key))

    def get_stat(self, key, default=0):
        with self.lock:
            with self._conn() as c:
                row = c.execute("SELECT value FROM stats WHERE key = ?", (key,)).fetchone()
                return row["value"] if row else default

    def get_all_user_ids(self):
        with self.lock:
            with self._conn() as c:
                rows = c.execute("SELECT user_id FROM users").fetchall()
                return [row["user_id"] for row in rows]

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
                
                opts.add_argument("--js-flags=--max-old-space-size=192")
                opts.add_argument("--disable-extensions")
                opts.add_argument("--disable-background-networking")
                opts.add_argument("--disable-default-apps")
                opts.add_argument("--disable-sync")
                opts.add_argument("--metrics-recording-only")
                opts.add_argument("--mute-audio")
                opts.add_argument("--no-first-run")
                opts.add_argument("--safebrowsing-disable-auto-update")
                opts.add_argument("--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter,OptimizationHints")
                
                opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
                opts.add_experimental_option("excludeSwitches", ["enable-automation"])
                opts.add_experimental_option("useAutomationExtension", False)
                opts.binary_location = CHROME_BIN

                service = Service(executable_path=CHROMEDRIVER_PATH)
                self.driver = webdriver.Chrome(service=service, options=opts)
                self.jobs_since_reset = 0
                logger.info("[browser] Chrome launched successfully")
                return
            except Exception as e:
                logger.error(f"[browser] Launch failed (attempt {attempt+1}): {e}")
                self._kill()
                time.sleep(2)
        raise RuntimeError("Chrome failed to launch after 3 attempts")

    def reset_between_jobs(self):
        if self.driver is None: return
        try:
            self.driver.get("about:blank")
            self.driver.delete_all_cookies()
        except Exception:
            self._kill()
            self._launch()

    def maybe_restart(self):
        self.jobs_since_reset += 1
        if self.jobs_since_reset >= JOBS_BEFORE_CHROME_RESET:
            logger.info("[browser] Periodic restart to free memory")
            self._kill()
            self._launch()

    def _kill(self):
        try:
            if self.driver: self.driver.quit()
        except Exception: pass
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
    files = [os.path.join(CACHE_DIR, f) for f in os.listdir(CACHE_DIR)]
    total = sum(os.path.getsize(f) for f in files if os.path.exists(f))
    if total > MAX_CACHE_BYTES:
        for old in sorted(files, key=os.path.getmtime):
            sz = os.path.getsize(old) if os.path.exists(old) else 0
            try: os.remove(old)
            except: pass
            total -= sz
            if total < MAX_CACHE_BYTES * 0.7: break
    shutil.copyfile(src_path, cache_path(doc_id))

# ==============================================================
# SCRIBD LOGIC
# ==============================================================

def is_scribd_url(url):
    return bool(re.search(r"scribd\.com/(?:document|doc|read)/\d+", url, re.I))

def extract_doc_id(url):
    m = re.search(r"scribd\.com/(?:document|doc|read)/(\d+)", url)
    if not m: raise ValueError("Invalid Scribd URL")
    return m.group(1)

def embed_url_for(url):
    return f"https://www.scribd.com/embeds/{extract_doc_id(url)}/content"

def configure_timeout(driver, seconds):
    try:
        exec_ = driver.command_executor
        cfg = getattr(exec_, "client_config", None) or getattr(exec_, "_client_config", None)
        if cfg: cfg.timeout = seconds
    except: pass

def wait_for_pages(driver, timeout=60):
    driver.set_script_timeout(timeout + 5)
    return int(driver.execute_async_script("""
        const budget = arguments[0], done = arguments[arguments.length - 1], start = performance.now();
        function tick() {
            const n = document.querySelectorAll('.outer_page').length;
            if (n > 0) { done(n); return; }
            if (performance.now() - start >= budget) { done(0); return; }
            setTimeout(tick, 250);
        }
        tick();
    """, timeout * 1000) or 0)

def load_page_batch(driver, nums, timeout=45):
    configure_timeout(driver, timeout + 10)
    driver.set_script_timeout(timeout + 10)
    result = driver.execute_async_script("""
        const nums = arguments[0], timeoutMs = arguments[1], done = arguments[arguments.length - 1];
        const mgr = window.docManager;
        if (!mgr || !mgr.pages) { done({supported: false}); return; }
        const states = nums.map(n => ({ pageNum: n, page: mgr.pages[n], error: null }));
        const start = Date.now();
        for (const s of states) {
            if (!s.page) { s.error = 'missing'; continue; }
            try { if (!s.page.innerPageElem) s.page.load(); } catch (e) { s.error = String(e); }
        }
        const timer = setInterval(() => {
            let ready = 0;
            for (const s of states) {
                if (s.error) { ready++; continue; }
                const p = s.page;
                if (!p.innerPageElem) continue;
                try { p.display(); if (!p._imagesTurnedOn) p.turnOnImages(); } 
                catch (e) { s.error = String(e); ready++; continue; }
                const imgs = Array.from(p.innerPageElem.querySelectorAll('img'));
                if (imgs.every(i => i.complete || i.naturalWidth === 0)) ready++;
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
    return result

def release_batch(driver, nums):
    driver.execute_script("""
        const mgr = window.docManager;
        if (!mgr || !mgr.pages) return;
        for (const n of arguments[0]) {
            const p = mgr.pages[n];
            if (p) { try { p.remove(); } catch (e) {} }
        }
    """, list(nums))
    try: driver.execute_cdp_cmd("HeapProfiler.collectGarbage", {})
    except: pass
    gc.collect()

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
            el.style.position  = 'static'; el.style.overflow  = 'visible'; el.style.height    = 'auto';
            el.style.maxHeight = 'none'; el.style.margin    = '0'; el.style.padding   = '0';
        });
    """)

def export_page_to_pdf(driver, index, page_count):
    info = driver.execute_script("""
        const i = arguments[0], pages = Array.from(document.querySelectorAll('.outer_page')), target = pages[i];
        if (!target) return null;
        const old = document.getElementById('isolated-print-css'); if (old) old.remove();
        pages.forEach(p => {
            ['display','visibility','position','top','left','right','bottom','margin','break-after','page-break-after','break-before','page-break-before'].forEach(prop => p.style.removeProperty(prop));
        });
        const r = target.getBoundingClientRect(), w = Math.ceil(r.width), h = Math.ceil(r.height);
        pages.forEach(p => p.removeAttribute('data-export-target'));
        target.setAttribute('data-export-target','true');
        const s = document.createElement('style');
        s.id = 'isolated-print-css';
        s.textContent = `
            @page { size: ${w}px ${h}px; margin: 0; }
            @media print {
                html, body { width: ${w}px !important; height: ${h}px !important; min-width: ${w}px !important; min-height: ${h}px !important; max-width: ${w}px !important; max-height: ${h}px !important; margin: 0 !important; padding: 0 !important; overflow: hidden !important; -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
                .outer_page { display: none !important; }
                .outer_page[data-export-target="true"] { display: block !important; visibility: visible !important; position: absolute !important; top: 0 !important; left: 0 !important; width: ${w}px !important; height: ${h}px !important; margin: 0 !important; padding: 0 !important; overflow: hidden !important; }
            }
        `;
        document.head.appendChild(s); return { width: w, height: h };
    """, index)
    if not info: raise RuntimeError(f"Page {index+1}/{page_count} missing")
    w_in, h_in = int(info["width"]) / 96.0, int(info["height"]) / 96.0
    driver.execute_cdp_cmd("Emulation.setEmulatedMedia", {"media": "print"})
    result = driver.execute_cdp_cmd("Page.printToPDF", {
        "landscape": False, "displayHeaderFooter": False, "printBackground": True, "scale": 1,
        "paperWidth": w_in, "paperHeight": h_in, "marginTop": 0, "marginBottom": 0, "marginLeft": 0, "marginRight": 0,
        "preferCSSPageSize": True, "pageRanges": "1", "transferMode": "ReturnAsBase64",
    })
    return base64.b64decode(result["data"])

def fetch_scribd_pdf(url, output_path, progress_cb=None):
    embed = embed_url_for(url)
    driver = browser.get()

    try:
        driver.get(embed)
        hide_cookies(driver)
        pages = wait_for_pages(driver, 60)
        if pages <= 0: raise RuntimeError("No printable pages detected")
        
        current_limit = db.get_stat("max_pages_limit", 80)
        if pages > current_limit:
            raise RuntimeError(f"too_many_pages:{pages}:{current_limit}")

        if progress_cb: progress_cb(f"📄 Detected <b>{pages}</b> pages. Preparing...")

        prepare_for_print(driver)
        spool = tempfile.TemporaryDirectory(prefix="scribd-pages-")
        files = []

        try:
            for i in range(pages):
                if i % EXPORT_BATCH_SIZE == 0:
                    batch_end = min(pages, i + EXPORT_BATCH_SIZE)
                    batch = list(range(i + 1, batch_end + 1))
                    if progress_cb: progress_cb(f"🖼️ Rendering pages <b>{i+1}–{batch_end}/{pages}</b>...")
                    
                    res = load_page_batch(driver, batch)
                    if res.get("failed"):
                        logger.warning(f"Some pages in batch failed: {res['failed']}")

                pdf_bytes = export_page_to_pdf(driver, i, pages)
                if len(PdfReader(BytesIO(pdf_bytes)).pages) != 1: raise RuntimeError(f"Page {i+1} render failed")

                p = os.path.join(spool.name, f"p-{i+1:05d}.pdf")
                with open(p, "wb") as f: f.write(pdf_bytes)
                files.append(p)
                del pdf_bytes

                if progress_cb and (i + 1) % 5 == 0:
                    progress_cb(f"✅ Exported <b>{i+1}/{pages}</b> pages...")
                    logger.info(f"Exported {i+1}/{pages}")

                if (i + 1) % EXPORT_BATCH_SIZE == 0 or (i + 1) == pages:
                    release_batch(driver, batch)

            if progress_cb: progress_cb(f"🧩 Stitching <b>{len(files)}</b> pages...")
            logger.info(f"Stitching {len(files)} pages into PDF...")
            
            writer = PdfWriter()
            try:
                for p in files:
                    if os.path.getsize(p) < 1000:
                        logger.warning(f"Skipping empty/corrupt page file: {p}")
                        continue
                    try:
                        writer.append(p)
                        try: os.remove(p)
                        except: pass
                    except Exception as e:
                        logger.error(f"Failed to append {p}: {e}")
                        
                with open(output_path, "wb") as out: 
                    writer.write(out)
            finally: 
                writer.close()
                
            logger.info("PDF stitching complete.")
            
        finally:
            spool.cleanup()
    finally:
        try: browser.reset_between_jobs(); browser.maybe_restart()
        except Exception as ex: logger.error(f"[browser restart error] {ex}")
    return pages

# ==============================================================
# QUEUE
# ==============================================================

def format_duration(seconds):
    if seconds < 60: return f"{int(seconds)}s"
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{mins}m {secs}s" if secs > 0 else f"{mins}m"

class Job:
    def __init__(self, chat_id, user_id, url, status_msg_id, reply_to_id, user_name):
        self.chat_id, self.user_id, self.url = chat_id, user_id, url
        self.status_msg_id, self.reply_to_id, self.user_name = status_msg_id, reply_to_id, user_name

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

    def cancel(self, user_id):
        with self.lock:
            before = len(self.pending)
            self.pending = deque(j for j in self.pending if j.user_id != user_id)
            removed_pending = before - len(self.pending)
            
            killed_active = False
            if self.active and self.active.user_id == user_id:
                killed_active = True
                logger.warning(f"[cancel] User {user_id} killed active job. Terminating Chrome.")
                browser._kill()
                self.active = None
                
                if self.pending:
                    next_job = self.pending.popleft()
                    self.active = next_job
                    threading.Thread(target=self._run, args=(next_job,), daemon=True).start()
                    
            return removed_pending, killed_active

    def _run(self, job):
        try:
            process_job(job)
        except Exception:
            logger.error(f"Queue error: {traceback.format_exc()}")
        finally:
            with self.lock:
                if self.active is job: 
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
                                chat_id=chat_nex, message_id=next_id, parse_mode="HTML",
                            )
                        except: pass

                    for i, wj in enumerate(remaining):
                        wait_str = format_duration(LAST_JOB_DURATION * (i + 1))
                        try:
                            bot.edit_message_text(
                                f"⏳ <b>Queue update</b>\n\n"
                                f"📍 You are now <b>#{i+1}</b> in line.\n"
                                f"⏱ Est. wait: <b>~{wait_str}</b>\n\n"
                                f"<i>Thanks for your patience!</i>",
                                chat_id=wj.chat_id, message_id=wj.status_msg_id, parse_mode="HTML",
                            )
                        except: pass

queue = QueueManager()

# ==============================================================
# UPLOAD HELPER
# ==============================================================

def safe_upload(chat_id, file_path, caption, reply_to_id):
    for attempt in range(1, 4):
        try:
            with open(file_path, "rb") as f:
                bot.send_document(chat_id, f, caption=caption, reply_to_message_id=reply_to_id, parse_mode="HTML", timeout=600)
            return True
        except Exception as e:
            logger.error(f"[upload attempt {attempt}] Failed: {e}")
            if attempt < 3: time.sleep(3)
    return False

# ==============================================================
# JOB PROCESSOR
# ==============================================================

def process_job(job):
    global LAST_JOB_DURATION
    chat_id, msg_id = job.chat_id, job.status_msg_id
    start = time.time()
    timeout_occurred = False

    def timeout_handler():
        nonlocal timeout_occurred
        timeout_occurred = True
        logger.error(f"[timeout] Job exceeded {MAX_JOB_SECONDS}s. Killing Chrome.")
        browser._kill()

    timer = threading.Timer(MAX_JOB_SECONDS, timeout_handler)
    timer.start()

    def edit(text):
        try: bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
        except: pass

    try:
        doc_id = extract_doc_id(job.url)
        logger.info(f"New Job: {doc_id}")

        cached = cache_lookup(doc_id)
        if cached:
            edit("⚡ <b>Found in cache!</b>\n\nSending instantly...")
            success = safe_upload(
                chat_id, cached,
                f"✅ <b>Delivered from cache</b>\n\n⚡ Fetched in under a second.\n\n⭐ /support to tip the dev with Stars!",
                job.reply_to_id,
            )
            if success:
                db.increment_jobs(); db.increment_user_downloads(job.user_id)
                try: bot.delete_message(chat_id, msg_id)
                except: pass
                return
            else:
                logger.warning(f"[cache upload fail] Removing corrupt cache for {doc_id}")
                try: os.remove(cached)
                except: pass

        edit("🚀 <b>Starting now...</b>\n\n⚙️ Booting engine...")

        workdir = tempfile.mkdtemp(prefix="scribd-job-")
        try:
            output_pdf = os.path.join(workdir, f"scribd_{doc_id}.pdf")
            pages = fetch_scribd_pdf(job.url, output_pdf, progress_cb=edit)

            if not os.path.exists(output_pdf): raise RuntimeError("PDF generation failed")

            size = os.path.getsize(output_pdf)
            if size > MAX_TG_UPLOAD:
                edit(f"❌ <b>File too large</b>\n\nThe PDF is {size/1024/1024:.1f} MB — Telegram's bot limit is 50 MB.")
                return

            try: cache_store(doc_id, output_pdf)
            except: pass

            elapsed = time.time() - start
            edit("📤 <b>Uploading to Telegram...</b>")

            success = safe_upload(
                chat_id, output_pdf,
                f"✅ <b>Download complete!</b>\n\n📄 <b>Doc ID:</b> <code>{doc_id}</code>\n📑 <b>Pages:</b> {pages}\n📦 <b>Size:</b> {size/1024/1024:.1f} MB\n⏱ <b>Time:</b> {elapsed:.1f}s\n\n⭐ /support to tip the dev with Stars!",
                job.reply_to_id,
            )

            if success:
                db.increment_jobs(); db.increment_user_downloads(job.user_id)
                try: bot.delete_message(chat_id, msg_id)
                except: pass
                if not timeout_occurred and elapsed > 5:
                    LAST_JOB_DURATION = elapsed
                    logger.info(f"Job finished in {elapsed:.1f}s")
            else:
                edit("❌ <b>Upload failed.</b>\n\nTelegram is rejecting the connection. The server is busy. Please try again in a few minutes.")
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    except Exception as e:
        err = str(e).lower()
        if timeout_occurred:
            friendly = "❌ <b>Job timed out.</b>\n\nThis document is too large or Scribd is being slow. Please try a smaller document."
        elif "too_many_pages" in err:
            _, pages, limit = str(e).split(":")
            friendly = f"❌ <b>Document too large</b>\n\nThis document has <b>{pages}</b> pages, but the current limit is <b>{limit}</b> pages to prevent server overload."
        elif "invalid" in err or "no printable" in err or "could not find" in err:
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
        logger.error(f"[job error] {traceback.format_exc()}")
    finally:
        timer.cancel()

# ==============================================================
# TELEGRAM HANDLERS
# ==============================================================

# --- STARS PAYMENT HANDLERS ---

@bot.message_handler(commands=["support"])
def cmd_support(message):
    text = (
        "⭐ <b>Support the Dev with Telegram Stars</b>\n\n"
        "Heart Over Money is completely free.\nNo ads. No trackers. No hidden fees.\n\n"
        "Server costs come straight out of my pocket.\nIf this bot saves you time, you can send me Stars directly here on Telegram!\n\n"
        f"👇 Tap the button below to send <b>{STARS_AMOUNT} Stars</b>."
    )
    
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton(f"Send {STARS_AMOUNT} ⭐", callback_data="send_stars"))
    bot.reply_to(message, text, parse_mode="HTML", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "send_stars")
def callback_send_stars(call):
    try:
        bot.answer_callback_query(call.id)
        bot.send_invoice(
            chat_id=call.message.chat.id,
            title="Support Heart Over Money ⭐",
            description="Send Telegram Stars to keep the servers running and the PDFs flowing!",
            invoice_payload="support_donation",
            provider_token="", # MUST be empty for Stars
            currency="XTR",    # Telegram Stars currency code
            prices=[telebot.types.LabeledPrice(label="Support", amount=STARS_AMOUNT)],
            start_parameter="support-stars"
        )
    except Exception as e:
        logger.error(f"Failed to send invoice: {e}")
        bot.send_message(call.message.chat.id, "Failed to generate invoice. Please try again later.")

@bot.pre_checkout_query_handler(func=lambda query: True)
def checkout(pre_checkout_query):
    bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def got_payment(message):
    stars = message.successful_payment.total_amount
    db.add_stars(message.from_user.id, stars)
    bot.reply_to(
        message,
        f"✅ <b>Thank you so much!</b>\n\n"
        f"You just sent <b>{stars} ⭐ Stars</b>. I really appreciate the support! "
        f"Your contribution goes directly toward server costs and keeping the bot free for everyone. 💙",
        parse_mode="HTML"
    )

# --- ADMIN COMMANDS ---

@bot.message_handler(commands=["log"])
def cmd_log(message):
    if not ADMIN_ID or str(message.from_user.id) != ADMIN_ID: return
    if not log_buffer: return bot.reply_to(message, "No logs yet.")
    text = "\n".join(log_buffer)
    if len(text) > 4000: text = "... (truncated)\n" + text[-4000:]
    bot.reply_to(message, f"<b>System Logs:</b>\n<pre>{html.escape(text)}</pre>", parse_mode="HTML")

@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    uid = message.from_user.id
    removed, killed = queue.cancel(uid)
    if killed:
        bot.reply_to(message, "🛑 <b>Active job killed.</b>\n\nI've cleared the queue for you and moved to the next person.", parse_mode="HTML")
    elif removed > 0:
        bot.reply_to(message, "✅ <b>Removed from queue.</b>", parse_mode="HTML")
    else:
        bot.reply_to(message, "ℹ️ You're not in the queue.", parse_mode="HTML")

@bot.message_handler(commands=["setlimit"])
def cmd_setlimit(message):
    if not ADMIN_ID or str(message.from_user.id) != ADMIN_ID: return
    parts = message.text.split()
    if len(parts) < 2: return bot.reply_to(message, "Usage: /setlimit 80")
    try:
        amount = int(parts[1]); db.set_stat("max_pages_limit", amount)
        bot.reply_to(message, f"✅ Max page limit updated to <b>{amount}</b>.", parse_mode="HTML")
    except: bot.reply_to(message, "Usage: /setlimit 80")

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(message):
    if not ADMIN_ID or str(message.from_user.id) != ADMIN_ID: return
    
    if message.reply_to_message:
        msg_to_send = message.reply_to_message
    elif len(message.text.split(None, 1)) > 1:
        msg_to_send = message.text.split(None, 1)[1]
    else:
        bot.reply_to(message, "Usage: Reply to any message with `/broadcast` OR type `/broadcast <text>` to broadcast a text message.", parse_mode="HTML")
        return

    status_msg = bot.reply_to(message, "⏳ <b>Starting broadcast...</b>", parse_mode="HTML")

    def run_broadcast():
        users = db.get_all_user_ids()
        success = 0
        blocked = 0
        
        for uid in users:
            try:
                if isinstance(msg_to_send, str):
                    bot.send_message(uid, msg_to_send, parse_mode="HTML")
                else:
                    bot.copy_message(chat_id=uid, from_chat_id=msg_to_send.chat.id, message_id=msg_to_send.message_id)
                success += 1
            except Exception:
                blocked += 1
            time.sleep(0.05)
            
        bot.edit_message_text(
            f"✅ <b>Broadcast Finished</b>\n\n"
            f"👥 Total Users: <code>{len(users)}</code>\n"
            f"🟢 Delivered: <code>{success}</code>\n"
            f"🔴 Blocked/Error: <code>{blocked}</code>",
            chat_id=status_msg.chat.id, 
            message_id=status_msg.message_id, 
            parse_mode="HTML"
        )
        
    threading.Thread(target=run_broadcast, daemon=True).start()

# --- PUBLIC COMMANDS ---

@bot.message_handler(commands=["start"])
def cmd_start(message):
    u = message.from_user
    name = html.escape(u.first_name or "friend")
    is_new = db.register_user(u.id, u.username or "", u.first_name or "")
    jobs, users, stars = db.get_stats()
    my_downloads = db.get_user_downloads(u.id)

    stats_block = (
        f"🔢 <b>Global Stats</b>\n"
        f"👥 Users served: <code>{users:,}</code>\n"
        f"✅ Documents delivered: <code>{jobs:,}</code>\n"
        f"⭐ Stars tipped: <code>{stars:,}</code>\n\n"
    )

    help_block = (
        "📖 <b>How to use</b>\n"
        "1️⃣ Copy a Scribd link\n2️⃣ Paste it here\n3️⃣ Get your PDF!\n\n"
        "⭐ /support — keep the server running"
    )

    if is_new:
        text = (
            f"⚡ <b>Welcome to Heart Over Money!</b>\n\n"
            f"Hey <b>{name}</b>, I turn Scribd links into beautiful PDFs in seconds.\n\n"
            f"🚫 <b>100% Free.</b> No ads. No redirects. No spam.\n"
            f"🛠 Hosted personally by the Dev.\n\n"
            f"{stats_block}\n{help_block}"
        )
    else:
        text = (
            f"👋 <b>Welcome back, {name}!</b>\n\n"
            f"📊 Your downloads: <code>{my_downloads:,}</code>\n\n"
            f"{stats_block}\n{help_block}"
        )
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=["help"])
def cmd_help(message):
    text = (
        "📖 <b>How to use Heart Over Money</b>\n\n"
        "1️⃣ Open Scribd in your browser\n2️⃣ Copy the link\n3️⃣ Paste it here\n\n"
        "I'll fetch the full PDF and send it back to you.\n\n"
        "🔗 <b>Supported formats:</b>\n<code>https://www.scribd.com/document/…</code>\n<code>https://www.scribd.com/doc/…</code>\n\n"
        "⚡ <b>Features</b>\n• Free forever\n• Zero ads\n• Fair queuing\n• Cache\n\n"
        "⭐ /support — keep the server running"
    )
    bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    jobs, users, stars = db.get_stats()
    limit = db.get_stat("max_pages_limit", 80)
    bot.reply_to(
        message,
        f"🔢 <b>Live Stats</b>\n\n👥 Users: <code>{users:,}</code>\n✅ Documents delivered: <code>{jobs:,}</code>\n⭐ Stars tipped: <code>{stars:,}</code>\n📄 Max Pages: <code>{limit}</code>",
        parse_mode="HTML",
    )

@bot.message_handler(content_types=["text"])
def cmd_text(message):
    text = (message.text or "").strip()
    if text.startswith("/"): return

    if not is_scribd_url(text):
        bot.reply_to(message, "❌ <b>Invalid link</b>\n\nPlease send a Scribd URL like:\n<code>https://www.scribd.com/document/123456789/Title</code>", parse_mode="HTML")
        return

    u = message.from_user
    db.register_user(u.id, u.username or "", u.first_name or "")

    try: status = bot.reply_to(message, "⏳ <b>Received!</b>\n\nChecking queue...", parse_mode="HTML")
    except: return

    job = Job(
        chat_id=message.chat.id, user_id=u.id, url=text,
        status_msg_id=status.message_id, reply_to_id=message.message_id,
        user_name=u.first_name or "friend",
    )

    pos = queue.add(job)
    if pos == 0: return

    estimated_wait = LAST_JOB_DURATION * pos
    wait_str = format_duration(estimated_wait)

    try:
        bot.edit_message_text(
            f"⏳ <b>You're in line!</b>\n\n📍 Position: <b>#{pos}</b>\n⏱ Est. wait: <b>~{wait_str}</b>\n\n<i>I'll ping you when it's your turn.</i>",
            chat_id=message.chat.id, message_id=status.message_id, parse_mode="HTML",
        )
    except: pass

# ==============================================================
# MAIN
# ==============================================================

def main():
    if not BOT_TOKEN:
        logger.error("!! BOT_TOKEN not set")
        sys.exit(1)

    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(PROFILE_DIR, exist_ok=True)

    threading.Thread(target=start_health_server, daemon=True).start()
    logger.info(f"[health] listening on :{PORT}")

    def warm():
        try:
            browser.get()
            logger.info("[warmup] Chrome ready")
        except Exception as e:
            logger.error(f"[warmup] Chrome failed: {e}")
    threading.Thread(target=warm, daemon=True).start()

    jobs, users, stars = db.get_stats()
    logger.info("📚 Heart Over Money PDF Bot online")
    logger.info(f"   Total jobs delivered: {jobs:,}")
    logger.info(f"   Total users served:   {users:,}")
    logger.info(f"   Total stars tipped:   {stars:,}")

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=50)
        except Exception as e:
            logger.error(f"[polling] crashed: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
