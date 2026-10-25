#!/usr/bin/env python3
"""
scribd.py — Telegram Scribd downloader bot.
Optimized for Render.com Docker deployment using standard Selenium.
"""

import os
import re
import sys
import time
import base64
import shutil
import tempfile
import threading
import traceback
from io import BytesIO
from http.server import BaseHTTPRequestHandler, HTTPServer

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from pypdf import PdfReader, PdfWriter

import telebot

# ============================== CONFIG ==============================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
PORT = int(os.environ.get("PORT", 8080))

CHROME_BIN = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")
HEADLESS = True

MAX_TG_UPLOAD = 49 * 1024 * 1024
CDP_TIMEOUT = 600
EXPORT_BATCH_SIZE = 4

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ======================== HEALTH CHECK SERVER =======================

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass # Silence HTTP logs in console

def run_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    server.serve_forever()

# ============================ UTILITIES =============================

def is_scribd_url(url: str) -> bool:
    return bool(re.search(r"scribd\.com/(?:document|doc|read)/\d+", url, re.I))

def extract_doc_id(url: str) -> str:
    m = re.search(r"scribd\.com/(?:document|doc|read)/(\d+)", url)
    if not m:
        raise ValueError("Couldn't find a document ID in that URL.")
    return m.group(1)

def embed_url_for(url: str) -> str:
    return f"https://www.scribd.com/embeds/{extract_doc_id(url)}/content"

def configure_command_timeout(driver, timeout_seconds):
    executor = getattr(driver, "command_executor", None)
    if executor is None: return
    client_config = getattr(executor, "client_config", None) or getattr(executor, "_client_config", None)
    if client_config is not None:
        client_config.timeout = timeout_seconds

# ======================== SELENIUM SETUP ===========================

def build_chrome_options(profile_dir: str) -> Options:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,2000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--force-color-profile=srgb")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument(f"--user-data-dir={profile_dir}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.binary_location = CHROME_BIN
    return opts

def launch_driver(profile_dir: str):
    service = Service(executable_path=CHROMEDRIVER_PATH)
    return webdriver.Chrome(service=service, options=build_chrome_options(profile_dir))

# ======================== PAGE LOADING =============================

def wait_for_pages(driver, timeout_seconds: int = 30) -> int:
    driver.set_script_timeout(timeout_seconds + 5)
    count = driver.execute_async_script(
        """
        const budget = arguments[0];
        const done = arguments[arguments.length - 1];
        const start = performance.now();
        function tick() {
            const n = document.querySelectorAll('.outer_page').length;
            if (n > 0) { done(n); return; }
            if (performance.now() - start >= budget) { done(0); return; }
            setTimeout(tick, 250);
        }
        tick();
        """,
        timeout_seconds * 1000,
    )
    return int(count or 0)

def load_page_batch(driver, page_numbers: list[int], timeout_seconds: int = 120):
    configure_command_timeout(driver, timeout_seconds + 10)
    driver.set_script_timeout(timeout_seconds + 10)
    result = driver.execute_async_script(
        """
        const pageNumbers = arguments[0];
        const timeoutMs = arguments[1];
        const done = arguments[arguments.length - 1];
        const manager = window.docManager;
        if (!manager || !manager.pages) { done({supported: false}); return; }
        const states = pageNumbers.map(n => ({ pageNum: n, page: manager.pages[n], error: null }));
        const startedAt = Date.now();
        for (const s of states) {
            if (!s.page) { s.error = 'page object missing'; continue; }
            try { if (!s.page.innerPageElem) { s.page.load(); } } catch (e) { s.error = String(e); }
        }
        const timer = setInterval(() => {
            let ready = 0;
            for (const s of states) {
                if (s.error) { ready++; continue; }
                const page = s.page;
                if (!page.innerPageElem) { continue; }
                try { page.display(); if (!page._imagesTurnedOn) { page.turnOnImages(); } } 
                catch (e) { s.error = String(e); ready++; continue; }
                const imgs = Array.from(page.innerPageElem.querySelectorAll('img'));
                if (imgs.every(img => img.complete)) { ready++; }
            }
            if (ready === states.length) {
                clearInterval(timer);
                done({ supported: true, failed: states.filter(s => s.error).map(s => ({ pageNum: s.pageNum, reason: s.error })) });
                return;
            }
            if (Date.now() - startedAt >= timeoutMs) {
                clearInterval(timer);
                done({ supported: true, failed: states.filter(s => s.error).map(s => ({ pageNum: s.pageNum, reason: s.error || 'timeout' })) });
            }
        }, 50);
        """,
        list(page_numbers), timeout_seconds * 1000,
    )
    if not result.get("supported"): raise RuntimeError("Scribd docManager unavailable.")
    if result["failed"]: raise RuntimeError(f"Page load failed: {result['failed']}")

def release_page_batch(driver, page_numbers: list[int]):
    driver.execute_script(
        """
        const manager = window.docManager;
        if (!manager || !manager.pages) return;
        for (const n of arguments[0]) {
            const page = manager.pages[n];
            if (page) { try { page.remove(); } catch (e) {} }
        }
        """,
        list(page_numbers),
    )
    try: driver.execute_cdp_cmd("HeapProfiler.collectGarbage", {})
    except Exception: pass

# ======================== DOM PREP & PDF ===========================

def hide_cookie_dialogs(driver):
    driver.execute_script(
        """
        const kill = [ '[class*="cookie"]', '[class*="Cookie"]', '[class*="consent"]', '[class*="Consent"]', '[class*="gdpr"]', '[class*="GDPR"]', '[id*="cookie"]', '[id*="consent"]', '[class*="osano-cm"]', '[id*="osano"]', '#onetrust-consent-sdk' ];
        kill.forEach(sel => { try { document.querySelectorAll(sel).forEach(el => el.remove()); } catch (e) {} });
        """
    )

def prepare_for_print(driver):
    driver.execute_script(
        """
        const top = document.querySelector('.toolbar_top'); if (top) top.remove();
        const bottom = document.querySelector('.toolbar_bottom'); if (bottom) bottom.remove();
        document.querySelectorAll('.document_scroller').forEach(el => {
            el.style.position = 'static'; el.style.overflow = 'visible'; el.style.height = 'auto';
            el.style.maxHeight = 'none'; el.style.margin = '0'; el.style.padding = '0';
        });
        """
    )

def export_page_to_pdf(driver, index: int, page_count: int) -> bytes:
    page_info = driver.execute_script(
        """
        const i = arguments[0];
        const pages = Array.from(document.querySelectorAll('.outer_page'));
        const target = pages[i];
        if (!target) return null;
        const old = document.getElementById('isolated-print-css'); if (old) old.remove();
        pages.forEach(p => {
            ['display','visibility','position','top','left','right','bottom','margin',
             'break-after','page-break-after','break-before','page-break-before']
                .forEach(prop => p.style.removeProperty(prop));
        });
        const rect = target.getBoundingClientRect();
        const w = Math.ceil(rect.width); const h = Math.ceil(rect.height);
        pages.forEach(p => p.removeAttribute('data-export-target'));
        target.setAttribute('data-export-target', 'true');
        const style = document.createElement('style');
        style.id = 'isolated-print-css';
        style.textContent = `
            @page { size: ${w}px ${h}px; margin: 0; }
            @media print {
                html, body {
                    width: ${w}px !important; height: ${h}px !important;
                    min-width: ${w}px !important; min-height: ${h}px !important;
                    max-width: ${w}px !important; max-height: ${h}px !important;
                    margin: 0 !important; padding: 0 !important; overflow: hidden !important;
                    -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important;
                }
                .outer_page { display: none !important; }
                .outer_page[data-export-target="true"] {
                    display: block !important; visibility: visible !important;
                    position: absolute !important; top: 0 !important; left: 0 !important;
                    width: ${w}px !important; height: ${h}px !important;
                    margin: 0 !important; padding: 0 !important; overflow: hidden !important;
                }
            }
        `;
        document.head.appendChild(style);
        return { width: w, height: h };
        """,
        index,
    )
    if not page_info: raise RuntimeError(f"Page {index + 1}/{page_count} missing.")
    width_in = int(page_info["width"]) / 96.0
    height_in = int(page_info["height"]) / 96.0

    driver.execute_cdp_cmd("Emulation.setEmulatedMedia", {"media": "print"})
    result = driver.execute_cdp_cmd(
        "Page.printToPDF",
        {
            "landscape": False, "displayHeaderFooter": False, "printBackground": True, "scale": 1,
            "paperWidth": width_in, "paperHeight": height_in,
            "marginTop": 0, "marginBottom": 0, "marginLeft": 0, "marginRight": 0,
            "preferCSSPageSize": True, "pageRanges": "1", "transferMode": "ReturnAsBase64",
        },
    )
    return base64.b64decode(result["data"])

def fetch_scribd_pdf(url: str, output_path: str, progress_cb=None) -> str:
    embed = embed_url_for(url)
    if progress_cb: progress_cb("🔍 Inspecting document...")
    with tempfile.TemporaryDirectory(prefix="scribd-chrome-") as profile_dir:
        driver = launch_driver(profile_dir)
        try:
            driver.get(embed)
            time.sleep(1)
            hide_cookie_dialogs(driver)
            page_count = wait_for_pages(driver, timeout_seconds=30)
            if page_count <= 0: raise RuntimeError("No printable pages found.")
            if progress_cb: progress_cb(f"📄 Found {page_count} pages. Preparing...")
            prepare_for_print(driver)
            configure_command_timeout(driver, CDP_TIMEOUT)

            spool = tempfile.TemporaryDirectory(prefix="scribd-pages-")
            page_files = []
            try:
                for index in range(page_count):
                    if index % EXPORT_BATCH_SIZE == 0:
                        batch_end = min(page_count, index + EXPORT_BATCH_SIZE)
                        batch = list(range(index + 1, batch_end + 1))
                        if progress_cb: progress_cb(f"🖼️ Loading pages {index + 1}–{batch_end}/{page_count}...")
                        load_page_batch(driver, batch)

                    pdf_bytes = export_page_to_pdf(driver, index, page_count)
                    reader = PdfReader(BytesIO(pdf_bytes))
                    if len(reader.pages) != 1: raise RuntimeError(f"Page {index + 1} produced {len(reader.pages)} sheets.")
                    
                    page_path = os.path.join(spool.name, f"page-{index + 1:08d}.pdf")
                    with open(page_path, "wb") as f: f.write(pdf_bytes)
                    page_files.append(page_path)

                    if progress_cb and (index + 1) % 5 == 0: progress_cb(f"✅ Exported {index + 1}/{page_count} pages...")
                    if (index + 1) % EXPORT_BATCH_SIZE == 0 or index + 1 == page_count:
                        release_page_batch(driver, batch)

                if not page_files: raise RuntimeError("No pages exported.")
                if progress_cb: progress_cb(f"🧩 Merging {len(page_files)} pages...")
                writer = PdfWriter()
                try:
                    for p_path in page_files: writer.append(p_path)
                    with open(output_path, "wb") as out: writer.write(out)
                finally: writer.close()
            finally: spool.cleanup()
        finally:
            try: driver.quit()
            except Exception: pass
    return os.path.abspath(output_path)

# ======================== TELEGRAM BOT =============================

@bot.message_handler(commands=["start", "help"])
def cmd_start(message): bot.reply_to(message, "📚 Send me any Scribd document URL and I'll render it to PDF.")

@bot.message_handler(commands=["ping"])
def cmd_ping(message): bot.reply_to(message, "🏓 pong")

@bot.message_handler(content_types=["text"])
def handle_text(message):
    text = (message.text or "").strip()
    if text.startswith("/"): return
    if not is_scribd_url(text):
        bot.reply_to(message, "Send me a Scribd document URL.\nExample: https://www.scribd.com/document/123456789/Title")
        return
    process_download(message, text)

def process_download(message, url: str):
    chat_id = message.chat.id
    status = bot.send_message(chat_id, "⏳ Queued...")
    status_id = status.message_id
    def edit(text: str):
        try: bot.edit_message_text(text, chat_id, status_id)
        except Exception: pass

    workdir = tempfile.mkdtemp(prefix="scribd-job-")
    try:
        edit("⚙️ Starting Chrome...")
        doc_id = extract_doc_id(url)
        output_pdf = os.path.join(workdir, f"scribd_{doc_id}.pdf")
        fetch_scribd_pdf(url, output_pdf, progress_cb=edit)
        size = os.path.getsize(output_pdf)
        if size > MAX_TG_UPLOAD:
            edit(f"❌ PDF is {size / 1024 / 1024:.1f} MB — over Telegram's 50 MB limit.")
            return
        edit("📤 Uploading to Telegram...")
        with open(output_pdf, "rb") as f:
            bot.send_document(chat_id, f, caption=f"Scribd doc {doc_id} • {size / 1024 / 1024:.1f} MB", reply_to_message_id=message.message_id)
        try: bot.delete_message(chat_id, status_id)
        except Exception: pass
    except Exception as e:
        edit(f"❌ Failed: {e}")
        traceback.print_exc()
    finally: shutil.rmtree(workdir, ignore_errors=True)

# ============================== ENTRY ==============================

def main():
    if not BOT_TOKEN:
        print("!! BOT_TOKEN is missing. Set it in the environment variables.")
        sys.exit(1)

    print("📚 Scribd bot starting...")
    print(f"   Health server on port {PORT}")
    threading.Thread(target=run_health_server, daemon=True).start()

    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=50)
        except Exception as e:
            print(f"!! Polling crashed: {e}")
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    main()
