"""
Video Downloader Backend â€” Multi-layer fallback engine
======================================================
Layer 1: yt-dlp (works for 95% of sites)
Layer 2: curl_cffi (Cloudflare TLS-fingerprint bypass)
Layer 3: Playwright headless browser (JS challenge bypass)
Layer 4: Direct MP4 download (for raw video URLs)

Plus: WebSocket heartbeat to prevent UI from freezing at 0%
"""

import os
import re
import uuid
import time
import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional, List

import yt_dlp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

logging.getLogger("yt_dlp").setLevel(logging.ERROR)

try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False


# ============================================================
# Configuration
# ============================================================
BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "Static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
DOWNLOADS_DIR.mkdir(exist_ok=True)

FILE_LIFETIME = 2 * 60 * 60  # 2 hours

# ============================================================
# App
# ============================================================
app = FastAPI(title="Video Downloader API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ============================================================
# Shared state
# ============================================================
active_connections: Dict[str, WebSocket] = {}
download_jobs: Dict[str, dict] = {}
main_loop: asyncio.AbstractEventLoop = None


# ============================================================
# Utilities
# ============================================================
def clean_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|\n\r\t]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:150] or "video"


def format_bytes(num: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024.0:
            return f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"


async def send_progress(job_id: str, data: dict):
    """Send update to WebSocket AND store it for HTTP polling fallback."""
    if job_id in download_jobs:
        download_jobs[job_id]["last_state"] = data
        download_jobs[job_id]["last_update"] = time.time()

    ws = active_connections.get(job_id)
    if ws is None:
        return
    try:
        await ws.send_json(data)
    except Exception:
        active_connections.pop(job_id, None)


def progress_hook_factory(job_id: str):
    def hook(d):
        if main_loop is None:
            return
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            downloaded = d.get("downloaded_bytes", 0)
            speed = d.get("speed") or 0
            eta = d.get("eta") or 0
            percentage = (downloaded / total * 100) if total else 0

            payload = {
                "type": "progress",
                "percentage": round(percentage, 2),
                "speed": speed,
                "speed_text": format_bytes(speed) + "/s" if speed else "â€”",
                "eta": eta,
                "downloaded": downloaded,
                "downloaded_text": format_bytes(downloaded),
                "total": total,
                "total_text": format_bytes(total) if total else "â€”",
            }
            asyncio.run_coroutine_threadsafe(
                send_progress(job_id, payload), main_loop
            )
        elif d["status"] == "finished":
            asyncio.run_coroutine_threadsafe(
                send_progress(job_id, {
                    "type": "processing",
                    "message": "Ø¬Ø§Ø±ÙŠ Ø¯Ù…Ø¬ Ø§Ù„ØµÙˆØª ÙˆØ§Ù„ÙÙŠØ¯ÙŠÙˆ ÙˆØªØ­ÙˆÙŠÙ„ Ø§Ù„Ù…Ù„Ù Ø¥Ù„Ù‰ MP4..."
                }),
                main_loop,
            )
    return hook


# ============================================================
# yt-dlp options
# ============================================================
def _build_ydl_opts(job_id: str, out_template: str = None, with_postprocessors: bool = True) -> dict:
    opts = {
        "format": "bestvideo*+bestaudio/best",
        "merge_output_format": "mp4",
        "progress_hooks": [progress_hook_factory(job_id)],
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "concurrent_fragment_downloads": 4,
        "no_check_certificates": True,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        },
        "cookiefile": str(BASE_DIR / "cookies.txt")
            if (BASE_DIR / "cookies.txt").exists() else None,
        "retries": 5,
        "fragment_retries": 5,
        "socket_timeout": 30,
        "geo_bypass": True,
        "geo_bypass_country": "US",
    }
    if out_template:
        opts["outtmpl"] = out_template
    if with_postprocessors:
        opts["postprocessors"] = [
            {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}
        ]
    return {k: v for k, v in opts.items() if v is not None}


# ============================================================
# Layer 2: curl_cffi â€” Cloudflare TLS bypass
# ============================================================
def fetch_with_browser_fingerprint(url: str, referer: str = "") -> Optional[str]:
    if not HAS_CFFI:
        return None
    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer or url,
        }
        r = cffi_requests.get(
            url, impersonate="chrome120", headers=headers,
            timeout=30, allow_redirects=True,
        )
        if r.status_code == 200 and len(r.text) > 500:
            return r.text
    except Exception:
        return None
    return None


def find_video_urls_in_html(html: str, base_domain: str) -> List[str]:
    """Extract candidate video URLs from HTML."""
    if not html:
        return []
    urls = []
    patterns = [
        r'"(?:videoUrl|video_url|playUrl|hlsUrl|mp4Url|src)"\s*:\s*"([^"]+)"',
        r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)',
        r'(https?://[^\s"\'<>]+/embed/[^\s"\'<>]+)',
        r'(?:src|href|data-src)\s*=\s*["\']([^"\']*(?:video|play|watch)[^"\']*)["\']',
    ]
    for pat in patterns:
        for m in re.finditer(pat, html, re.IGNORECASE):
            url = m.group(1).replace("\\/", "/").replace("\\u0026", "&")
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = "https://" + base_domain + url
            if url not in urls:
                urls.append(url)
    return urls


# ============================================================
# Layer 3: Playwright headless browser
# ============================================================
async def fetch_with_playwright(url: str, timeout_ms: int = 30000) -> Optional[str]:
    """Open URL in real headless Chrome, wait for network idle, return HTML."""
    if not HAS_PLAYWRIGHT:
        return None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 720},
                locale="en-US",
            )
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                window.chrome = { runtime: {} };
            """)
            page = await context.new_page()

            video_urls_found = []
            async def on_request(req):
                if any(ext in req.url.lower() for ext in [".mp4", ".m3u8", "/video/", "videoplayback"]):
                    video_urls_found.append(req.url)
            page.on("request", on_request)

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                try:
                    await page.wait_for_load_state("networkidle", timeout=timeout_ms // 2)
                except Exception:
                    pass
                await asyncio.sleep(3)
                html = await page.content()
            except Exception:
                html = None

            await context.close()
            await browser.close()

            if video_urls_found:
                return "<!--VIDEO_URLS-->\n" + "\n".join(video_urls_found) + "\n<!--/VIDEO_URLS-->\n" + (html or "")
            return html
    except Exception:
        return None


# ============================================================
# Layer 4: Direct MP4 download with curl_cffi
# ============================================================
async def download_direct_mp4(job_id: str, mp4_url: str, referer: str, title: str = "video"):
    final_path = DOWNLOADS_DIR / f"{job_id}.mp4"
    try:
        await send_progress(job_id, {
            "type": "processing",
            "message": "Ø¬Ø§Ø±ÙŠ ØªØ­Ù…ÙŠÙ„ Ù…Ù„Ù Ø§Ù„ÙÙŠØ¯ÙŠÙˆ Ø§Ù„Ù…Ø¨Ø§Ø´Ø±..."
        })

        def do_download():
            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": referer,
                "Range": "bytes=0-",
            }
            r = cffi_requests.get(
                mp4_url, impersonate="chrome120", headers=headers,
                timeout=120, stream=True, allow_redirects=True,
            )
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0)) or 0
            downloaded = 0
            last_update = 0
            start_time = time.time()
            with open(final_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_update > 0.3:
                            last_update = now
                            pct = (downloaded / total * 100) if total else 0
                            speed = downloaded / (now - start_time) if now > start_time else 0
                            payload = {
                                "type": "progress",
                                "percentage": round(pct, 2),
                                "speed": speed,
                                "speed_text": format_bytes(speed) + "/s" if speed else "â€”",
                                "eta": (total - downloaded) / speed if speed else 0,
                                "downloaded": downloaded,
                                "downloaded_text": format_bytes(downloaded),
                                "total": total,
                                "total_text": format_bytes(total) if total else "â€”",
                            }
                            asyncio.run_coroutine_threadsafe(
                                send_progress(job_id, payload), main_loop
                            )

        await asyncio.to_thread(do_download)

        file_size = final_path.stat().st_size
        filename = f"{clean_filename(title)}.mp4"
        download_jobs[job_id].update({
            "file_path": str(final_path),
            "filename": filename,
            "file_size": file_size,
            "completed": True,
            "completed_at": time.time(),
        })
        await send_progress(job_id, {
            "type": "complete",
            "download_url": f"/download/{job_id}",
            "filename": filename,
            "size_text": format_bytes(file_size),
        })
        return True
    except Exception as e:
        await send_progress(job_id, {
            "type": "error",
            "message": f"ÙØ´Ù„ Ø§Ù„ØªØ­Ù…ÙŠÙ„ Ø§Ù„Ù…Ø¨Ø§Ø´Ø±: {str(e)}"
        })
        return False


# ============================================================
# Finalize a yt-dlp download
# ============================================================
async def finalize_ydl_download(job_id: str, title: str) -> bool:
    downloaded_file = None
    for ext in (".mp4", ".mkv", ".webm", ".m4a", ".mp3"):
        candidate = DOWNLOADS_DIR / f"{job_id}{ext}"
        if candidate.exists():
            downloaded_file = candidate
            break
    if not downloaded_file:
        for f in DOWNLOADS_DIR.glob(f"{job_id}*"):
            if f.is_file():
                downloaded_file = f
                break
    if not downloaded_file:
        return False

    if downloaded_file.stat().st_size == 0:
        downloaded_file.unlink(missing_ok=True)
        return False

    final_path = DOWNLOADS_DIR / f"{job_id}.mp4"
    if downloaded_file != final_path:
        if final_path.exists():
            final_path.unlink()
        downloaded_file.rename(final_path)

    file_size = final_path.stat().st_size
    filename = f"{clean_filename(title)}.mp4"
    download_jobs[job_id].update({
        "file_path": str(final_path),
        "filename": filename,
        "file_size": file_size,
        "completed": True,
        "completed_at": time.time(),
    })
    await send_progress(job_id, {
        "type": "complete",
        "download_url": f"/download/{job_id}",
        "filename": filename,
        "size_text": format_bytes(file_size),
    })
    return True


# ============================================================
# Main download task â€” multi-layer fallback
# ============================================================
async def download_video_task(job_id: str, url: str):
    base_domain_match = re.match(r"https?://([^/]+)", url)
    base_domain = base_domain_match.group(1) if base_domain_match else ""

    # Send immediate "preparing" status so UI doesn't freeze at 0%
    await send_progress(job_id, {
        "type": "info",
        "title": "Ø¬Ø§Ø±ÙŠ Ø§Ù„ØªØ­Ø¶ÙŠØ±...",
        "thumbnail": "",
        "duration": 0,
        "uploader": "",
    })
    await send_progress(job_id, {
        "type": "processing",
        "message": "Ø¬Ø§Ø±ÙŠ Ø§Ù„Ø§ØªØµØ§Ù„ Ø¨Ø§Ù„Ù…ÙˆÙ‚Ø¹..."
    })

    # ====== LAYER 1: yt-dlp ======
    try:
        await send_progress(job_id, {
            "type": "processing",
            "message": "Ø§Ù„Ù…Ø­Ø§ÙˆÙ„Ø© Ø§Ù„Ø£ÙˆÙ„Ù‰: yt-dlp..."
        })
        meta_opts = _build_ydl_opts(job_id, out_template=None, with_postprocessors=False)

        def extract_meta():
            with yt_dlp.YoutubeDL(meta_opts) as ydl:
                return ydl.extract_info(url, download=False)

        info = await asyncio.to_thread(extract_meta)
        title = clean_filename(info.get("title", "video"))
        thumbnail = info.get("thumbnail", "") or ""
        duration = info.get("duration", 0) or 0
        uploader = info.get("uploader", "") or ""

        await send_progress(job_id, {
            "type": "info",
            "title": title,
            "thumbnail": thumbnail,
            "duration": duration,
            "uploader": uploader,
        })

        out_template = str(DOWNLOADS_DIR / f"{job_id}.%(ext)s")
        ydl_opts = _build_ydl_opts(job_id, out_template=out_template, with_postprocessors=True)

        def run_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

        await asyncio.to_thread(run_download)

        if await finalize_ydl_download(job_id, title):
            return
    except Exception as e:
        err_text = str(e).lower()
        is_bot_block = any(code in err_text for code in ["410", "403", "401", "unable to download"])
        if not is_bot_block:
            await send_progress(job_id, {
                "type": "error",
                "message": f"Ø®Ø·Ø£ Ø£Ø«Ù†Ø§Ø¡ Ø§Ù„ØªØ­Ù…ÙŠÙ„: {str(e)[:200]}"
            })
            return

    # ====== LAYER 2: curl_cffi ======
    if HAS_CFFI:
        await send_progress(job_id, {
            "type": "processing",
            "message": "Ø§Ù„Ù…Ø­Ø§ÙˆÙ„Ø© Ø§Ù„Ø«Ø§Ù†ÙŠØ©: ØªØ¬Ø§ÙˆØ² Ø­Ù…Ø§ÙŠØ© Ø§Ù„Ù…ÙˆÙ‚Ø¹..."
        })
        html = await asyncio.to_thread(fetch_with_browser_fingerprint, url, url)
        if html:
            video_urls = find_video_urls_in_html(html, base_domain)
            for vurl in video_urls:
                if ".mp4" in vurl.lower():
                    await send_progress(job_id, {
                        "type": "info",
                        "title": "video",
                        "thumbnail": "",
                        "duration": 0,
                        "uploader": "",
                    })
                    if await download_direct_mp4(job_id, vurl, url):
                        return
            for vurl in video_urls:
                try:
                    out_template = str(DOWNLOADS_DIR / f"{job_id}.%(ext)s")
                    ydl_opts = _build_ydl_opts(job_id, out_template=out_template, with_postprocessors=True)

                    def run_dl(u=vurl):
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            ydl.download([u])

                    await asyncio.to_thread(run_dl)
                    if await finalize_ydl_download(job_id, "video"):
                        return
                except Exception:
                    continue

    # ====== LAYER 3: Playwright ======
    if HAS_PLAYWRIGHT:
        await send_progress(job_id, {
            "type": "processing",
            "message": "Ø§Ù„Ù…Ø­Ø§ÙˆÙ„Ø© Ø§Ù„Ø«Ø§Ù„Ø«Ø©: Ù…ØªØµÙØ­ Ø§ÙØªØ±Ø§Ø¶ÙŠ ÙƒØ§Ù…Ù„..."
        })
        try:
            html = await fetch_with_playwright(url, timeout_ms=30000)
            if html:
                if "<!--VIDEO_URLS-->" in html:
                    vu_section = html.split("<!--VIDEO_URLS-->")[1].split("<!--/VIDEO_URLS-->")[0]
                    for vurl in vu_section.strip().split("\n"):
                        vurl = vurl.strip()
                        if vurl and ".mp4" in vurl.lower():
                            if await download_direct_mp4(job_id, vurl, url):
                                return

                video_urls = find_video_urls_in_html(html, base_domain)
                for vurl in video_urls:
                    if ".mp4" in vurl.lower():
                        if await download_direct_mp4(job_id, vurl, url):
                            return
        except Exception:
            pass

    # ====== ALL LAYERS FAILED ======
    await send_progress(job_id, {
        "type": "error",
        "message": "ØªØ¹Ø°Ù‘Ø± ØªØ­Ù…ÙŠÙ„ Ø§Ù„ÙÙŠØ¯ÙŠÙˆ Ø¨Ø¹Ø¯ ØªØ¬Ø±Ø¨Ø© Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø·Ø±Ù‚ Ø§Ù„Ù…ØªØ§Ø­Ø©. ØªØ£ÙƒØ¯ Ù…Ù† ØµØ­Ø© Ø§Ù„Ø±Ø§Ø¨Ø· Ø£Ùˆ Ø¬Ø±Ù‘Ø¨ Ø±Ø§Ø¨Ø·Ø§Ù‹ Ø¢Ø®Ø±."
    })


# ============================================================
# Cleanup task
# ============================================================
async def cleanup_old_files():
    while True:
        await asyncio.sleep(600)
        now = time.time()
        for job_id, job in list(download_jobs.items()):
            completed_at = job.get("completed_at")
            if completed_at and (now - completed_at) > FILE_LIFETIME:
                fp = job.get("file_path")
                if fp and Path(fp).exists():
                    try: Path(fp).unlink()
                    except Exception: pass
                download_jobs.pop(job_id, None)


# ============================================================
# Routes
# ============================================================
@app.on_event("startup")
async def on_startup():
    global main_loop
    main_loop = asyncio.get_event_loop()
    asyncio.create_task(cleanup_old_files())


@app.get("/")
async def root():
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


@app.post("/api/download")
async def start_download(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Ø·Ù„Ø¨ ØºÙŠØ± ØµØ§Ù„Ø­"}, status_code=400)

    url = (body.get("url") or "").strip()
    if not url:
        return JSONResponse({"error": "Ø§Ù„Ø±Ø§Ø¨Ø· Ù…Ø·Ù„ÙˆØ¨"}, status_code=400)
    if not re.match(r"^https?://", url):
        return JSONResponse({"error": "ÙŠØ¬Ø¨ Ø£Ù† ÙŠØ¨Ø¯Ø£ Ø§Ù„Ø±Ø§Ø¨Ø· Ø¨Ù€ http:// Ø£Ùˆ https://"}, status_code=400)

    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = {
        "url": url,
        "completed": False,
        "created_at": time.time(),
        "last_state": None,
        "last_update": time.time(),
    }

    background_tasks.add_task(download_video_task, job_id, url)
    return {"job_id": job_id, "message": "Ø¨Ø¯Ø£ Ø§Ù„ØªØ­Ù…ÙŠÙ„"}


@app.get("/api/status/{job_id}")
async def get_status(job_id: str):
    """HTTP polling fallback â€” returns the latest state for a job."""
    job = download_jobs.get(job_id)
    if not job:
        return JSONResponse({"error": "job not found"}, status_code=404)
    return {
        "job_id": job_id,
        "completed": job.get("completed", False),
        "last_state": job.get("last_state"),
        "last_update": job.get("last_update"),
    }


@app.get("/download/{job_id}")
async def download_file(job_id: str):
    job = download_jobs.get(job_id)
    if not job or not job.get("completed"):
        return JSONResponse({"error": "Ø§Ù„Ù…Ù„Ù ØºÙŠØ± Ù…ØªØ§Ø­"}, status_code=404)
    file_path = job["file_path"]
    if not Path(file_path).exists():
        return JSONResponse({"error": "Ø§Ù†ØªÙ‡Øª ØµÙ„Ø§Ø­ÙŠØ© Ø§Ù„Ù…Ù„Ù"}, status_code=404)
    return FileResponse(
        file_path, media_type="video/mp4",
        filename=job.get("filename", "video.mp4"),
    )


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """WebSocket with heartbeat â€” sends a ping every 5s to keep UI alive."""
    await websocket.accept()
    active_connections[job_id] = websocket

    job = download_jobs.get(job_id)
    if job and job.get("last_state"):
        try:
            await websocket.send_json(job["last_state"])
        except Exception:
            pass

    async def heartbeat():
        while True:
            await asyncio.sleep(5)
            try:
                await websocket.send_json({"type": "heartbeat", "t": time.time()})
            except Exception:
                return

    hb_task = asyncio.create_task(heartbeat())
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        hb_task.cancel()
        active_connections.pop(job_id, None)


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "layers": {
            "yt_dlp": True,
            "curl_cffi": HAS_CFFI,
            "playwright": HAS_PLAYWRIGHT,
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
