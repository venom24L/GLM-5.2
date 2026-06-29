"""
Video Downloader Backend
FastAPI + yt-dlp + WebSocket for real-time progress
"""

import os
import re
import uuid
import time
import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional

import yt_dlp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Silent logger for yt-dlp noise
logging.getLogger("yt_dlp").setLevel(logging.ERROR)

# Optional: curl_cffi for Cloudflare TLS-fingerprint bypass
try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

# ============================================================
# Configuration
# ============================================================
BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
DOWNLOADS_DIR.mkdir(exist_ok=True)

# File auto-cleanup time (seconds) - 2 hours
FILE_LIFETIME = 2 * 60 * 60

# ============================================================
# App initialization
# ============================================================
app = FastAPI(title="Video Downloader API", version="1.0.0")

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
main_loop: asyncio.AbstractEventLoop = None  # set on startup


# ============================================================
# Utility functions
# ============================================================
def clean_filename(name: str) -> str:
    """Remove illegal characters from filename."""
    name = re.sub(r'[\\/*?:"<>|\n\r\t]', "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:150]


def format_bytes(num: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num < 1024.0:
            return f"{num:.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} PB"


async def send_progress(job_id: str, data: dict):
    """Send JSON update via WebSocket to the connected client."""
    ws = active_connections.get(job_id)
    if ws is None:
        return
    try:
        await ws.send_json(data)
    except Exception:
        active_connections.pop(job_id, None)


def progress_hook_factory(job_id: str):
    """yt-dlp progress hook → forwards events to the WebSocket via the main loop."""
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
                "speed_text": format_bytes(speed) + "/s" if speed else "—",
                "eta": eta,
                "downloaded": downloaded,
                "downloaded_text": format_bytes(downloaded),
                "total": total,
                "total_text": format_bytes(total) if total else "—",
            }
            asyncio.run_coroutine_threadsafe(
                send_progress(job_id, payload), main_loop
            )
        elif d["status"] == "finished":
            asyncio.run_coroutine_threadsafe(
                send_progress(job_id, {
                    "type": "processing",
                    "message": "جاري دمج الصوت والفيديو وتحويل الملف إلى MP4..."
                }),
                main_loop,
            )
    return hook


# ============================================================
# Cloudflare / bot-protection bypass — curl_cffi impersonator
# ============================================================
def fetch_with_browser_fingerprint(url: str, referer: str = "") -> Optional[str]:
    """Fetch a URL using a Chrome TLS fingerprint (bypasses Cloudflare).

    Returns HTML text on success, None on failure.
    """
    if not HAS_CFFI:
        return None
    try:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer or url,
        }
        r = cffi_requests.get(
            url,
            impersonate="chrome120",
            headers=headers,
            timeout=30,
            allow_redirects=True,
        )
        if r.status_code == 200 and len(r.text) > 1000:
            return r.text
    except Exception:
        return None
    return None


def extract_embed_url(html: str, base_domain: str) -> Optional[str]:
    """Find an embed/video URL inside an HTML page."""
    if not html:
        return None
    # Common patterns used by video sites
    patterns = [
        r'"(?:videoUrl|video_url|playUrl|hlsUrl|mp4Url|src)"\s*:\s*"([^"]+)"',
        r'(?:src|href|data-src)\s*=\s*["\']([^"\']*embed[^"\']*)["\']',
        r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)',
        r'(https?://[^\s"\'<>]+/embed/[^\s"\'<>]+)',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            url = m.group(1).replace("\\/", "/").replace("\\u0026", "&")
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                url = "https://" + base_domain + url
            return url
    return None


# ============================================================
# yt-dlp options builder (shared between metadata + download)
# ============================================================
def _build_ydl_opts(job_id: str, out_template: str = None, with_postprocessors: bool = True) -> dict:
    """Build a yt-dlp options dict with anti-bot protection.

    job_id              : identifier used for progress callbacks
    out_template        : output filename template; None for metadata-only
    with_postprocessors : include the FFmpeg→mp4 postprocessor
    """
    opts = {
        # Best video + best audio, fall back to single best file
        "format": "bestvideo*+bestaudio/best",
        "merge_output_format": "mp4",
        "progress_hooks": [progress_hook_factory(job_id)],
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "concurrent_fragment_downloads": 4,
        # ===== Anti-bot / Cloudflare bypass =====
        "no_check_certificates": True,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        },
        # Optional cookies file (Netscape format) next to main.py
        "cookiefile": str(BASE_DIR / "cookies.txt")
            if (BASE_DIR / "cookies.txt").exists() else None,
        "retries": 10,
        "fragment_retries": 10,
        "socket_timeout": 60,
        # Universal age-gate bypass (no site-specific naming)
        "age_limit": 0,
        "geo_bypass": True,
        "geo_bypass_country": "US",
    }

    if out_template:
        opts["outtmpl"] = out_template
    if with_postprocessors:
        opts["postprocessors"] = [
            {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"}
        ]

    # Strip None values (older yt-dlp rejects some)
    return {k: v for k, v in opts.items() if v is not None}


# ============================================================
# Background download task
# ============================================================
async def download_video_task(job_id: str, url: str):
    """Download video with yt-dlp, merge audio+video, output MP4.

    Flow:
      1) Try yt-dlp directly (works for most sites)
      2) On Cloudflare/bot error (410/403), use curl_cffi to fetch
         the page and extract the real embed/mp4 URL, then retry yt-dlp
      3) If a direct .mp4 URL is found, download it with curl_cffi directly
    """
    # Determine base domain for relative-URL resolution
    base_domain = re.match(r"https?://([^/]+)", url)
    base_domain = base_domain.group(1) if base_domain else ""

    current_url = url
    last_error = None

    for attempt in range(3):
        try:
            # ---- Step 1: extract metadata first ----
            meta_opts = _build_ydl_opts(job_id, out_template=None, with_postprocessors=False)
            def extract_meta():
                with yt_dlp.YoutubeDL(meta_opts) as ydl:
                    return ydl.extract_info(current_url, download=False)

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

            # ---- Step 2: download + merge to mp4 ----
            out_template = str(DOWNLOADS_DIR / f"{job_id}.%(ext)s")
            ydl_opts = _build_ydl_opts(
                job_id,
                out_template=out_template,
                with_postprocessors=True,
            )

            def run_download():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([current_url])

            await asyncio.to_thread(run_download)

            # ---- Step 3: locate the produced file ----
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
                raise RuntimeError("لم يتم العثور على الملف بعد التحميل")

            # ---- Step 4: ensure final name is <job_id>.mp4 ----
            final_path = DOWNLOADS_DIR / f"{job_id}.mp4"
            if downloaded_file != final_path:
                if final_path.exists():
                    final_path.unlink()
                downloaded_file.rename(final_path)

            file_size = final_path.stat().st_size
            filename = f"{title}.mp4"

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
            return  # SUCCESS

        except Exception as e:
            last_error = e
            err_text = str(e).lower()

            # If it's a Cloudflare/bot block (410/403/401) → try curl_cffi bypass
            if ("410" in err_text or "403" in err_text or "401" in err_text
                or "unable to download webpage" in err_text):
                if HAS_CFFI:
                    await send_progress(job_id, {
                        "type": "processing",
                        "message": "جاري تجاوز حماية الموقع بمتصفح افتراضي..."
                    })
                    html = fetch_with_browser_fingerprint(current_url, referer=url)
                    if html:
                        new_url = extract_embed_url(html, base_domain)
                        if new_url and new_url != current_url:
                            current_url = new_url
                            continue  # retry yt-dlp with the embed URL

                        # If we found a direct .mp4 URL, download it ourselves
                        if new_url and new_url.endswith(".mp4"):
                            await _download_direct_mp4(job_id, new_url, current_url)
                            return
                # else: no cffi available, fall through to error

            # Not a recoverable error → report and stop
            await send_progress(job_id, {
                "type": "error",
                "message": f"خطأ أثناء التحميل: {str(e)}"
            })
            return

    # All retries exhausted
    await send_progress(job_id, {
        "type": "error",
        "message": f"فشل التحميل بعد عدة محاولات: {last_error}"
    })


async def _download_direct_mp4(job_id: str, mp4_url: str, referer: str):
    """Download a direct .mp4 URL using curl_cffi (browser-fingerprinted)."""
    if not HAS_CFFI:
        await send_progress(job_id, {
            "type": "error",
            "message": "curl_cffi غير متاح لتحميل هذا الرابط"
        })
        return

    final_path = DOWNLOADS_DIR / f"{job_id}.mp4"
    try:
        await send_progress(job_id, {
            "type": "info",
            "title": "video",
            "thumbnail": "",
            "duration": 0,
            "uploader": "",
        })
        await send_progress(job_id, {
            "type": "processing",
            "message": "جاري تحميل ملف MP4 المباشر..."
        })

        def do_download():
            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": referer,
                "Range": "bytes=0-",
            }
            with cffi_requests.get(
                mp4_url,
                impersonate="chrome120",
                headers=headers,
                timeout=60,
                stream=True,
                allow_redirects=True,
            ) as r:
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
                                    "speed_text": format_bytes(speed) + "/s" if speed else "—",
                                    "eta": (total - downloaded) / speed if speed else 0,
                                    "downloaded": downloaded,
                                    "downloaded_text": format_bytes(downloaded),
                                    "total": total,
                                    "total_text": format_bytes(total) if total else "—",
                                }
                                asyncio.run_coroutine_threadsafe(
                                    send_progress(job_id, payload), main_loop
                                )

        await asyncio.to_thread(do_download)

        file_size = final_path.stat().st_size
        download_jobs[job_id].update({
            "file_path": str(final_path),
            "filename": "video.mp4",
            "file_size": file_size,
            "completed": True,
            "completed_at": time.time(),
        })

        await send_progress(job_id, {
            "type": "complete",
            "download_url": f"/download/{job_id}",
            "filename": "video.mp4",
            "size_text": format_bytes(file_size),
        })

    except Exception as e:
        await send_progress(job_id, {
            "type": "error",
            "message": f"خطأ في التحميل المباشر: {str(e)}"
        })


# ============================================================
# Periodic cleanup of old files
# ============================================================
async def cleanup_old_files():
    """Delete downloaded files older than FILE_LIFETIME."""
    while True:
        await asyncio.sleep(600)  # every 10 min
        now = time.time()
        for job_id, job in list(download_jobs.items()):
            completed_at = job.get("completed_at")
            if completed_at and (now - completed_at) > FILE_LIFETIME:
                file_path = job.get("file_path")
                if file_path and Path(file_path).exists():
                    try:
                        Path(file_path).unlink()
                    except Exception:
                        pass
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
    """Serve the main HTML page."""
    index_path = STATIC_DIR / "index.html"
    return HTMLResponse(index_path.read_text(encoding="utf-8"))


@app.post("/api/download")
async def start_download(request: Request, background_tasks: BackgroundTasks):
    """Accept a URL, return a job_id, start background download."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "طلب غير صالح"}, status_code=400)

    url = (body.get("url") or "").strip()
    if not url:
        return JSONResponse({"error": "الرابط مطلوب"}, status_code=400)
    if not re.match(r"^https?://", url):
        return JSONResponse({"error": "يجب أن يبدأ الرابط بـ http:// أو https://"}, status_code=400)

    job_id = str(uuid.uuid4())[:8]
    download_jobs[job_id] = {
        "url": url,
        "completed": False,
        "created_at": time.time(),
    }

    background_tasks.add_task(download_video_task, job_id, url)

    return {"job_id": job_id, "message": "بدأ التحميل"}


@app.get("/download/{job_id}")
async def download_file(job_id: str):
    """Serve the final MP4 file for download."""
    job = download_jobs.get(job_id)
    if not job or not job.get("completed"):
        return JSONResponse({"error": "الملف غير متاح أو لم يكتمل بعد"}, status_code=404)

    file_path = job["file_path"]
    if not Path(file_path).exists():
        return JSONResponse({"error": "انتهت صلاحية الملف"}, status_code=404)

    return FileResponse(
        file_path,
        media_type="video/mp4",
        filename=job.get("filename", "video.mp4"),
    )


@app.websocket("/ws/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    """WebSocket channel for real-time progress updates."""
    await websocket.accept()
    active_connections[job_id] = websocket
    try:
        while True:
            # keep alive; client can ping
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_connections.pop(job_id, None)
    except Exception:
        active_connections.pop(job_id, None)


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
            )
