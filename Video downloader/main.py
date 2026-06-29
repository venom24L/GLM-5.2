"""
Video Downloader Backend
FastAPI + yt-dlp + WebSocket for real-time progress
"""

import os
import re
import uuid
import time
import asyncio
from pathlib import Path
from typing import Dict

import yt_dlp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

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
# Background download task
# ============================================================
async def download_video_task(job_id: str, url: str):
    """Download video with yt-dlp, merge audio+video, output MP4."""
    try:
        # ---- Step 1: extract metadata first ----
        def extract_meta():
            with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
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

        # ---- Step 2: download + merge to mp4 ----
        out_template = str(DOWNLOADS_DIR / f"{job_id}.%(ext)s")
        ydl_opts = {
            # Best video + best audio, fall back to single best file
            "format": "bestvideo*+bestaudio/best",
            "merge_output_format": "mp4",
            "outtmpl": out_template,
            "progress_hooks": [progress_hook_factory(job_id)],
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "concurrent_fragment_downloads": 4,
            # ensure final container is mp4
            "postprocessors": [
                {
                    "key": "FFmpegVideoConvertor",
                    "preferedformat": "mp4",
                }
            ],
        }

        def run_download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

        await asyncio.to_thread(run_download)

        # ---- Step 3: locate the produced file ----
        downloaded_file = None
        # look for mp4 first (preferred), then other containers
        for ext in (".mp4", ".mkv", ".webm", ".m4a", ".mp3"):
            candidate = DOWNLOADS_DIR / f"{job_id}{ext}"
            if candidate.exists():
                downloaded_file = candidate
                break

        if not downloaded_file:
            # fallback: any file starting with job_id
            for f in DOWNLOADS_DIR.glob(f"{job_id}*"):
                if f.is_file():
                    downloaded_file = f
                    break

        if not downloaded_file:
            await send_progress(job_id, {
                "type": "error",
                "message": "لم يتم العثور على الملف بعد التحميل"
            })
            return

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

    except Exception as e:
        await send_progress(job_id, {
            "type": "error",
            "message": f"خطأ أثناء التحميل: {str(e)}"
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
