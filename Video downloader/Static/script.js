/* ============================================================
   Video Downloader — Frontend v2
   - GSAP animations
   - WebSocket with heartbeat
   - HTTP polling fallback (if WS fails)
   - Multi-layer status messages
   ============================================================ */

// ----- DOM -----
const form          = document.getElementById('download-form');
const urlInput      = document.getElementById('url-input');
const downloadBtn   = document.getElementById('download-btn');
const progressCard  = document.getElementById('progress-card');
const videoInfo     = document.getElementById('video-info');
const videoThumb    = document.getElementById('video-thumbnail');
const videoTitle    = document.getElementById('video-title');
const videoMeta     = document.getElementById('video-meta');
const statusText    = document.getElementById('status-text');
const statusSpinner = document.getElementById('status-spinner');
const percentageText= document.getElementById('percentage-text');
const progressBar   = document.getElementById('progress-bar');
const speedText     = document.getElementById('speed-text');
const downloadedText= document.getElementById('downloaded-text');
const totalText     = document.getElementById('total-text');
const finalBtn      = document.getElementById('final-download-btn');
const finalBtnText  = document.getElementById('final-btn-text');
const toast         = document.getElementById('toast');

// ----- Helpers -----
function formatTime(s) {
    if (!s || s < 0) return '—';
    const m = Math.floor(s / 60), x = Math.floor(s % 60);
    return `${m}:${x.toString().padStart(2, '0')}`;
}

function showToast(msg, type = 'error') {
    toast.textContent = msg;
    toast.className = `toast ${type} show`;
    setTimeout(() => { toast.className = 'toast'; }, 3500);
}

function setButtonLoading(loading) {
    if (loading) {
        downloadBtn.disabled = true;
        downloadBtn.innerHTML = `<div class="spinner w-5 h-5 sm:w-6 sm:h-6" style="border-top-color:white"></div><span>جاري البدء...</span>`;
    } else {
        downloadBtn.disabled = false;
        downloadBtn.innerHTML = `<svg class="w-5 h-5 sm:w-6 sm:h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5"><path stroke-linecap="round" stroke-linejoin="round" d="M19 14l-7 7m0 0l-7-7m7 7V3"/></svg><span>تحميل الفيديو</span>`;
    }
}

function resetProgressCard() {
    finalBtn.classList.add('hidden');
    finalBtn.removeAttribute('href');
    statusSpinner.style.display = 'block';
    percentageText.textContent = '0%';
    progressBar.style.width = '0%';
    progressBar.style.backgroundColor = '';
    speedText.textContent = '—';
    downloadedText.textContent = '—';
    totalText.textContent = '—';
    statusText.textContent = 'جاري التحضير...';
    videoThumb.classList.add('hidden');
    videoThumb.src = '';
    videoTitle.textContent = '';
    videoMeta.textContent = '';
    videoInfo.style.opacity = '0';
}

// ----- Handle progress events (from WS or polling) -----
function handleEvent(data) {
    if (!data || !data.type) return;
    switch (data.type) {
        case 'heartbeat':
            break;

        case 'info':
            videoTitle.textContent = data.title || 'فيديو';
            const meta = [];
            if (data.uploader) meta.push(data.uploader);
            if (data.duration) meta.push(formatTime(data.duration));
            videoMeta.textContent = meta.join(' • ');
            if (data.thumbnail) {
                videoThumb.src = data.thumbnail;
                videoThumb.classList.remove('hidden');
                videoThumb.onload  = () => gsap.to(videoInfo, { opacity: 1, duration: 0.5 });
                videoThumb.onerror = () => { videoThumb.classList.add('hidden'); gsap.to(videoInfo, { opacity: 1, duration: 0.5 }); };
            } else {
                gsap.to(videoInfo, { opacity: 1, duration: 0.5 });
            }
            break;

        case 'progress':
            const pct = Math.max(0, Math.min(100, data.percentage));
            percentageText.textContent = `${pct.toFixed(1)}%`;
            gsap.to(progressBar, { width: `${pct}%`, duration: 0.4, ease: 'power2.out' });
            speedText.textContent      = data.speed_text || '—';
            downloadedText.textContent = data.downloaded_text || '—';
            totalText.textContent      = data.total_text || '—';
            statusText.textContent     = 'جاري التحميل...';
            break;

        case 'processing':
            statusText.textContent = data.message || 'جاري المعالجة...';
            if (!percentageText.textContent || percentageText.textContent === '0%') {
                percentageText.textContent = '...';
            }
            break;

        case 'complete':
            statusText.textContent = 'اكتمل التحميل!';
            statusSpinner.style.display = 'none';
            percentageText.textContent = '100%';
            gsap.to(progressBar, { width: '100%', duration: 0.5, ease: 'power2.out' });
            finalBtn.href = data.download_url;
            finalBtnText.textContent = `تنزيل (${data.size_text || 'MP4'})`;
            finalBtn.classList.remove('hidden');
            gsap.fromTo(finalBtn, { opacity: 0, y: 20 }, { opacity: 1, y: 0, duration: 0.6, ease: 'back.out(1.7)' });
            showToast('اكتمل التحميل بنجاح! 🎉', 'success');
            stopAllConnections();
            break;

        case 'error':
            statusText.textContent = data.message || 'حدث خطأ';
            statusSpinner.style.display = 'none';
            percentageText.textContent = '!';
            gsap.to(progressBar, { width: '100%', duration: 0.3, backgroundColor: '#ef4444' });
            showToast(data.message || 'حدث خطأ أثناء التحميل', 'error');
            stopAllConnections();
            break;
    }
}

// ----- WebSocket connection -----
let ws = null;
let pollTimer = null;

function stopAllConnections() {
    if (ws) { try { ws.close(); } catch(e) {} ws = null; }
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

function connectWebSocket(jobId) {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host;
    const wsUrl = `${protocol}//${host}/ws/${jobId}`;

    let wsConnected = false;
    try {
        ws = new WebSocket(wsUrl);

        const wsTimeout = setTimeout(() => {
            if (!wsConnected) {
                console.warn('WebSocket timeout, falling back to polling');
                startPolling(jobId);
            }
        }, 4000);

        ws.onopen = () => { wsConnected = true; clearTimeout(wsTimeout); };
        ws.onmessage = (e) => {
            try { handleEvent(JSON.parse(e.data)); } catch(err) {}
        };
        ws.onerror = () => {
            clearTimeout(wsTimeout);
            if (!wsConnected) startPolling(jobId);
        };
        ws.onclose = () => {
            clearTimeout(wsTimeout);
            if (!finalBtn.classList.contains('hidden')) return;
            if (!statusText.textContent.includes('اكتمل') && !statusText.textContent.includes('خطأ')) {
                startPolling(jobId);
            }
        };
    } catch (e) {
        startPolling(jobId);
    }
}

// ----- HTTP polling fallback -----
function startPolling(jobId) {
    if (pollTimer) return;
    console.info('Using HTTP polling fallback');
    let lastUpdate = 0;
    pollTimer = setInterval(async () => {
        try {
            const r = await fetch(`/api/status/${jobId}`);
            if (!r.ok) return;
            const data = await r.json();
            if (data.last_state && data.last_update > lastUpdate) {
                lastUpdate = data.last_update;
                handleEvent(data.last_state);
            }
        } catch (e) {}
    }, 1000);
}

// ----- Form submit -----
form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const url = urlInput.value.trim();
    if (!url) {
        showToast('الرجاء إدخال رابط الفيديو', 'error');
        gsap.fromTo(urlInput, { x: -10 }, { x: 0, duration: 0.4, ease: 'elastic.out(1, 0.3)' });
        return;
    }

    setButtonLoading(true);
    resetProgressCard();
    stopAllConnections();

    try {
        progressCard.style.display = 'block';
        gsap.fromTo(progressCard, { opacity: 0, height: 0 }, { opacity: 1, height: 'auto', duration: 0.6, ease: 'power3.out' });

        const response = await fetch('/api/download', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url })
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.error || 'فشل بدء التحميل');
        }

        const { job_id } = await response.json();
        connectWebSocket(job_id);
    } catch (err) {
        showToast(err.message || 'حدث خطأ غير متوقع', 'error');
        progressCard.style.display = 'none';
    } finally {
        setButtonLoading(false);
    }
});

// ----- Page load animation -----
window.addEventListener('load', () => {
    const tl = gsap.timeline({ defaults: { ease: 'power3.out' } });
    tl.from('#header', { opacity: 0, y: -30, duration: 0.8 })
      .from('#main-card', { opacity: 0, y: 40, duration: 0.8 }, '-=0.4')
      .from('#main-card form > *', { opacity: 0, y: 20, duration: 0.5, stagger: 0.15 }, '-=0.4')
      .from('#footer', { opacity: 0, y: 20, duration: 0.6 }, '-=0.3');
});

// ----- Paste detection -----
window.addEventListener('paste', (e) => {
    const pasted = (e.clipboardData || window.clipboardData).getData('text');
    if (pasted && pasted.match(/^https?:\/\//)) {
        setTimeout(() => {
            urlInput.value = pasted.trim();
            gsap.fromTo(urlInput, { scale: 1.05 }, { scale: 1, duration: 0.4, ease: 'back.out(2)' });
            downloadBtn.focus();
        }, 0);
    }
});
