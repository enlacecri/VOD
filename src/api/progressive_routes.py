import uuid
from typing import Optional
from pathlib import Path
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import HTMLResponse, JSONResponse

from src.core.security import SecurityError
from src.worker.progressive_manager import progressive_manager

router = APIRouter()

class ProgressiveStartRequest(BaseModel):
    source_uri: str

class ProgressiveStartResponse(BaseModel):
    session_uuid: str
    status: str

@router.post("/start", response_model=ProgressiveStartResponse, status_code=status.HTTP_201_CREATED)
def start_progressive(payload: ProgressiveStartRequest):
    try:
        session = progressive_manager.start_session(payload.source_uri)
        return ProgressiveStartResponse(
            session_uuid=session.session_uuid,
            status=session.status
        )
    except SecurityError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{e.code}: {e.message}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/{session_uuid}")
def get_progressive_status(session_uuid: str):
    session = progressive_manager.get_session(session_uuid)
    if not session:
        # Check if auxiliary session.json exists on disk
        from src.core.config import settings
        import json
        aux_file = Path(settings.PROGRESSIVE_ROOT).resolve() / session_uuid / "session.json"
        if aux_file.exists():
            try:
                with open(aux_file, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Progressive session not found"
        )
    return session.to_dict()

@router.get("/player/ui", response_class=HTMLResponse)
def progressive_player(session_uuid: Optional[str] = None):
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Progressive HLS POC Player</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: #0f172a;
            color: #f8fafc;
            margin: 0;
            padding: 24px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .container {{
            width: 100%;
            max-width: 900px;
            background: #1e293b;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
        }}
        h1 {{
            margin-top: 0;
            font-size: 1.5rem;
            color: #38bdf8;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .badge {{
            font-size: 0.8rem;
            padding: 4px 10px;
            border-radius: 9999px;
            text-transform: uppercase;
            font-weight: 700;
        }}
        .badge-STARTING {{ background: #475569; color: #f8fafc; }}
        .badge-PROCESSING {{ background: #ca8a04; color: #fef08a; }}
        .badge-PLAYABLE {{ background: #16a34a; color: #bbf7d0; }}
        .badge-COMPLETED {{ background: #0284c7; color: #bae6fd; }}
        .badge-FAILED {{ background: #dc2626; color: #fecaca; }}
        
        .video-wrapper {{
            position: relative;
            width: 100%;
            background: #000;
            border-radius: 8px;
            overflow: hidden;
            margin-bottom: 20px;
            aspect-ratio: 16 / 9;
        }}
        video {{
            width: 100%;
            height: 100%;
            display: block;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }}
        .card {{
            background: #334155;
            padding: 14px 16px;
            border-radius: 8px;
        }}
        .card-label {{
            font-size: 0.75rem;
            text-transform: uppercase;
            color: #94a3b8;
            letter-spacing: 0.05em;
            margin-bottom: 4px;
        }}
        .card-value {{
            font-size: 1.3rem;
            font-weight: 600;
            color: #f1f5f9;
        }}
        .progress-bar-container {{
            width: 100%;
            background: #475569;
            height: 12px;
            border-radius: 6px;
            overflow: hidden;
            margin-top: 6px;
        }}
        .progress-bar {{
            height: 100%;
            background: #38bdf8;
            width: 0%;
            transition: width 0.4s ease;
        }}
        .info-row {{
            margin-top: 12px;
            font-size: 0.85rem;
            color: #cbd5e1;
            word-break: break-all;
        }}
        .info-row a {{
            color: #38bdf8;
            text-decoration: none;
        }}
        .info-row a:hover {{
            text-decoration: underline;
        }}
        .controls-bar {{
            display: flex;
            gap: 10px;
            margin-bottom: 16px;
        }}
        input[type="text"] {{
            flex: 1;
            padding: 10px 14px;
            background: #334155;
            border: 1px solid #475569;
            border-radius: 6px;
            color: #fff;
            font-size: 0.95rem;
        }}
        button {{
            padding: 10px 20px;
            background: #2563eb;
            color: #fff;
            border: none;
            border-radius: 6px;
            font-weight: 600;
            cursor: pointer;
            transition: background 0.2s;
        }}
        button:hover {{
            background: #1d4ed8;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>
            Progressive HLS POC
            <span id="statusBadge" class="badge badge-STARTING">STARTING</span>
        </h1>

        <div class="controls-bar">
            <input type="text" id="sessionInput" placeholder="Session UUID" value="{session_uuid or ''}">
            <button onclick="loadSession()">Load Session</button>
        </div>

        <div class="video-wrapper">
            <video id="player" controls playsinline></video>
        </div>

        <div class="metrics-grid">
            <div class="card">
                <div class="card-label">FFmpeg Progress</div>
                <div id="progressVal" class="card-value">0%</div>
                <div class="progress-bar-container">
                    <div id="progressBar" class="progress-bar"></div>
                </div>
            </div>

            <div class="card">
                <div class="card-label">Available Content</div>
                <div id="availVal" class="card-value">0.0s</div>
                <div style="font-size: 0.8rem; color: #94a3b8; margin-top: 4px;">HLS ready to play</div>
            </div>

            <div class="card">
                <div class="card-label">Total Duration</div>
                <div id="durationVal" class="card-value">--</div>
                <div style="font-size: 0.8rem; color: #94a3b8; margin-top: 4px;">Source media length</div>
            </div>

            <div class="card">
                <div class="card-label">Active Renditions</div>
                <div id="variantsVal" class="card-value">--</div>
                <div id="segmentsVal" style="font-size: 0.8rem; color: #94a3b8; margin-top: 4px;">0 segments</div>
            </div>
        </div>

        <div class="info-row">
            <strong>Session UUID:</strong> <span id="sessionUuidText">None</span>
        </div>
        <div class="info-row">
            <strong>Manifest URL:</strong> <a id="manifestLink" href="#" target="_blank">--</a>
        </div>
        <div class="info-row" id="errorBox" style="display: none; color: #ef4444; margin-top: 10px;">
            <strong>Error:</strong> <span id="errorText"></span>
        </div>
    </div>

    <script>
        let currentSessionUuid = "{session_uuid or ''}";
        let videoLoaded = false;
        let pollInterval = null;

        const player = document.getElementById('player');
        const statusBadge = document.getElementById('statusBadge');
        const progressVal = document.getElementById('progressVal');
        const progressBar = document.getElementById('progressBar');
        const availVal = document.getElementById('availVal');
        const durationVal = document.getElementById('durationVal');
        const variantsVal = document.getElementById('variantsVal');
        const segmentsVal = document.getElementById('segmentsVal');
        const sessionUuidText = document.getElementById('sessionUuidText');
        const manifestLink = document.getElementById('manifestLink');
        const errorBox = document.getElementById('errorBox');
        const errorText = document.getElementById('errorText');

        function loadSession() {{
            const inputVal = document.getElementById('sessionInput').value.trim();
            if (inputVal) {{
                currentSessionUuid = inputVal;
                videoLoaded = false;
                window.history.replaceState(null, '', `?session_uuid=${{currentSessionUuid}}`);
                startPolling();
            }}
        }}

        async function fetchStatus() {{
            if (!currentSessionUuid) return;

            try {{
                const res = await fetch(`/api/v1/experimental/progressive/${{currentSessionUuid}}`);
                if (!res.ok) {{
                    statusBadge.className = 'badge badge-FAILED';
                    statusBadge.innerText = 'NOT FOUND';
                    return;
                }}
                const data = await res.json();
                
                // Update UI badges & values
                statusBadge.className = `badge badge-${{data.status}}`;
                statusBadge.innerText = data.status;
                
                progressVal.innerText = `${{data.progress}}%`;
                progressBar.style.width = `${{data.progress}}%`;
                
                availVal.innerText = `${{data.available_until_seconds}}s`;
                durationVal.innerText = `${{data.duration_seconds}}s`;
                
                if (data.active_variants && data.active_variants.length > 0) {{
                    variantsVal.innerText = data.active_variants.join(', ');
                }}
                if (data.segments_per_variant) {{
                    const segSummary = Object.entries(data.segments_per_variant).map(([k, v]) => `${{k}}:${{v}}`).join(' | ');
                    segmentsVal.innerText = `Segments: ${{segSummary}}`;
                }}

                sessionUuidText.innerText = data.session_uuid;
                manifestLink.innerText = data.manifest_url;
                manifestLink.href = data.manifest_url;

                if (data.error_message) {{
                    errorBox.style.display = 'block';
                    errorText.innerText = data.error_message;
                }} else {{
                    errorBox.style.display = 'none';
                }}

                // Playback condition: if PLAYABLE or COMPLETED and not loaded yet
                if ((data.status === 'PLAYABLE' || data.status === 'COMPLETED') && !videoLoaded) {{
                    videoLoaded = true;
                    console.log('Loading manifest into player:', data.manifest_url);
                    player.src = data.manifest_url;
                    player.play().catch(e => console.log('Autoplay deferred:', e));
                }}

                if (data.status === 'COMPLETED' || data.status === 'FAILED') {{
                    // Slow down polling when finished
                    clearInterval(pollInterval);
                    pollInterval = setInterval(fetchStatus, 5000);
                }}
            }} catch (err) {{
                console.error('Polling error:', err);
            }}
        }}

        function startPolling() {{
            if (pollInterval) clearInterval(pollInterval);
            fetchStatus();
            pollInterval = setInterval(fetchStatus, 1000);
        }}

        if (currentSessionUuid) {{
            startPolling();
        }}
    </script>
</body>
</html>"""
    return HTMLResponse(content=html_content)
