import socket
import os
import threading
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

open_ports = []
bindable_ports = []
scan_done = False
scan_started = False
lock = threading.Lock()

def scan_port(port):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.settimeout(0.3)
        result = s.connect_ex(("0.0.0.0", port))
        s.close()
        if result == 0:
            with lock:
                open_ports.append(port)
    except:
        pass

def run_scan():
    global scan_done, open_ports, bindable_ports
    open_ports = []
    bindable_ports = []

    # Connect scan
    threads = []
    for port in range(1, 65536):
        t = threading.Thread(target=scan_port, args=(port,))
        t.daemon = True
        threads.append(t)
        t.start()
        if len(threads) >= 500:
            for th in threads:
                th.join()
            threads = []
    for t in threads:
        t.join()

    open_ports.sort()

    # Bind scan
    common = [80, 443, 3000, 4000, 5000, 8000, 8080, 8443, 8888, 9000, 10000]
    for port in common:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", port))
            s.close()
            bindable_ports.append(port)
        except:
            pass

    scan_done = True
    print(f"✅ Scan complete! Open: {open_ports} | Bindable: {bindable_ports}")

@app.route("/")
def index():
    global scan_started
    render_port = os.environ.get("PORT", "unknown")

    if not scan_started:
        scan_started = True
        threading.Thread(target=run_scan, daemon=True).start()

    html = f"""
    <html>
    <head>
        <title>Port Scanner — Render</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body {{ background:#0f1623; color:#c0cfe8; font-family:monospace; padding:30px; }}
            h1 {{ color:#6366f1; }}
            .done {{ color:#22d87a; font-size:20px; }}
            .waiting {{ color:#f59e0b; font-size:18px; }}
            .box {{ background:#151e2e; border:1px solid #1a2540; border-radius:8px; padding:20px; margin:16px 0; }}
            span.open {{ color:#22d87a; }} span.closed {{ color:#f43f5e; }}
        </style>
    </head>
    <body>
        <h1>🔍 Render Port Scanner</h1>
        <div class="box">
            <b>Render $PORT:</b> <span class="open">{render_port}</span><br>
            <b>Scan Status:</b> {'<span class="done">✅ Complete!</span>' if scan_done else '<span class="waiting">⏳ Scanning... (page auto-refreshes every 5s)</span>'}
        </div>
    """

    if scan_done:
        html += f"""
        <div class="box">
            <h2>📋 Connect Scan — Open Ports</h2>
            <p><span class="open">{open_ports if open_ports else 'None found'}</span></p>
        </div>
        <div class="box">
            <h2>🔓 Bind Scan — Ports YOU Can Use</h2>
        """
        common = [80, 443, 3000, 4000, 5000, 8000, 8080, 8443, 8888, 9000, 10000]
        for p in common:
            status = f'<span class="open">✅ Available</span>' if p in bindable_ports else f'<span class="closed">❌ Blocked</span>'
            html += f"<div>Port {p} — {status}</div>"
        html += f"""
        </div>
        <div class="box">
            <h2>💡 Use This In Your Flask App</h2>
            <code>port = int(os.environ.get('PORT', 10000))<br>
            app.run(host='0.0.0.0', port=port)</code>
        </div>
        """

    html += "</body></html>"
    return html

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"\n🚀 Starting on port {port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)
