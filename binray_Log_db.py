import socket
import os
import threading
from datetime import datetime

open_ports = []
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
                print(f"  ✅ OPEN  → Port {port}")
    except:
        pass

def main():
    print("=" * 50)
    print("  Full Port Scanner — 1 to 65535")
    print(f"  Started: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 50)

    render_port = os.environ.get("PORT")
    if render_port:
        print(f"\n🔵 Render $PORT = {render_port}\n")

    print("Scanning all 65535 ports (threaded)...\n")

    threads = []
    for port in range(1, 65536):
        t = threading.Thread(target=scan_port, args=(port,))
        t.daemon = True
        threads.append(t)
        t.start()

        # Batch of 500 threads at a time
        if len(threads) >= 500:
            for th in threads:
                th.join()
            threads = []

    # Wait for remaining
    for t in threads:
        t.join()

    open_ports.sort()

    print("\n" + "=" * 50)
    print(f"  Scan Complete: {datetime.now().strftime('%H:%M:%S')}")
    print(f"  Total Open Ports: {len(open_ports)}")
    print("=" * 50)
    if open_ports:
        print(f"\n📋 Open Ports List:\n{open_ports}")
    else:
        print("\n⚠️  No open ports found via connect scan.")
        print("    Try bind scan below ↓")

    # Also try bind scan
    print("\n" + "=" * 50)
    print("  Bind Scan (ports available to use)")
    print("=" * 50)
    bindable = []
    common = [80, 443, 3000, 4000, 5000, 8000, 8080, 8443, 8888, 9000, 10000]
    for port in common:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", port))
            s.close()
            bindable.append(port)
            print(f"  ✅ Can bind → {port}")
        except:
            print(f"  ❌ Blocked  → {port}")

    print(f"\n✅ Bindable ports: {bindable}")
    print("\n💡 Use in Flask:")
    print("   port = int(os.environ.get('PORT', 10000))")
    print("   app.run(host='0.0.0.0', port=port)")
    print("=" * 50)

if __name__ == "__main__":
    main()
