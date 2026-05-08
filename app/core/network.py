from __future__ import annotations

import socket


def detect_local_network_host() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            host = sock.getsockname()[0]
            if host:
                return host
    except OSError:
        pass

    return "127.0.0.1"
