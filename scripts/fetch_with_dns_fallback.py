#!/usr/bin/env python3
"""Small urllib compatibility helper for environments with DNS sinkholing."""

from __future__ import annotations

import json
import socket
import threading
from contextlib import contextmanager
from typing import Dict, Optional
from urllib import request as urllib_request
from urllib.parse import urlsplit


_DNS_CACHE: Dict[str, str | None] = {}
_CACHE_LOCK = threading.Lock()
_GETADDRINFO_LOCK = threading.Lock()


def _is_local_blocked_host(host: str) -> bool:
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        return False
    return ip == "0.0.0.0" or ip.startswith("127.")


def _resolve_public_dns(host: str, timeout: float = 3.0) -> Optional[str]:
    with _CACHE_LOCK:
        if host in _DNS_CACHE:
            return _DNS_CACHE[host]
    try:
        url = f"https://dns.google/resolve?name={host}&type=A"
        with urllib_request.urlopen(url, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
        for answer in payload.get("Answer", []):
            if answer.get("type") == 1:
                ip = answer.get("data")
                if isinstance(ip, str) and ip and ip != "0.0.0.0":
                    with _CACHE_LOCK:
                        _DNS_CACHE[host] = ip
                    return ip
    except Exception:
        pass
    with _CACHE_LOCK:
        _DNS_CACHE[host] = None
    return None


@contextmanager
def _patched_getaddrinfo(host: str, ip: str):
    original = socket.getaddrinfo

    def getaddrinfo(hostname, *args, **kwargs):
        if hostname == host:
            return original(ip, *args, **kwargs)
        return original(hostname, *args, **kwargs)

    socket.getaddrinfo = getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original


def open_url(url: str, request_kwargs: Dict[str, object] | None = None, timeout: float = 30.0):
    req = urllib_request.Request(url, **(request_kwargs or {}))
    parsed = urlsplit(url)
    host = parsed.hostname
    if not host:
        return urllib_request.urlopen(req, timeout=timeout)

    if not _is_local_blocked_host(host):
        return urllib_request.urlopen(req, timeout=timeout)

    fallback_ip = _resolve_public_dns(host)
    if not fallback_ip:
        return urllib_request.urlopen(req, timeout=timeout)

    with _GETADDRINFO_LOCK:
        with _patched_getaddrinfo(host, fallback_ip):
            return urllib_request.urlopen(req, timeout=timeout)
