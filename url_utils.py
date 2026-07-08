"""
url_utils.py — Input-flow helper for the Manobhava-AI Website Product Analyzer
================================================================================

This module has exactly one job: decide whether the raw text a user typed
into the input box is a **website URL** or a **company name**, and if it's
a URL, turn it into a clean, normalized, fetchable form.

It is intentionally standalone and side-effect free (no network calls, no
imports from company_discovery / product_discovery / sentiment / dashboard)
so it can be dropped into the input step of app.py without touching any of
those modules.

Public functions
-----------------
    is_url(text: str) -> bool
        True if `text` looks like a website URL rather than a company name.

    normalize_url(text: str) -> str
        Normalizes a URL-like input into a canonical form:
            headphonezone.in        -> https://www.headphonezone.in
            www.apple.com           -> https://www.apple.com
            http://nike.com         -> http://nike.com   (scheme left as-is)
            shop.example.co.uk      -> https://shop.example.co.uk (subdomain kept)
        Returns "" if the text cannot be turned into a valid URL.

    derive_company_name(url: str) -> str
        Best-effort brand/display name guessed from the domain, used only
        for labeling (download folder names, summary text) when Company
        Discovery is skipped because a URL was supplied directly.
"""

import re
from urllib.parse import urlparse, urlunparse

_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+\-.]*://")

# Matches bare domains like "apple.com", "headphonezone.in", "shop.example.co.uk"
# (one or more "label." groups followed by a final alphabetic TLD of length >= 2).
_BARE_DOMAIN_RE = re.compile(
    r"^([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$"
)


def is_url(text: str) -> bool:
    """Return True if `text` looks like a website URL rather than a company name."""
    value = (text or "").strip()
    if not value or " " in value:
        return False

    if _SCHEME_RE.match(value):
        return True

    if value.lower().startswith("www."):
        return True

    # Strip any path / query / fragment / port before checking the host part
    host_part = value.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    host_part = host_part.split(":", 1)[0]

    return bool(_BARE_DOMAIN_RE.match(host_part))


def normalize_url(text: str) -> str:
    """
    Normalize a URL-like input into a canonical, fetchable form.

    - Adds "https://" if no scheme was given.
    - Adds "www." to bare root domains (e.g. apple.com -> www.apple.com)
      but leaves existing subdomains alone (shop.apple.com stays as-is).
    - Returns "" if the input cannot be turned into a valid URL.
    """
    value = (text or "").strip()
    if not value:
        return ""

    if not _SCHEME_RE.match(value):
        value = f"https://{value}"

    parsed = urlparse(value)
    host = parsed.netloc
    if not host or "." not in host:
        return ""

    if not host.lower().startswith("www."):
        labels = host.split(".")
        if len(labels) == 2:  # root domain only, e.g. "apple.com"
            host = f"www.{host}"

    normalized = parsed._replace(netloc=host)
    result = urlunparse(normalized)
    return result.rstrip("/")


def derive_company_name(url: str) -> str:
    """
    Best-effort brand name guess from a normalized URL. Used only for
    labeling (download folder names, executive summary text) when Company
    Discovery is skipped because the user supplied a website URL directly.
    """
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0]
    core = host.split(".")[0] if host else ""
    return core.replace("-", " ").replace("_", " ").title() or "Website"
