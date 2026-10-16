"""Master Trust (Tradelab platform) hosts and the one request helper every module uses."""

import httpx

from utils.httpx_client import get_httpx_client
from utils.logging import get_logger
from utils.thread_safe_cache import LockedTTLCache

logger = get_logger(__name__)

BASE_URL = "https://masterswift-beta.mastertrust.co.in"
WS_URL = "wss://masterswift-beta.mastertrust.co.in/ws/v1/feeds"
CONTRACT_URL = f"{BASE_URL}/api/v1/contract/Compact?info=download&exchanges=NSE,NFO,BSE,BFO,MCX"

# Tradelab wants the trading account code (client_id) on most calls. It is
# resolved once per access token from /user/trading_info; tokens roll daily.
_CLIENT_IDS = LockedTTLCache(maxsize=8, ttl=12 * 3600)


def api_request(method, path, auth, params=None, payload=None, timeout=15.0):
    """Send one request and return (http_status, body_dict).

    Never raises for an HTTP or transport error: the body becomes
    {"status": "error", "message": ...} so callers have a single shape to read.
    """
    headers = {"Authorization": f"Bearer {auth}", "Content-Type": "application/json"}
    # The beta host now and then never answers one request (ReadTimeout on
    # /charts/tdv) while the next goes through in under a second. Retry reads
    # once; never writes, so a slow order placement cannot be sent twice.
    attempts = 2 if method == "GET" else 1
    for attempt in range(attempts):
        try:
            response = get_httpx_client().request(
                method,
                f"{BASE_URL}{path}",
                headers=headers,
                params=params,
                json=payload,
                timeout=timeout,
            )
            break
        except httpx.TransportError as e:
            if attempt + 1 < attempts:
                logger.warning(f"Master Trust {method} {path} failed ({e!r}); retrying")
                continue
            logger.exception(f"Master Trust {method} {path} failed")
            return 503, {"status": "error", "message": f"Could not reach Master Trust: {e}"}
        except httpx.HTTPError as e:
            logger.exception(f"Master Trust {method} {path} failed")
            return 503, {"status": "error", "message": f"Could not reach Master Trust: {e}"}
    try:
        body = response.json()
    except ValueError:
        body = {"status": "error", "message": response.text[:200] or f"HTTP {response.status_code}"}
    if response.status_code == 401:
        body = {"status": "error", "message": "Master Trust session expired. Please log in again."}
    return response.status_code, body


def get_client_id(auth):
    """Trading account code for this access token, or None when it cannot be read."""
    client_id = _CLIENT_IDS.get(auth)
    if client_id:
        return client_id
    status, body = api_request("GET", "/api/v1/user/trading_info", auth)
    client_id = (
        (body.get("data") or {}).get("client_id") if body.get("status") == "success" else None
    )
    if client_id:
        _CLIENT_IDS[auth] = client_id
    else:
        logger.error(f"Master Trust trading_info failed ({status}): {body.get('message')}")
    return client_id
