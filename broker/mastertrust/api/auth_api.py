"""Master Trust login: OAuth2 authorization code (Tradelab), exchanged at /oauth2/token."""

import os

import httpx

from broker.mastertrust.api.baseurl import BASE_URL, get_client_id
from utils.config import get_broker_api_key, get_broker_api_secret
from utils.httpx_client import get_httpx_client
from utils.logging import get_logger

logger = get_logger(__name__)


def authenticate_broker(auth_code, state=None):
    """Exchange the OAuth code for an access token.

    Returns (access_token, feed_token, client_id, error). feed_token is always
    None: the market-data socket takes the same access token.
    """
    if not auth_code:
        return (
            None,
            None,
            None,
            "Master Trust did not send an authorization code. Please log in again.",
        )

    client_id = get_broker_api_key()
    client_secret = get_broker_api_secret()
    if not client_id or not client_secret:
        return (
            None,
            None,
            None,
            "Set BROKER_API_KEY (OAuth client id) and BROKER_API_SECRET in .env.",
        )

    redirect_uri = os.getenv("REDIRECT_URL", "http://127.0.0.1:5000/mastertrust/callback")
    try:
        # Tradelab expects the client credentials as a Basic auth header.
        response = get_httpx_client().post(
            f"{BASE_URL}/oauth2/token",
            auth=(client_id, client_secret),
            data={
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": redirect_uri,
            },
            timeout=15.0,
        )
        token_data = response.json()
    except (httpx.HTTPError, ValueError) as e:
        logger.exception("Master Trust token exchange failed")
        return None, None, None, f"Could not reach Master Trust to complete login: {e}"

    access_token = token_data.get("access_token")
    if response.status_code != 200 or not access_token:
        message = (
            token_data.get("error_description")
            or token_data.get("message")
            or token_data.get("error")
        )
        logger.error(f"Master Trust token exchange rejected ({response.status_code}): {token_data}")
        return None, None, None, f"Master Trust rejected the login: {message or 'unknown reason'}"

    account = get_client_id(access_token)
    if not account:
        return (
            None,
            None,
            None,
            "Logged in, but Master Trust did not return your client ID. Please try again.",
        )
    return access_token, None, account, None
