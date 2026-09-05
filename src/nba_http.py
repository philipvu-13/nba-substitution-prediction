import requests


NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/151.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_configured = False


def configure_nba_http() -> None:
    global _configured

    if _configured:
        return

    original_request = requests.sessions.Session.request

    def request_with_nba_headers(
        session,
        method,
        url,
        **kwargs,
    ):
        if "stats.nba.com" in url:
            headers = NBA_HEADERS.copy()
            headers.update(kwargs.get("headers") or {})
            kwargs["headers"] = headers

        return original_request(
            session,
            method,
            url,
            **kwargs,
        )

    requests.sessions.Session.request = request_with_nba_headers
    _configured = True