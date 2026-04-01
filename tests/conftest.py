"""
Fixtures for Ghostie end-to-end tests.

Base URLs are resolved from environment variables, which are set in the
GitHub Actions workflow by querying CloudFormation stack outputs.
"""

import os
from urllib.parse import urlparse

import httpx
import pytest


# ── pytest-html report customisation ──────────────────────────────────────


def pytest_html_results_table_row(report, cells):
    """Tag Charlie API tests with [EXTERNAL API] in the report table."""
    if "External" in report.nodeid:
        # Prepend marker to the test name cell (first cell after result)
        for cell in cells:
            if hasattr(cell, "text") and "External" in str(cell):
                break


def pytest_collection_modifyitems(items):
    """Add 'external' marker to all TestExternal* classes."""
    for item in items:
        if "External" in item.nodeid:
            item.add_marker(pytest.mark.external)


def pytest_html_report_title(report):
    report.title = "Ghostie E2E Test Report"


# ── Endpoint coverage tracking ─────────────────────────────────────────────

# Every endpoint across all services that must be hit at least once.
REQUIRED_ENDPOINTS = {
    # Middleware
    ("GET", "/api"),
    # Data Collection
    ("GET", "/health", "collection"),
    ("GET", "/", "collection"),
    ("GET", "/results"),
    ("POST", "/collect"),
    # Data Retrieval
    ("GET", "/health", "retrieval"),
    ("GET", "/", "retrieval"),
    ("GET", "/companies"),
    ("GET", "/retrieve"),
    ("GET", "/retrieve/{hash_key}"),
    # Analytical Model
    ("GET", "/health", "analytical"),
    ("GET", "/", "analytical"),
    ("GET", "/analyse"),
    ("GET", "/sentiment"),
    ("GET", "/leaderboard"),
    ("GET", "/history"),
    # Charlie API (external group)
    ("POST", "/v1/auth/signup"),
    ("POST", "/v1/auth/login"),
    ("GET", "/v1/auth/me"),
    ("GET", "/v1/post/search"),
    ("GET", "/v1/post/comments"),
    ("GET", "/v1/events"),
    ("GET", "/v1/events/{eventId}"),
    ("GET", "/v1/events/{eventId}/posts"),
    ("GET", "/v1/events/{eventId}/snapshots"),
    ("POST", "/v1/events/subscribe"),
}


class EndpointTracker:
    """Records every (method, path) pair the test suite calls."""

    def __init__(self):
        self.called: set[tuple[str, str]] = set()

    def record(self, method: str, url: str):
        path = urlparse(url).path
        # Strip the /Prod prefix that API Gateway adds
        if path.startswith("/Prod"):
            path = path[len("/Prod"):]
        if not path:
            path = "/"
        self.called.add((method.upper(), path))

    def uncovered(self) -> list[str]:
        """Return human-readable list of endpoints never called."""
        # Flatten REQUIRED_ENDPOINTS to just (method, path) for matching
        required_paths = {(ep[0], ep[1]) for ep in REQUIRED_ENDPOINTS}
        missing = []
        for method, path in sorted(required_paths):
            # For parameterised paths like /retrieve/{hash_key},
            # check if any called path starts with the prefix
            if "{" in path:
                prefix = path.split("{")[0]
                if not any(m == method and p.startswith(prefix) and p != prefix.rstrip("/")
                           for m, p in self.called):
                    missing.append(f"{method} {path}")
            else:
                if (method, path) not in self.called:
                    missing.append(f"{method} {path}")
        return missing


_tracker = EndpointTracker()


def get_tracker() -> EndpointTracker:
    return _tracker


@pytest.fixture(scope="session")
def endpoint_tracker():
    """Expose the endpoint tracker to tests."""
    return _tracker


# ── HTTP client with tracking ──────────────────────────────────────────────


class TrackingClient:
    """Wraps httpx.Client to record every request for coverage checks."""

    def __init__(self, inner: httpx.Client):
        self._inner = inner

    def _track(self, method: str, url: str, response: httpx.Response) -> httpx.Response:
        _tracker.record(method, str(response.request.url))
        return response

    def get(self, url, **kwargs) -> httpx.Response:
        r = self._inner.get(url, **kwargs)
        return self._track("GET", url, r)

    def post(self, url, **kwargs) -> httpx.Response:
        r = self._inner.post(url, **kwargs)
        return self._track("POST", url, r)

    def put(self, url, **kwargs) -> httpx.Response:
        r = self._inner.put(url, **kwargs)
        return self._track("PUT", url, r)

    def patch(self, url, **kwargs) -> httpx.Response:
        r = self._inner.patch(url, **kwargs)
        return self._track("PATCH", url, r)

    def delete(self, url, **kwargs) -> httpx.Response:
        r = self._inner.delete(url, **kwargs)
        return self._track("DELETE", url, r)


@pytest.fixture(scope="session")
def client():
    """Shared HTTP client with tracking and generous timeout for cold starts."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as c:
        yield TrackingClient(c)


# ── Base URL fixtures ──────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def middleware_url():
    url = os.environ.get("MIDDLEWARE_URL", "")
    if not url:
        pytest.skip("MIDDLEWARE_URL not set")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def data_collection_url():
    url = os.environ.get("DATA_COLLECTION_URL", "")
    if not url:
        pytest.skip("DATA_COLLECTION_URL not set")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def data_retrieval_url():
    url = os.environ.get("DATA_RETRIEVAL_URL", "")
    if not url:
        pytest.skip("DATA_RETRIEVAL_URL not set")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def analytical_model_url():
    url = os.environ.get("ANALYTICAL_MODEL_URL", "")
    if not url:
        pytest.skip("ANALYTICAL_MODEL_URL not set")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def charlie_api_url():
    url = os.environ.get("CHARLIE_API_URL", "")
    if not url:
        pytest.skip("CHARLIE_API_URL not set")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def charlie_auth(client, charlie_api_url):
    """
    Sign up (idempotent) then login to Charlie API.

    Returns dict with all auth context:
        token, user (id, username, email),
        signup_response, login_response
    """
    email = os.environ.get("CHARLIE_API_EMAIL", "")
    password = os.environ.get("CHARLIE_API_PASSWORD", "")
    username = os.environ.get("CHARLIE_API_USERNAME", "ghostie_e2e")
    if not email or not password:
        pytest.skip("CHARLIE_API_EMAIL / CHARLIE_API_PASSWORD not set")

    # Sign up first — safe to call every run, won't error if account exists
    signup_resp = client.post(
        f"{charlie_api_url}/v1/auth/signup",
        json={"username": username, "email": email, "password": password},
    )

    # Login to get the JWT
    login_resp = client.post(
        f"{charlie_api_url}/v1/auth/login",
        json={"email": email, "password": password},
    )
    assert login_resp.status_code == 200, f"Charlie login failed: {login_resp.text}"

    login_body = login_resp.json()
    return {
        "token": login_body["token"],
        "user": login_body["user"],
        "signup_status": signup_resp.status_code,
        "signup_body": signup_resp.json(),
        "login_status": login_resp.status_code,
        "login_body": login_body,
    }


@pytest.fixture(scope="session")
def charlie_auth_token(charlie_auth):
    return charlie_auth["token"]


@pytest.fixture(scope="session")
def charlie_headers(charlie_auth_token):
    """Authorization headers for Charlie API requests."""
    return {"Authorization": f"Bearer {charlie_auth_token}"}
