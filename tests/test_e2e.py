"""
End-to-end tests across all Ghostie microservices.

Runs against live deployed APIs after the middleware is deployed,
verifying the full ecosystem is healthy and data flows correctly
between services.

Services under test:
  - Ghostie Middleware          (this repo)
  - Ghostie Data Collection     (Ghostie_data-collection)
  - Ghostie Data Retrieval      (Ghostie_data-retrieval)
  - Ghostie Analytical Model    (Ghostie_analytical-model)
  - Charlie API                 (external group — auth, reddit posts, events)
"""

import os

import pytest



# ── Health checks ──────────────────────────────────────────────────────────


class TestHealthChecks:
    """Verify every service in the Ghostie ecosystem is reachable."""

    def test_middleware_api(self, client, middleware_url):
        r = client.get(f"{middleware_url}/api")
        assert r.status_code == 200
        assert "message" in r.json()

    def test_data_collection_health(self, client, data_collection_url):
        r = client.get(f"{data_collection_url}/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

    def test_data_retrieval_health(self, client, data_retrieval_url):
        r = client.get(f"{data_retrieval_url}/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "healthy"
        assert body["dynamodb"] == "ACTIVE"

    def test_analytical_model_health(self, client, analytical_model_url):
        r = client.get(f"{analytical_model_url}/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


# ── Service info / root endpoints ──────────────────────────────────────────


class TestServiceInfo:
    """Verify root endpoints return correct service metadata."""

    def test_data_collection_root(self, client, data_collection_url):
        r = client.get(f"{data_collection_url}/")
        assert r.status_code == 200
        body = r.json()
        assert body["service"] == "Ghostie Data Collection API"
        assert "version" in body

    def test_data_retrieval_root(self, client, data_retrieval_url):
        r = client.get(f"{data_retrieval_url}/")
        assert r.status_code == 200
        body = r.json()
        assert body["service"] == "Ghostie Data Retrieval API"
        assert "version" in body

    def test_analytical_model_root(self, client, analytical_model_url):
        r = client.get(f"{analytical_model_url}/")
        assert r.status_code == 200
        body = r.json()
        assert body["service"] == "Ghostie Analytical Model API"
        assert "version" in body


# ── Data Retrieval service ─────────────────────────────────────────────────


class TestDataRetrieval:
    """Verify the data retrieval service responds correctly."""

    def test_companies_returns_list(self, client, data_retrieval_url):
        r = client.get(f"{data_retrieval_url}/companies")
        assert r.status_code == 200
        body = r.json()
        assert "count" in body
        assert "companies" in body
        assert isinstance(body["companies"], list)

    def test_retrieve_missing_params_returns_422(self, client, data_retrieval_url):
        r = client.get(f"{data_retrieval_url}/retrieve")
        assert r.status_code == 422

    def test_retrieve_unknown_business_returns_404(self, client, data_retrieval_url):
        r = client.get(
            f"{data_retrieval_url}/retrieve",
            params={
                "business_name": "NonexistentTestBusiness99999",
                "location": "Nowhere",
                "category": "none",
            },
        )
        assert r.status_code == 404

    def test_retrieve_by_invalid_hash_returns_404(self, client, data_retrieval_url):
        r = client.get(f"{data_retrieval_url}/retrieve/0000000000000000000000000000000000000000000000000000000000000000")
        assert r.status_code == 404


# ── Analytical Model service ───────────────────────────────────────────────


class TestAnalyticalModel:
    """Verify the analytical model service responds correctly."""

    def test_analyse_positive_text(self, client, analytical_model_url):
        r = client.get(
            f"{analytical_model_url}/analyse",
            params={"text": "This restaurant is absolutely wonderful, great food and amazing service!"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["sentiment"] in ("positive", "negative", "neutral")
        assert 0 <= body["score"] <= 100
        assert 1 <= body["rating"] <= 5

    def test_analyse_negative_text(self, client, analytical_model_url):
        r = client.get(
            f"{analytical_model_url}/analyse",
            params={"text": "Terrible experience, the food was cold and the staff was extremely rude."},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["sentiment"] in ("positive", "negative", "neutral")
        assert 0 <= body["score"] <= 100
        assert 1 <= body["rating"] <= 5

    def test_analyse_missing_text_returns_422(self, client, analytical_model_url):
        r = client.get(f"{analytical_model_url}/analyse")
        assert r.status_code == 422

    def test_leaderboard_returns_list(self, client, analytical_model_url):
        r = client.get(f"{analytical_model_url}/leaderboard")
        assert r.status_code == 200
        body = r.json()
        assert "leaderboard" in body
        assert isinstance(body["leaderboard"], list)

    def test_history_missing_params_returns_422(self, client, analytical_model_url):
        r = client.get(f"{analytical_model_url}/history")
        assert r.status_code == 422


# ── Data Collection service ────────────────────────────────────────────────


class TestDataCollection:
    """Verify the data collection service responds correctly."""

    def test_results_endpoint(self, client, data_collection_url):
        r = client.get(f"{data_collection_url}/results")
        assert r.status_code == 200
        body = r.json()
        assert "count" in body
        assert "files" in body

    def test_collect_missing_body_returns_422(self, client, data_collection_url):
        r = client.post(f"{data_collection_url}/collect")
        assert r.status_code == 422


# ── End-to-end pipeline ───────────────────────────────────────────────────


class TestEndToEndPipeline:
    """
    Integration tests that verify data flows between services.

    Uses existing data already in DynamoDB to avoid triggering
    expensive external API calls (NewsAPI / SerpAPI).
    """

    def test_retrieve_and_analyse_existing_business(
        self, client, data_retrieval_url, analytical_model_url
    ):
        """
        Full pipeline: retrieval → analytical model.

        1. List companies from the retrieval service
        2. Retrieve data for the first company
        3. Run sentiment analysis on that company
        4. Verify the company appears in history
        5. Verify the leaderboard is populated
        """
        # Step 1 — get companies
        companies_resp = client.get(f"{data_retrieval_url}/companies")
        assert companies_resp.status_code == 200
        companies = companies_resp.json()

        if companies["count"] == 0:
            pytest.skip("No companies in retrieval DB — cannot run pipeline test")

        company = companies["companies"][0]
        biz = company["business_name"]
        loc = company["location"]
        cat = company["category"]

        # Step 2 — retrieve stored data
        retrieve_resp = client.get(
            f"{data_retrieval_url}/retrieve",
            params={"business_name": biz, "location": loc, "category": cat},
        )
        assert retrieve_resp.status_code == 200
        assert retrieve_resp.json()["status"] in ("NEW DATA", "NO NEW DATA")

        # Step 3 — sentiment analysis (calls retrieval internally)
        sentiment_resp = client.get(
            f"{analytical_model_url}/sentiment",
            params={"business_name": biz, "location": loc, "category": cat},
        )
        assert sentiment_resp.status_code == 200
        sentiment = sentiment_resp.json()
        assert sentiment["business_name"] == biz
        assert sentiment["overall_sentiment"] in ("positive", "negative", "neutral")
        assert 1 <= sentiment["overall_rating"] <= 5
        assert 0 <= sentiment["overall_score"] <= 100
        assert sentiment["items_analysed"] > 0
        assert isinstance(sentiment["breakdown"], list)
        assert len(sentiment["breakdown"]) > 0

        # Step 4 — verify history was recorded
        history_resp = client.get(
            f"{analytical_model_url}/history",
            params={"business_name": biz, "location": loc, "category": cat},
        )
        assert history_resp.status_code == 200
        history = history_resp.json()
        assert history["count"] >= 1
        assert len(history["results"]) >= 1

        # Step 5 — leaderboard should have at least one entry
        leaderboard_resp = client.get(f"{analytical_model_url}/leaderboard")
        assert leaderboard_resp.status_code == 200
        assert len(leaderboard_resp.json()["leaderboard"]) >= 1


# ── Charlie API [EXTERNAL] — Auth ─────────────────────────────────────────


class TestExternalCharlieAuth:
    """[EXTERNAL API] Charlie API — Authentication flow."""

    def test_signup_returns_token_and_user(self, charlie_auth):
        body = charlie_auth["signup_body"]
        assert "token" in body
        assert "user" in body
        assert "id" in body["user"]
        assert "username" in body["user"]
        assert "email" in body["user"]

    def test_login_returns_token_and_user(self, charlie_auth):
        body = charlie_auth["login_body"]
        assert charlie_auth["login_status"] == 200
        assert isinstance(body["token"], str)
        assert len(body["token"]) > 0
        assert "user" in body
        assert "id" in body["user"]
        assert "username" in body["user"]
        assert "email" in body["user"]

    def test_login_user_matches_credentials(self, charlie_auth):
        user = charlie_auth["user"]
        expected_email = os.environ.get("CHARLIE_API_EMAIL", "")
        assert user["email"] == expected_email

    def test_auth_me_returns_user(self, client, charlie_api_url, charlie_headers, charlie_auth):
        r = client.get(f"{charlie_api_url}/v1/auth/me", headers=charlie_headers)
        assert r.status_code == 200
        body = r.json()
        assert "user" in body
        assert body["user"]["id"] == charlie_auth["user"]["id"]
        assert body["user"]["email"] == charlie_auth["user"]["email"]
        assert body["user"]["username"] == charlie_auth["user"]["username"]

    def test_auth_me_rejects_no_token(self, client, charlie_api_url):
        r = client.get(f"{charlie_api_url}/v1/auth/me")
        assert r.status_code == 401


# ── Charlie API [EXTERNAL] — Reddit Posts ─────────────────────────────────


class TestExternalCharlieRedditPosts:
    """[EXTERNAL API] Charlie API — Reddit post search and comments."""

    def test_search_posts(self, client, charlie_api_url, charlie_headers):
        r = client.get(
            f"{charlie_api_url}/v1/post/search",
            params={"query": "wuhan", "subreddit": "worldnews", "limit": 2},
            headers=charlie_headers,
        )
        assert r.status_code == 200
        body = r.json()
        assert "data" in body
        assert "events" in body["data"]
        assert isinstance(body["data"]["events"], list)

    def test_search_posts_requires_auth(self, client, charlie_api_url):
        r = client.get(
            f"{charlie_api_url}/v1/post/search",
            params={"query": "test"},
        )
        assert r.status_code == 401

    def test_get_comments(self, client, charlie_api_url, charlie_headers):
        # First get a post to find a link_id
        search_resp = client.get(
            f"{charlie_api_url}/v1/post/search",
            params={"query": "wuhan", "subreddit": "worldnews", "limit": 1},
            headers=charlie_headers,
        )
        assert search_resp.status_code == 200
        events = search_resp.json().get("data", {}).get("events", [])

        if not events:
            pytest.skip("No posts returned — cannot test comments")

        post_id = events[0]["attributes"]["id"]

        r = client.get(
            f"{charlie_api_url}/v1/post/comments",
            params={"link_id": post_id, "limit": 2},
            headers=charlie_headers,
        )
        assert r.status_code == 200
        body = r.json()
        assert "data" in body
        assert "events" in body["data"]

    def test_comments_missing_link_id_returns_400(self, client, charlie_api_url, charlie_headers):
        r = client.get(
            f"{charlie_api_url}/v1/post/comments",
            headers=charlie_headers,
        )
        assert r.status_code == 400


# NOTE: Charlie API internal routes (/v1/events/*) are NOT tested here.
# They require elevated permissions beyond standard signup/login auth.
# See: "Internal Routes (Email z5479862@ad.unsw.edu.au to access)"


# ── Endpoint coverage gate ─────────────────────────────────────────────────


class TestEndpointCoverage:
    """
    Runs LAST (alphabetically after all other test classes).

    Fails if any endpoint in REQUIRED_ENDPOINTS was never called,
    ensuring every route is exercised at least once.
    """

    def test_all_endpoints_were_called(self, endpoint_tracker):
        missing = endpoint_tracker.uncovered()
        if missing:
            report = "\n".join(f"  - {ep}" for ep in missing)
            pytest.fail(
                f"The following endpoints were never called during the test run:\n{report}\n"
                "Add tests that hit these endpoints."
            )
