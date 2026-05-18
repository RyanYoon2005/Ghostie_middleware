import asyncio
import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Annotated

import bcrypt
import boto3
import jwt
import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from mangum import Mangum
from pydantic import BaseModel

# ── App setup ──────────────────────────────────────────────────────────────

app = FastAPI(title="Ghostie Middleware", version="1.0.0", root_path="/Prod")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config from environment ────────────────────────────────────────────────

CHARLIE_API_URL = os.environ.get("CHARLIE_API_URL", "")
DATA_COLLECTION_URL = os.environ.get("DATA_COLLECTION_URL", "")
DATA_RETRIEVAL_URL = os.environ.get("DATA_RETRIEVAL_URL", "")
ANALYTICAL_MODEL_URL = os.environ.get("ANALYTICAL_MODEL_URL", "")

USERS_TABLE = os.environ.get("USERS_TABLE", "users")
AWS_REGION = os.environ.get("AWS_REGION", "ap-southeast-2")

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = 86400  # 24 hours

# ── DynamoDB ───────────────────────────────────────────────────────────────

_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
_users = _dynamodb.Table(USERS_TABLE)

# ── Hop-by-hop headers to strip when proxying ──────────────────────────────

_HOP_BY_HOP = {
    "transfer-encoding",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "upgrade",
    "content-encoding",
    "content-length",
}

# ── Pydantic models ────────────────────────────────────────────────────────


class SignupRequest(BaseModel):
    email: str
    username: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class ComparisonBusiness(BaseModel):
    business_name: str
    location: str
    category: str


class SaveComparisonRequest(BaseModel):
    name: str
    businesses: list[ComparisonBusiness]


# ── JWT helpers ────────────────────────────────────────────────────────────


def _create_token(email: str, username: str) -> str:
    payload = {
        "sub": email,
        "username": username,
        "iat": int(time.time()),
        "exp": int(time.time()) + JWT_EXPIRY_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ── Auth dependency ────────────────────────────────────────────────────────

_bearer = HTTPBearer()


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_bearer)],
) -> dict:
    """Decode our JWT and return the user payload (email + username)."""
    return _decode_token(credentials.credentials)


# ── Auth endpoints ─────────────────────────────────────────────────────────


@app.post("/auth/signup", status_code=201)
def signup(body: SignupRequest):
    existing = _users.get_item(Key={"email": body.email}).get("Item")
    if existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    hashed = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    _users.put_item(Item={
        "email": body.email,
        "username": body.username,
        "password": hashed,
    })

    token = _create_token(body.email, body.username)
    return {"token": token, "user": {"email": body.email, "username": body.username}}


@app.post("/auth/refresh")
def refresh(user: Annotated[dict, Depends(get_current_user)]):
    token = _create_token(user["sub"], user["username"])
    return {"token": token}


@app.post("/auth/login")
def login(body: LoginRequest):
    item = _users.get_item(Key={"email": body.email}).get("Item")
    if not item:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not bcrypt.checkpw(body.password.encode(), item["password"].encode()):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _create_token(body.email, item["username"])
    return {"token": token, "user": {"email": body.email, "username": item["username"]}}


# ── User endpoints ─────────────────────────────────────────────────────────


@app.get("/users/me")
def get_me(user: Annotated[dict, Depends(get_current_user)]):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="User not found")
    return {"email": item["email"], "username": item["username"]}


@app.get("/users/me/past")
def get_past(user: Annotated[dict, Depends(get_current_user)]):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="User not found")
    return {"past": list(item.get("past", []))}


@app.get("/users/me/favourites")
def get_favourites(user: Annotated[dict, Depends(get_current_user)]):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="User not found")
    return {"favourited": list(item.get("favourited", []))}


@app.post("/users/me/favourites/{business_key}", status_code=200)
def add_favourite(business_key: str, user: Annotated[dict, Depends(get_current_user)]):
    _users.update_item(
        Key={"email": user["sub"]},
        UpdateExpression="SET favourited = list_append(if_not_exists(favourited, :empty), :key)",
        ExpressionAttributeValues={":key": [business_key], ":empty": []},
    )
    return {"message": "Added to favourites", "business_key": business_key}


@app.delete("/users/me/favourites/{business_key}", status_code=200)
def remove_favourite(business_key: str, user: Annotated[dict, Depends(get_current_user)]):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item", {})
    favourited = [k for k in item.get("favourited", []) if k != business_key]
    _users.update_item(
        Key={"email": user["sub"]},
        UpdateExpression="SET favourited = :val",
        ExpressionAttributeValues={":val": favourited},
    )
    return {"message": "Removed from favourites", "business_key": business_key}


# ── Comparisons endpoints ─────────────────────────────────────────────────


@app.post("/users/me/comparisons", status_code=201)
def save_comparison(
    body: SaveComparisonRequest,
    user: Annotated[dict, Depends(get_current_user)],
):
    if not body.name.strip():
        raise HTTPException(status_code=422, detail="Comparison name must not be empty")
    if len(body.businesses) < 2:
        raise HTTPException(status_code=422, detail="A comparison needs at least 2 businesses")
    if len(body.businesses) > 5:
        raise HTTPException(status_code=422, detail="A comparison supports at most 5 businesses")

    comparison = {
        "comparison_id": uuid.uuid4().hex,
        "name": body.name.strip(),
        "businesses": [b.model_dump() for b in body.businesses],
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    _users.update_item(
        Key={"email": user["sub"]},
        UpdateExpression="SET comparisons = list_append(if_not_exists(comparisons, :empty), :item)",
        ExpressionAttributeValues={":item": [comparison], ":empty": []},
    )
    return comparison


@app.get("/users/me/comparisons")
def list_comparisons(user: Annotated[dict, Depends(get_current_user)]):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="User not found")
    return {"comparisons": list(item.get("comparisons", []))}


@app.get("/users/me/comparisons/{comparison_id}")
async def get_comparison(
    comparison_id: str,
    user: Annotated[dict, Depends(get_current_user)],
):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="User not found")

    comparison = next(
        (c for c in item.get("comparisons", []) if c["comparison_id"] == comparison_id),
        None,
    )
    if not comparison:
        raise HTTPException(status_code=404, detail="Comparison not found")

    # Fire sentiment analysis for all businesses concurrently and record each in past
    results = await asyncio.gather(
        *[_fetch_sentiment_for_business(b, user) for b in comparison["businesses"]]
    )

    return {
        "comparison_id": comparison["comparison_id"],
        "name": comparison["name"],
        "created_at": comparison["created_at"],
        "results": list(results),
    }


@app.delete("/users/me/comparisons/{comparison_id}", status_code=200)
def delete_comparison(
    comparison_id: str,
    user: Annotated[dict, Depends(get_current_user)],
):
    item = _users.get_item(Key={"email": user["sub"]}).get("Item", {})
    existing = item.get("comparisons", [])
    updated = [c for c in existing if c["comparison_id"] != comparison_id]

    if len(updated) == len(existing):
        raise HTTPException(status_code=404, detail="Comparison not found")

    _users.update_item(
        Key={"email": user["sub"]},
        UpdateExpression="SET comparisons = :val",
        ExpressionAttributeValues={":val": updated},
    )
    return {"message": "Comparison deleted", "comparison_id": comparison_id}


# ── Public health endpoint ─────────────────────────────────────────────────


@app.get("/api")
def my_api():
    return {"message": "Hello from Ghostie Middleware!"}


# ── ASX announcements endpoint ────────────────────────────────────────────


@app.get("/asx/announcements")
async def get_asx_announcements(
    business_name: str,
    location: str,
    category: str,
):
    """
    Return ASX announcements for a business, extracted from stored collected data.

    Filters items where source == 'asx_announcements' from the latest data snapshot
    in the retrieval service. Returns [] for companies that are not ASX-listed.
    No auth required — ASX data is public.
    """
    if not DATA_RETRIEVAL_URL:
        raise HTTPException(status_code=503, detail="Data retrieval service not configured")

    # Step 1: Fetch the latest stored data for this business
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            retrieve_resp = await client.get(
                f"{DATA_RETRIEVAL_URL.rstrip('/')}/retrieve",
                params={
                    "business_name": business_name,
                    "location": location,
                    "category": category,
                },
            )
    except Exception:
        raise HTTPException(status_code=503, detail="Could not reach data retrieval service")

    if retrieve_resp.status_code == 404:
        return {
            "ticker": None,
            "business_name": business_name,
            "announcements": [],
            "message": "No data collected for this business yet. Run /collect first.",
        }

    if retrieve_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Data retrieval service error")

    body = retrieve_resp.json()

    # Step 2: If NO NEW DATA, the full data array is not returned — fetch by hash_key
    if body.get("status") == "NO NEW DATA":
        hash_key = body.get("hash_key")
        if not hash_key:
            return {
                "ticker": None,
                "business_name": business_name,
                "announcements": [],
                "message": "No stored data available",
            }
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                hash_resp = await client.get(
                    f"{DATA_RETRIEVAL_URL.rstrip('/')}/retrieve/{hash_key}",
                )
            if hash_resp.status_code != 200:
                return {
                    "ticker": None,
                    "business_name": business_name,
                    "announcements": [],
                    "message": "Could not retrieve stored data",
                }
            body = hash_resp.json()
        except Exception:
            return {
                "ticker": None,
                "business_name": business_name,
                "announcements": [],
                "message": "Could not retrieve stored data",
            }

    # Step 3: Filter for ASX announcement items only
    data = body.get("data", [])
    asx_items = [item for item in data if item.get("source") == "asx_announcements"]

    if not asx_items:
        return {
            "ticker": None,
            "business_name": business_name,
            "location": location,
            "category": category,
            "total": 0,
            "market_sensitive_count": 0,
            "announcements": [],
            "message": "No ASX announcements found — this business may not be ASX-listed",
        }

    # Step 4: Extract ticker and format announcements
    ticker = asx_items[0].get("metadata", {}).get("ticker")

    announcements = []
    for item in asx_items:
        meta = item.get("metadata", {})
        item_ticker = meta.get("ticker") or ticker or ""
        raw_url = item.get("url", "")
        # Normalise stale URLs: strip trailing /announcements and ensure uppercase ticker
        if raw_url.endswith("/announcements"):
            raw_url = raw_url[: -len("/announcements")]
        canonical_url = (
            raw_url
            if raw_url
            else f"https://www.asx.com.au/markets/company/{item_ticker}"
        )
        announcements.append({
            "title": item.get("title", ""),
            "date": meta.get("document_date", ""),
            "released_at": item.get("timestamp", ""),
            "market_sensitive": meta.get("market_sensitive", False),
            "url": canonical_url,
            "pages": meta.get("number_of_pages"),
            "size": meta.get("size", ""),
        })

    # Sort most recent first
    announcements.sort(key=lambda x: x.get("released_at", ""), reverse=True)

    market_sensitive_count = sum(1 for a in announcements if a.get("market_sensitive"))

    return {
        "ticker": ticker,
        "business_name": business_name,
        "location": location,
        "category": category,
        "total": len(announcements),
        "market_sensitive_count": market_sensitive_count,
        "announcements": announcements,
    }


# ── Trending endpoint ──────────────────────────────────────────────────────


@app.get("/trending")
async def get_trending(limit: int = 10):
    """Return the most searched businesses across all users (no auth required)."""
    limit = max(1, min(limit, 20))

    # Step 1: Scan all users and count how many times each business_key appears
    # across every user's past list (duplicates count — repeat searches = more trending)
    counter: dict[str, int] = {}
    scan_kwargs: dict = {"ProjectionExpression": "past"}
    while True:
        response = _users.scan(**scan_kwargs)
        for item in response.get("Items", []):
            for key in item.get("past", []):
                counter[key] = counter.get(key, 0) + 1
        last_evaluated = response.get("LastEvaluatedKey")
        if not last_evaluated:
            break
        scan_kwargs["ExclusiveStartKey"] = last_evaluated

    if not counter:
        return {"trending": [], "total_searches": 0}

    # Step 2: Pick the top N business keys by total search count
    top_keys = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:limit]
    top_key_set = {k for k, _ in top_keys}

    # Step 3: Resolve business keys → human-readable info via data retrieval /companies
    name_map: dict[str, dict] = {}
    if DATA_RETRIEVAL_URL:
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
                resp = await client.get(f"{DATA_RETRIEVAL_URL.rstrip('/')}/companies")
                if resp.status_code == 200:
                    for company in resp.json().get("companies", []):
                        key = _compute_business_key(
                            company["business_name"],
                            company["location"],
                            company["category"],
                        )
                        if key in top_key_set:
                            name_map[key] = {
                                "business_name": company["business_name"],
                                "location": company["location"],
                                "category": company["category"],
                            }
        except Exception:
            pass  # Degrade gracefully — keys without names are still useful

    # Step 4: Build response
    trending = []
    for key, count in top_keys:
        entry: dict = {"business_key": key, "search_count": count}
        if key in name_map:
            entry.update(name_map[key])
        trending.append(entry)

    return {
        "trending": trending,
        "total_searches": sum(counter.values()),
    }


# ── Proxy helpers ──────────────────────────────────────────────────────────


def _compute_business_key(business_name: str, location: str, category: str) -> str:
    """Derive the business hash key the same way the other services do."""
    raw = f"{business_name}{location}{category}".lower().replace(" ", "")
    return hashlib.sha256(raw.encode()).hexdigest()


def _record_past(email: str, business_key: str):
    """Append business_key to the user's past list (fire-and-forget)."""
    try:
        _users.update_item(
            Key={"email": email},
            UpdateExpression="SET past = list_append(if_not_exists(past, :empty), :key)",
            ExpressionAttributeValues={":key": [business_key], ":empty": []},
        )
    except Exception:
        pass  # Never fail the main request over a history write


async def _fetch_sentiment_for_business(business: dict, user: dict) -> dict:
    """Call the analytical model for one business and return merged result.

    Always returns a dict that includes the original business fields so the
    caller can identify which business failed if the upstream is unavailable.
    """
    if not ANALYTICAL_MODEL_URL:
        return {**business, "error": "Analytical model not configured"}
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(
                f"{ANALYTICAL_MODEL_URL.rstrip('/')}/sentiment",
                params={
                    "business_name": business["business_name"],
                    "location": business["location"],
                    "category": business["category"],
                },
                headers={
                    "X-User-Email": user["sub"],
                    "X-User-Username": user["username"],
                },
            )
        if resp.status_code == 200:
            # Record in past list (same funnel as the proxy route)
            business_key = _compute_business_key(
                business["business_name"],
                business["location"],
                business["category"],
            )
            _record_past(user["sub"], business_key)
            return {**business, **resp.json()}
        return {**business, "error": f"Upstream returned {resp.status_code}"}
    except Exception:
        return {**business, "error": "Failed to reach analytical model"}


async def _proxy(base_url: str, path: str, request: Request, user: dict) -> Response:
    """Forward a request to a downstream service, injecting user context headers."""
    if not base_url:
        raise HTTPException(status_code=503, detail="Upstream service URL not configured")

    target_url = f"{base_url.rstrip('/')}/{path}"

    user_headers = {
        "X-User-Email": user["sub"],
        "X-User-Username": user["username"],
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        upstream = await client.request(
            method=request.method,
            url=target_url,
            params=dict(request.query_params),
            headers=user_headers,
            content=await request.body(),
        )

    forwarded_headers = {
        k: v for k, v in upstream.headers.items() if k.lower() not in _HOP_BY_HOP
    }

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=forwarded_headers,
        media_type=upstream.headers.get("content-type"),
    )


# ── Analyse endpoint (auto-collect + sentiment in one call) ───────────────


@app.get("/analyse")
async def analyse(
    business_name: str,
    location: str,
    category: str,
    user: Annotated[dict, Depends(get_current_user)],
):
    """
    Full analysis pipeline for a business.

    1. Calls the analytical model's /sentiment endpoint.
    2. If no data exists yet (404-style response), automatically triggers
       POST /collect on the data-collection service, then retries sentiment.
    3. Records the business in the user's past list on success.
    """
    if not ANALYTICAL_MODEL_URL:
        raise HTTPException(status_code=503, detail="Analytical model not configured")

    user_headers = {
        "X-User-Email": user["sub"],
        "X-User-Username": user["username"],
    }
    params = {
        "business_name": business_name,
        "location": location,
        "category": category,
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        sentiment_resp = await client.get(
            f"{ANALYTICAL_MODEL_URL.rstrip('/')}/sentiment",
            params=params,
            headers=user_headers,
        )

    # If the analytical model has no data, collect it now then retry once
    body = sentiment_resp.json() if sentiment_resp.content else {}
    no_data = sentiment_resp.status_code in (404, 400) or (
        sentiment_resp.status_code == 200
        and "no collected data" in str(body).lower()
    )

    if no_data:
        if not DATA_COLLECTION_URL:
            raise HTTPException(status_code=503, detail="Data collection service not configured")

        # Trigger collection (this takes ~10-20 s for a cold business)
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                collect_resp = await client.post(
                    f"{DATA_COLLECTION_URL.rstrip('/')}/collect",
                    json=params,
                )
            if collect_resp.status_code not in (200, 201):
                raise HTTPException(
                    status_code=502,
                    detail=f"Data collection failed: {collect_resp.text[:200]}",
                )
        except httpx.TimeoutException:
            raise HTTPException(
                status_code=504,
                detail="Data collection timed out — try again in a moment",
            )

        # Retry sentiment after collecting
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            sentiment_resp = await client.get(
                f"{ANALYTICAL_MODEL_URL.rstrip('/')}/sentiment",
                params=params,
                headers=user_headers,
            )

    if sentiment_resp.status_code != 200:
        raise HTTPException(
            status_code=sentiment_resp.status_code,
            detail=sentiment_resp.text[:300],
        )

    # Record in user's past list
    business_key = _compute_business_key(business_name, location, category)
    _record_past(user["sub"], business_key)

    return sentiment_resp.json()


# ── Authenticated proxy routes ─────────────────────────────────────────────


@app.api_route(
    "/data-collection/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def data_collection_proxy(
    path: str,
    request: Request,
    user: Annotated[dict, Depends(get_current_user)],
):
    return await _proxy(DATA_COLLECTION_URL, path, request, user)


@app.api_route(
    "/data-retrieval/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def data_retrieval_proxy(
    path: str,
    request: Request,
    user: Annotated[dict, Depends(get_current_user)],
):
    return await _proxy(DATA_RETRIEVAL_URL, path, request, user)


@app.api_route(
    "/analytical-model/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def analytical_model_proxy(
    path: str,
    request: Request,
    user: Annotated[dict, Depends(get_current_user)],
):
    response = await _proxy(ANALYTICAL_MODEL_URL, path, request, user)

    # For sentiment: if no data exists yet, auto-collect then retry once.
    # This handles new businesses transparently without any frontend changes.
    if path.rstrip("/") == "sentiment" and DATA_COLLECTION_URL:
        try:
            body = json.loads(response.body)
        except Exception:
            body = {}

        no_data = response.status_code in (404, 400) or (
            response.status_code == 200
            and (body.get("items_analysed", 1) == 0 or body.get("overall_score") is None)
        )

        if no_data:
            params = dict(request.query_params)
            biz = params.get("business_name", "")
            loc = params.get("location", "")
            cat = params.get("category", "")
            if biz and loc and cat:
                try:
                    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                        collect_resp = await client.post(
                            f"{DATA_COLLECTION_URL.rstrip('/')}/collect",
                            json={"business_name": biz, "location": loc, "category": cat},
                        )
                    if collect_resp.status_code in (200, 201):
                        # Brief pause to allow DynamoDB to propagate the write
                        await asyncio.sleep(2)
                        response = await _proxy(ANALYTICAL_MODEL_URL, path, request, user)
                except Exception:
                    pass  # Return original response if auto-collect fails

    # Funnel: record business in user's past list when sentiment is analysed
    if path.rstrip("/") == "sentiment" and response.status_code == 200:
        params = dict(request.query_params)
        biz = params.get("business_name", "")
        loc = params.get("location", "")
        cat = params.get("category", "")
        if biz and loc and cat:
            business_key = _compute_business_key(biz, loc, cat)
            _record_past(user["sub"], business_key)

    return response


# Mangum wraps your FastAPI app so it works perfectly inside AWS Lambda
handler = Mangum(app)
