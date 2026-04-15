import hashlib
import os
import time
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


# ── Public health endpoint ─────────────────────────────────────────────────


@app.get("/api")
def my_api():
    return {"message": "Hello from Ghostie Middleware!"}


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
