#!/usr/bin/env python3
"""
 Vehicle Lookup with Full Unmasked Chassis, Engine, Owner & Insurance

 Run Service (FastAPI / Uvicorn):
     uvicorn policyboss:app --host 0.0.0.0 --port 8000 --reload
     or:
     python policyboss.py --serve --port 8000

 Routes:
     GET  /veh?rc=hereTheVehicleNumber
     GET  /health

 CLI Usage:
     python policyboss.py KL01CS9191
     python policyboss.py MH12DE1433
"""

import argparse
import base64
import concurrent.futures
import datetime
import json
import logging
import os
import random
import re
import sqlite3
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from fastapi import FastAPI, HTTPException, Query, Request, Response
    from fastapi.middleware.cors import CORSMiddleware
    hasFastApi = True
except ImportError:
    hasFastApi = False

logging.basicConfig(level=logging.ERROR, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stderr)
logger = logging.getLogger("vehicle_api")

endpointDirectB64 = "aHR0cHM6Ly9hcGkucG9saWN5Ym9zcy5jb206NTQ0My9xdW90ZS92ZWhpY2xlX2luZm9fdXNlcg=="
endpointRedirectB64 = "aHR0cHM6Ly9ob3Jpem9uLnBvbGljeWJvc3MuY29tOjU0NDMvcXVvdGUvdmVoaWNsZV9pbmZvX3VzZXI="
originB64 = "aHR0cHM6Ly93d3cucG9saWN5Ym9zcy5jb20="
refererB64 = "aHR0cHM6Ly93d3cucG9saWN5Ym9zcy5jb20v"

secretKey = "SECRET-HZ07QRWY-JIBT-XRMQ-ZP95-J0RWP3DYRACW"
clientKey = "CLIENT-CNTP6NYE-CU9N-DUZW-CSPI-SH1IS4DOVHB9"
defaultProductId = 1
validSsIds = [150255, 150000, 16000, 1, 150254]
validSources = ["CARINFO", "SHEERDRIVE"]
requestTimeout = 10

cacheTtlSeconds = 86400
dbPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), "policyboss_cache.db")


def b64Decode(val: str) -> str:
    return base64.b64decode(val.encode("utf-8")).decode("utf-8")


def createMeta(startTime: float) -> Dict[str, str]:
    elapsed = round(time.perf_counter() - startTime, 4)
    return {
        "lookupSecs": f"{elapsed:.4f}/s",
        "dev": "4nsil!",
        "tg": "@drazexx",
    }


def stripInternalFields(data: Any):
    if not isinstance(data, dict):
        return
    for k in [
        "FastlaneResponse",
        "Ip_Address",
        "Calling_Source",
        "Source",
        "source",
        "FastLane_Execution_Time",
        "Fastlane_Execution_Time",
        "fastlane_execution_time",
        "Match_Mode",
        "match_mode",
        "meta",
    ]:
        data.pop(k, None)


browserProfiles = [
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "platform": '"Windows"',
        "mobile": "?0",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        "platform": '"Windows"',
        "mobile": "?0",
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "platform": '"Windows"',
        "mobile": "?0",
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "platform": '"macOS"',
        "mobile": "?0",
    },
    {
        "ua": "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.122 Mobile Safari/537.36",
        "sec_ch_ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        "platform": '"Android"',
        "mobile": "?1",
    },
]


def getRandomHeaders() -> Dict[str, str]:
    profile = random.choice(browserProfiles)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": profile["ua"],
        "Origin": b64Decode(originB64),
        "Referer": b64Decode(refererB64),
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    if profile.get("sec_ch_ua"):
        headers["Sec-Ch-Ua"] = profile["sec_ch_ua"]
        headers["Sec-Ch-Ua-Mobile"] = profile["mobile"]
        headers["Sec-Ch-Ua-Platform"] = profile["platform"]
    return headers


class MultiTierCache:
    def __init__(self, targetDbPath: str = dbPath, ttlSeconds: float = cacheTtlSeconds):
        self.dbPath = targetDbPath
        self.ttl = ttlSeconds
        self.memoryCache: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.initDb()

    def initDb(self):
        try:
            with sqlite3.connect(self.dbPath, timeout=5) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS vehicle_cache (
                        reg TEXT PRIMARY KEY,
                        data TEXT NOT NULL,
                        cached_at REAL NOT NULL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_cached_at ON vehicle_cache(cached_at)")
                conn.commit()
        except Exception:
            pass

    def get(self, reg: str) -> Optional[Dict[str, Any]]:
        cleanReg = reg.upper()
        now = time.time()

        with self.lock:
            entry = self.memoryCache.get(cleanReg)
            if entry and (now - entry["cached_at"] < self.ttl):
                return dict(entry["data"])

        try:
            with sqlite3.connect(self.dbPath, timeout=3) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data, cached_at FROM vehicle_cache WHERE reg = ?", (cleanReg,))
                row = cursor.fetchone()
                if row:
                    rawJson, cachedAt = row
                    if now - cachedAt < self.ttl:
                        data = json.loads(rawJson)
                        with self.lock:
                            self.memoryCache[cleanReg] = {"data": data, "cached_at": cachedAt}
                        return data
                    else:
                        cursor.execute("DELETE FROM vehicle_cache WHERE reg = ?", (cleanReg,))
                        conn.commit()
        except Exception:
            pass

        return None

    def set(self, reg: str, data: Dict[str, Any]):
        cleanReg = reg.upper()
        now = time.time()
        serialized = json.dumps(data)

        with self.lock:
            self.memoryCache[cleanReg] = {"data": data, "cached_at": now}

        try:
            with sqlite3.connect(self.dbPath, timeout=3) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO vehicle_cache (reg, data, cached_at) VALUES (?, ?, ?)",
                    (cleanReg, serialized, now)
                )
                conn.commit()
        except Exception:
            pass

    def evict(self, reg: str) -> bool:
        cleanReg = reg.upper()
        removed = False
        with self.lock:
            if cleanReg in self.memoryCache:
                del self.memoryCache[cleanReg]
                removed = True

        try:
            with sqlite3.connect(self.dbPath, timeout=3) as conn:
                cursor = conn.execute("DELETE FROM vehicle_cache WHERE reg = ?", (cleanReg,))
                conn.commit()
                if cursor.rowcount > 0:
                    removed = True
        except Exception:
            pass

        return removed

    def size(self) -> int:
        count = 0
        try:
            with sqlite3.connect(self.dbPath, timeout=3) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM vehicle_cache")
                row = cursor.fetchone()
                if row:
                    count = row[0]
        except Exception:
            with self.lock:
                count = len(self.memoryCache)
        return count


cache = MultiTierCache()


def createOptimizedSession() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.2,
        status_forcelist=[502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=50,
        pool_maxsize=50,
        pool_block=False
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


httpSession = createOptimizedSession()


class PrettyJsonResponse(Response if hasFastApi else object):
    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            separators=(", ", ": "),
        ).encode("utf-8")


class VehicleNotFound(Exception):
    pass


class UpstreamServiceError(Exception):
    pass


def normalizeReg(regInput: str) -> str:
    stripped = str(regInput or "").strip().upper()
    delimited = [p for p in re.split(r"[-\s]+", stripped) if p]

    if len(delimited) == 4:
        s, r, ser, num = delimited
        return f"{s[:2]}{r.zfill(2)}{ser}{num.zfill(4)}"
    elif len(delimited) == 3:
        if delimited[2].isdigit():
            return f"{delimited[0][:2]}{delimited[1].zfill(2)}{delimited[2].zfill(4)}"
        else:
            m = re.match(r"^([A-Z]+)(\d+)$", delimited[2])
            if m:
                return f"{delimited[0][:2]}{delimited[1].zfill(2)}{m.group(1)}{m.group(2).zfill(4)}"

    cleaned = re.sub(r"[^A-Za-z0-9]", "", stripped)

    mSeries = re.match(r"^([A-Z]{2})(\d{1,2})([A-Z]+)(\d{1,4})$", cleaned)
    if mSeries:
        s, r, ser, num = mSeries.groups()
        return f"{s}{r.zfill(2)}{ser}{num.zfill(4)}"

    mNoSeries4 = re.match(r"^([A-Z]{2})(\d{1,2})(\d{4})$", cleaned)
    if mNoSeries4:
        s, r, num = mNoSeries4.groups()
        return f"{s}{r.zfill(2)}{num}"

    mNoSeries = re.match(r"^([A-Z]{2})(\d{1,2})(\d{1,4})$", cleaned)
    if mNoSeries:
        s, r, num = mNoSeries.groups()
        return f"{s}{r.zfill(2)}{num.zfill(4)}"

    bharatMatch = re.match(r"^(\d{2}[A-Z]{2})(\d{1,4})([A-Z]*)$", cleaned)
    if bharatMatch:
        prefixPart, numPart, serPart = bharatMatch.groups()
        return f"{prefixPart}{numPart.zfill(4)}{serPart}"

    return cleaned


def getRegCandidates(regInput: str) -> List[str]:
    padded = normalizeReg(regInput)
    if not padded:
        return []

    candidates: List[str] = [padded]
    cleaned = re.sub(r"[^A-Za-z0-9]", "", str(regInput or "")).upper()
    if cleaned and cleaned not in candidates:
        candidates.append(cleaned)

    m = re.match(r"^([A-Z]{2})(\d{2})([A-Z]*)(\d{4})$", padded)
    if m:
        state, rto, series, num = m.groups()
        unpaddedRto = f"{state}{int(rto)}{series}{num}"
        unpaddedNum = f"{state}{rto}{series}{int(num)}"
        rawAllUnpadded = f"{state}{int(rto)}{series}{int(num)}"

        for c in [unpaddedRto, unpaddedNum, rawAllUnpadded]:
            if c and c not in candidates:
                candidates.append(c)

    return candidates


def enrichAndCleanResponse(data: Dict[str, Any], regNo: str) -> Dict[str, Any]:
    stripInternalFields(data)

    if not data.get("Registration_Number"):
        data["Registration_Number"] = regNo

    vahanMake = data.get("Vahan_Make") or data.get("Make_Name") or ""
    vahanModel = data.get("Vahan_Model") or data.get("Model_Name") or ""
    vahanFuel = data.get("Vahan_Fuel") or data.get("Fuel_Type") or ""
    vahanCc = data.get("Vahan_Cubic_Capacity") or data.get("Cubic_Capacity") or 0

    for key in list(data.keys()):
        if key.startswith("pb_"):
            cleanKey = key[3:]
            if cleanKey not in data:
                data[cleanKey] = data[key]
            data.pop(key, None)

    if not data.get("fastlane_make"):
        data["fastlane_make"] = vahanMake
    if not data.get("fastlane_model_variant"):
        data["fastlane_model_variant"] = vahanModel
    if not data.get("fastlane_fuel"):
        data["fastlane_fuel"] = vahanFuel
    if not data.get("fastlane_cc"):
        try:
            data["fastlane_cc"] = int(vahanCc)
        except (ValueError, TypeError):
            data["fastlane_cc"] = vahanCc

    if not data.get("fastlane_full"):
        parts = [str(vahanMake), str(vahanModel), str(vahanFuel), str(vahanCc)]
        data["fastlane_full"] = " ".join(p for p in parts if p).strip()

    if not data.get("Communication_Pincode"):
        data["Communication_Pincode"] = data.get("Permanent_Pincode") or ""

    if not data.get("vehicle_search_mode"):
        data["vehicle_search_mode"] = "LIKE"

    if "Call_Execution_Time" not in data:
        data["Call_Execution_Time"] = 0

    if "Gross_Vehicle_Weight" not in data:
        data["Gross_Vehicle_Weight"] = data.get("Vahan_GVW", 0)

    if not data.get("status"):
        data["status"] = data.get("Result", "Success")

    return data


def querySingle(reg: str, productId: int = defaultProductId) -> Optional[Dict[str, Any]]:
    headers = getRandomHeaders()
    endpoints = [b64Decode(endpointDirectB64), b64Decode(endpointRedirectB64)]

    safeProductId = defaultProductId
    if isinstance(productId, int):
        safeProductId = productId
    else:
        try:
            safeProductId = int(str(productId))
        except (ValueError, TypeError):
            safeProductId = defaultProductId

    for endpoint in endpoints:
        for ssId in validSsIds:
            for sourceName in validSources:
                payload = {
                    "secret_key": secretKey,
                    "client_key": clientKey,
                    "RegistrationNumber": reg,
                    "product_id": safeProductId,
                    "ss_id": ssId,
                    "source": sourceName,
                }

                try:
                    response = httpSession.post(
                        endpoint,
                        json=payload,
                        headers=headers,
                        timeout=requestTimeout,
                    )
                except requests.exceptions.RequestException:
                    continue

                if response.status_code == 500:
                    return None

                if response.status_code != 200:
                    continue

                try:
                    data = response.json()
                except Exception:
                    continue

                if not isinstance(data, dict):
                    continue

                errorMsg = data.get("Error_Message", "")
                result = data.get("Result", "")
                status = data.get("status", "")

                if errorMsg in ("FASTLANE_DATA_NA_API", "NO_VAHANA_DETAILS_AVAILABLE") or data.get("0") == "N":
                    return None

                if result == "Success" or status == "Success" or data.get("Chassis_Number") or data.get("Make_Name"):
                    return enrichAndCleanResponse(data, reg)

    return None


def fetchVehicle(regInput: str, forceRefresh: bool = False, productId: Any = defaultProductId, startTime: Optional[float] = None) -> Dict[str, Any]:
    if startTime is None:
        startTime = time.perf_counter()

    cleanReg = normalizeReg(regInput)
    if not cleanReg:
        raise VehicleNotFound("Registration number is required and cannot be empty")

    safePid = defaultProductId
    if isinstance(productId, int):
        safePid = productId
    else:
        try:
            safePid = int(str(productId))
        except (ValueError, TypeError):
            safePid = defaultProductId

    if not forceRefresh:
        cachedData = cache.get(cleanReg)
        if cachedData:
            outData = dict(cachedData)
            stripInternalFields(outData)
            outData["meta"] = createMeta(startTime)
            return outData

    candidates = getRegCandidates(cleanReg)
    resolvedData: Optional[Dict[str, Any]] = None
    successfulCandidate = cleanReg

    if len(candidates) > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(candidates), 3)) as executor:
            futureToCand = {
                executor.submit(querySingle, cand, safePid): cand
                for cand in candidates
            }
            for future in concurrent.futures.as_completed(futureToCand):
                cand = futureToCand[future]
                try:
                    data = future.result()
                    if data:
                        resolvedData = data
                        successfulCandidate = cand
                        for f in futureToCand:
                            f.cancel()
                        break
                except Exception:
                    pass
    else:
        resolvedData = querySingle(candidates[0], productId=safePid)
        successfulCandidate = candidates[0]

    if not resolvedData:
        raise VehicleNotFound(f"Vehicle details not found for registration number: {cleanReg}")

    stripInternalFields(resolvedData)

    cache.set(cleanReg, resolvedData)
    if successfulCandidate != cleanReg:
        cache.set(successfulCandidate, resolvedData)

    outData = dict(resolvedData)
    outData["meta"] = createMeta(startTime)
    return outData


if hasFastApi:
    app = FastAPI(
        title="Vehicle Intelligence API",
        version="2.1.0",
        default_response_class=PrettyJsonResponse,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(HTTPException)
    async def httpExceptionHandler(request: Request, exc: HTTPException):
        t0 = getattr(request.state, "startTime", time.perf_counter())
        return PrettyJsonResponse(
            status_code=exc.status_code,
            content={
                "status": "error",
                "detail": exc.detail,
                "meta": createMeta(t0),
            },
        )

    @app.middleware("http")
    async def addStartTime(request: Request, callNext):
        request.state.startTime = time.perf_counter()
        response = await callNext(request)
        return response

    @app.get("/", summary="API Root")
    async def rootEndpoint(request: Request):
        t0 = getattr(request.state, "startTime", time.perf_counter())
        return {
            "route": "/veh?rc=hereTheVehicleNumber",
            "example": "/veh?rc=KL39N3451",
            "meta": createMeta(t0),
        }

    @app.get("/veh", summary="Vehicle RC Lookup")
    @app.get("/lookup", summary="Lookup alias")
    @app.get("/vehicle", summary="Vehicle alias")
    async def vehEndpoint(
        request: Request,
        rc: Optional[str] = Query(None, description="Vehicle Registration Number, e.g. KL39N3451"),
        veh: Optional[str] = Query(None, description="Vehicle Registration Number alias"),
        registrationNumber: Optional[str] = Query(None, description="Registration number alias"),
        info: Optional[str] = Query(None, description="Vehicle info alias"),
        reg: Optional[str] = Query(None, description="Reg alias"),
        forceRefresh: bool = Query(False, description="Bypass cache and force refresh"),
        productId: Optional[int] = Query(None, description="Product ID (default: 1)"),
    ):
        t0 = getattr(request.state, "startTime", time.perf_counter())
        targetRc = rc or veh or registrationNumber or info or reg
        if not targetRc or len(targetRc.strip()) < 4:
            raise HTTPException(
                status_code=400,
                detail="Registration number required: /veh?rc=hereTheVehicleNumber"
            )

        pid = productId if isinstance(productId, int) else defaultProductId
        try:
            return fetchVehicle(targetRc, forceRefresh=forceRefresh, productId=pid, startTime=t0)
        except VehicleNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except UpstreamServiceError as exc:
            raise HTTPException(status_code=429, detail=str(exc))
        except Exception:
            raise HTTPException(status_code=500, detail="Internal server error while retrieving vehicle details")

    @app.get("/health", summary="Health Check")
    async def healthEndpoint(request: Request):
        t0 = getattr(request.state, "startTime", time.perf_counter())
        return {
            "status": "healthy",
            "cacheSize": cache.size(),
            "meta": createMeta(t0),
        }

    @app.delete("/cache/{registrationNumber}", summary="Evict vehicle from cache")
    async def evictCacheEndpoint(registrationNumber: str, request: Request):
        t0 = getattr(request.state, "startTime", time.perf_counter())
        cleanReg = normalizeReg(registrationNumber)
        removed = cache.evict(cleanReg)
        return {
            "status": "success",
            "evicted": removed,
            "registrationNumber": cleanReg,
            "meta": createMeta(t0),
        }
else:
    app = None


def main():
    parser = argparse.ArgumentParser(
        description="High-Speed Vehicle RC Intelligence Scraper & API",
        epilog="Examples:\n  python policyboss.py KL01CS9191\n  python policyboss.py --serve --port 8000",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("registration", nargs="?", help="Vehicle Registration Number (e.g. KL01CS9191)")
    parser.add_argument("--force-refresh", "-f", action="store_true", help="Bypass local cache")
    parser.add_argument("--serve", "-s", action="store_true", help="Run the FastAPI REST server")
    parser.add_argument("--host", default="0.0.0.0", help="Server host (default: 0.0.0.0)")
    parser.add_argument("--port", "-p", type=int, default=8000, help="Server port (default: 8000)")

    args = parser.parse_args()

    if args.serve:
        if not hasFastApi:
            print("Error: fastapi and uvicorn are required to run the server. Run: pip install fastapi uvicorn")
            sys.exit(1)
        import uvicorn
        print(f"Starting Vehicle API server on http://{args.host}:{args.port}")
        uvicorn.run(app, host=args.host, port=args.port)
        return

    if not args.registration:
        parser.print_help()
        sys.exit(1)

    t0 = time.perf_counter()
    try:
        data = fetchVehicle(args.registration, forceRefresh=args.force_refresh, startTime=t0)
        print(json.dumps(data, indent=2, ensure_ascii=False))
        sys.exit(0)
    except VehicleNotFound as exc:
        errRes = {
            "status": "error",
            "detail": str(exc),
            "meta": createMeta(t0),
        }
        print(json.dumps(errRes, indent=2, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        errRes = {
            "status": "error",
            "detail": str(exc),
            "meta": createMeta(t0),
        }
        print(json.dumps(errRes, indent=2, ensure_ascii=False), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()