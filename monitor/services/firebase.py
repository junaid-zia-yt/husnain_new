"""Read the two independently updated Firebase Realtime Database telemetry nodes."""

from __future__ import annotations

import json
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from django.conf import settings

try:
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2 import service_account
except ImportError:  # pragma: no cover
    GoogleAuthRequest = None
    service_account = None


class FirebaseError(RuntimeError):
    pass


# Warm serverless instances can reuse a valid OAuth token. Without caching,
# one-second polling could trigger an unnecessary Google token refresh per poll.
_CREDENTIALS_LOCK = threading.Lock()
_CACHED_CREDENTIALS = None
_CACHED_IDENTITY = None


@dataclass
class VitalReading:
    bpm: float | None
    ecg: float | None
    temperature: float | None
    patient_timestamp: float | None
    ecg_timestamp: float | None
    ecg_sequence: float | None
    received_at: datetime
    source: str
    raw: dict[str, Any]

    @property
    def sensor_timestamp(self) -> float | None:
        """Legacy API alias for the patient-node timestamp."""
        return self.patient_timestamp


class FirebaseClient:
    """Read /patient and /ecg_live; never assume ECG is nested under /patient.

    Each HTTP snapshot is independent, so these are the latest observed values,
    NOT a transactionally synchronized snapshot across both Firebase nodes.
    """

    def __init__(self):
        self.database_url = settings.FIREBASE_DATABASE_URL.rstrip("/")
        self.timeout = settings.FIREBASE_TIMEOUT_SECONDS

    def _access_token(self) -> str | None:
        file_path = settings.FIREBASE_SERVICE_ACCOUNT_FILE
        account_json = settings.FIREBASE_SERVICE_ACCOUNT_JSON
        if not file_path and not account_json:
            return None
        if not service_account or not GoogleAuthRequest:
            raise FirebaseError("google-auth is required for Firebase authentication")

        global _CACHED_CREDENTIALS, _CACHED_IDENTITY
        # Key by hash so raw service-account JSON is not duplicated in the cache.
        identity = (file_path, hashlib.sha256(account_json.encode("utf-8")).hexdigest())
        with _CREDENTIALS_LOCK:
            if _CACHED_CREDENTIALS is None or _CACHED_IDENTITY != identity:
                scopes = [
                    "https://www.googleapis.com/auth/firebase.database",
                    "https://www.googleapis.com/auth/userinfo.email",
                ]
                if account_json:
                    try:
                        credentials = service_account.Credentials.from_service_account_info(
                            json.loads(account_json), scopes=scopes
                        )
                    except (ValueError, TypeError, KeyError) as exc:
                        raise FirebaseError("Invalid FIREBASE_SERVICE_ACCOUNT_JSON") from exc
                else:
                    path = Path(file_path)
                    if not path.exists():
                        raise FirebaseError("Firebase service-account file not found")
                    credentials = service_account.Credentials.from_service_account_file(
                        str(path), scopes=scopes
                    )
                _CACHED_CREDENTIALS = credentials
                _CACHED_IDENTITY = identity
            try:
                if not _CACHED_CREDENTIALS.valid:
                    _CACHED_CREDENTIALS.refresh(GoogleAuthRequest())
            except Exception as exc:
                raise FirebaseError("Unable to authenticate Firebase service account") from exc
            return _CACHED_CREDENTIALS.token

    def _fetch_node(self, path: str, token: str | None) -> dict[str, Any]:
        clean_path = path.strip("/")
        url = f"{self.database_url}/{clean_path}.json"
        headers = {"Accept": "application/json", "Cache-Control": "no-cache"}
        params: dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif settings.FIREBASE_AUTH_TOKEN:
            params["auth"] = settings.FIREBASE_AUTH_TOKEN
        try:
            response = requests.get(url, params=params, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.Timeout as exc:
            raise FirebaseError(f"Firebase '/{clean_path}' request timed out") from exc
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else "unknown"
            raise FirebaseError(f"Firebase '/{clean_path}' returned HTTP {code}; check its read rules and credentials") from exc
        except (requests.RequestException, ValueError) as exc:
            # Never include the outgoing URL: auth tokens could be in its query string.
            raise FirebaseError(f"Firebase '/{clean_path}' request failed") from exc

        if payload is None:
            raise FirebaseError(f"No Firebase data found at '/{clean_path}'")
        if not isinstance(payload, dict):
            raise FirebaseError(f"Firebase '/{clean_path}' must contain a JSON object")
        return payload

    def fetch(self) -> VitalReading:
        # Obtain a single auth token per combined request. Fetch both child
        # nodes concurrently to minimize user-visible Vercel response latency.
        token = self._access_token()
        patient_path = settings.FIREBASE_PATIENT_PATH
        ecg_path = settings.FIREBASE_ECG_PATH
        with ThreadPoolExecutor(max_workers=2) as executor:
            patient_future = executor.submit(self._fetch_node, patient_path, token)
            ecg_future = executor.submit(self._fetch_node, ecg_path, token)
            patient = patient_future.result()
            ecg_live = ecg_future.result()

        return reading_from_payload(
            {"patient": patient, "ecg_live": ecg_live}, source="firebase-snapshot"
        )


def reading_from_payload(payload: dict[str, Any], *, source: str) -> VitalReading:
    """Parse a combined two-node snapshot, without synthesizing missing data."""
    patient = payload.get("patient")
    ecg_live = payload.get("ecg_live")
    if not isinstance(patient, dict) or not isinstance(ecg_live, dict):
        raise FirebaseError("Expected both 'patient' and 'ecg_live' JSON objects")
    return VitalReading(
        bpm=_to_float(patient.get("BPM")),
        temperature=_to_float(patient.get("Temperature")),
        patient_timestamp=_to_float(patient.get("Timestamp")),
        ecg=_to_float(ecg_live.get("ECG")),
        ecg_sequence=_to_float(ecg_live.get("Sequence")),
        ecg_timestamp=_to_float(ecg_live.get("Timestamp")),
        received_at=datetime.now(timezone.utc),
        source=source,
        raw={"patient": patient.copy(), "ecg_live": ecg_live.copy()},
    )


def apply_firebase_event(
    state: dict[str, Any], event_name: str, path: str, data: Any
) -> dict[str, Any]:
    """Utility for applying Firebase SSE events to a node (optional integrations)."""
    current = json.loads(json.dumps(state)) if state else {}
    segments = [part for part in str(path or "/").strip("/").split("/") if part]

    if event_name == "put":
        if not segments:
            return data.copy() if isinstance(data, dict) else {}
        _set_path(current, segments, data)
        return current
    if event_name == "patch" and isinstance(data, dict):
        if not segments:
            for key, value in data.items():
                _set_path(current, [str(key)], value)
        else:
            target = _get_or_create_dict(current, segments)
            for key, value in data.items():
                _set_path(target, [str(key)], value)
    return current


def _get_or_create_dict(root: dict[str, Any], segments: list[str]) -> dict[str, Any]:
    cursor = root
    for segment in segments:
        child = cursor.get(segment)
        if not isinstance(child, dict):
            child = {}
            cursor[segment] = child
        cursor = child
    return cursor


def _set_path(root: dict[str, Any], segments: list[str], value: Any) -> None:
    if not segments:
        return
    parent = _get_or_create_dict(root, segments[:-1]) if len(segments) > 1 else root
    key = segments[-1]
    if value is None:
        parent.pop(key, None)
    else:
        parent[key] = value


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    # Non-finite numbers are not valid JSON and must not be forwarded as such.
    import math
    return parsed if math.isfinite(parsed) else None
