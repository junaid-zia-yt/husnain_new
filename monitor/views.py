"""Read-only combined Firebase telemetry endpoints; no SQL usage."""

from __future__ import annotations

import json
import time

from django.conf import settings
from django.http import Http404, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.utils import timezone

from .auth import dashboard_login_required
from .services.firebase import FirebaseClient, FirebaseError, VitalReading
from .services.vitals import signal_status, status_for_bpm


@dashboard_login_required
def dashboard(request):
    return render(
        request,
        "monitor/dashboard.html",
        {
            "dashboard_username": settings.DASHBOARD_USERNAME,
            "firebase_path": settings.FIREBASE_PATIENT_PATH,
            "firebase_ecg_path": settings.FIREBASE_ECG_PATH,
            "firebase_database_url": settings.FIREBASE_DATABASE_URL,
            "poll_interval_ms": settings.POLL_INTERVAL_MS,
            "sse_enabled": settings.ENABLE_SSE,
        },
    )


def _payload_for_reading(reading: VitalReading):
    return {
        "ok": True,
        "reading": {
            "bpm": reading.bpm,
            "temperature": reading.temperature,
            "patient_timestamp": reading.patient_timestamp,
            "ecg": reading.ecg,
            "ecg_sequence": reading.ecg_sequence,
            "ecg_timestamp": reading.ecg_timestamp,
            # Keep the old key for existing API clients; this belongs to /patient.
            "sensor_timestamp": reading.patient_timestamp,
            "received_at": reading.received_at.isoformat(),
            "server_time": timezone.now().isoformat(),
            "source": reading.source,
        },
        "status": {
            "bpm": status_for_bpm(reading.bpm),
            "temperature": signal_status(reading.temperature),
            "patient_timestamp": signal_status(reading.patient_timestamp),
            "ecg": signal_status(reading.ecg),
            "ecg_sequence": signal_status(reading.ecg_sequence),
            "ecg_timestamp": signal_status(reading.ecg_timestamp),
            "sensor_timestamp": signal_status(reading.patient_timestamp),
        },
        "firebase": {
            "patient_path": f"/{settings.FIREBASE_PATIENT_PATH}",
            "ecg_path": f"/{settings.FIREBASE_ECG_PATH}",
            "paths": [f"/{settings.FIREBASE_PATIENT_PATH}", f"/{settings.FIREBASE_ECG_PATH}"],
            "database_url": settings.FIREBASE_DATABASE_URL,
        },
    }


@dashboard_login_required
def live_vitals(request):
    try:
        reading = FirebaseClient().fetch()
    except FirebaseError as exc:
        response = JsonResponse(
            {"ok": False, "error": "Unable to read Firebase Realtime Database", "detail": str(exc)},
            status=503,
        )
        response["Cache-Control"] = "no-store"
        return response
    response = JsonResponse(_payload_for_reading(reading))
    response["Cache-Control"] = "no-store"
    return response


@dashboard_login_required
def stream_vitals(request):
    """Optional merged SSE polling proxy for conventional (non-Vercel) servers.

    On Vercel, leave ENABLE_SSE=False. Browser-side /api/live/ polling is the
    supported one-second update path and avoids long-running serverless calls.
    """
    if not settings.ENABLE_SSE:
        raise Http404("SSE disabled: 1-second REST polling is the default on Vercel")

    def event_stream():
        yield ": connected\n\n"
        last_packet = None
        try:
            while True:
                reading = FirebaseClient().fetch()
                packet = (
                    reading.bpm, reading.temperature, reading.patient_timestamp,
                    reading.ecg, reading.ecg_sequence, reading.ecg_timestamp,
                )
                if packet != last_packet:
                    yield "event: telemetry\n"
                    yield f"data: {json.dumps(_payload_for_reading(reading), separators=(',', ':'))}\n\n"
                    last_packet = packet
                else:
                    yield ": keepalive\n\n"
                time.sleep(settings.POLL_INTERVAL_MS / 1000)
        except GeneratorExit:
            return
        except FirebaseError as exc:
            payload = json.dumps({"ok": False, "error": "Firebase stream disconnected", "detail": str(exc)})
            yield "event: telemetry_error\n"
            yield f"data: {payload}\n\n"

    response = StreamingHttpResponse(event_stream(), content_type="text/event-stream")
    response["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response["X-Accel-Buffering"] = "no"
    return response
