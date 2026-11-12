"""Regression tests for two-node Firebase data, read API and stateless login."""

from unittest.mock import patch

from django.conf import settings
from django.test import SimpleTestCase, override_settings
from django.urls import reverse

from monitor.services.firebase import (
    FirebaseClient,
    FirebaseError,
    apply_firebase_event,
    reading_from_payload,
)


FIXTURE = {
    "ecg_live": {"ECG": 4095, "Sequence": 169, "Timestamp": 332373},
    "patient": {"BPM": 89.06799, "Temperature": 27, "Timestamp": 46295},
}


class FirebasePayloadTests(SimpleTestCase):
    def test_two_node_exact_contract(self):
        reading = reading_from_payload(FIXTURE, source="test")
        self.assertAlmostEqual(reading.bpm, 89.06799)
        self.assertEqual(reading.ecg, 4095)
        self.assertEqual(reading.temperature, 27)
        self.assertEqual(reading.patient_timestamp, 46295)
        self.assertEqual(reading.sensor_timestamp, 46295)
        self.assertEqual(reading.ecg_timestamp, 332373)
        self.assertEqual(reading.ecg_sequence, 169)

    def test_ecg_does_not_read_patient_ecg_or_patient_timestamp(self):
        payload = {"patient": {**FIXTURE["patient"], "ECG": 999}, "ecg_live": FIXTURE["ecg_live"]}
        reading = reading_from_payload(payload, source="test")
        self.assertEqual(reading.ecg, 4095)
        self.assertNotEqual(reading.ecg_timestamp, reading.patient_timestamp)

    def test_missing_fields_are_null_not_fake_zero(self):
        reading = reading_from_payload({"patient": {}, "ecg_live": {}}, source="test")
        self.assertIsNone(reading.bpm)
        self.assertIsNone(reading.ecg)
        self.assertIsNone(reading.ecg_sequence)
        self.assertIsNone(reading.ecg_timestamp)
        self.assertIsNone(reading.patient_timestamp)

    def test_missing_node_is_error(self):
        with self.assertRaises(FirebaseError):
            reading_from_payload({"patient": FIXTURE["patient"]}, source="test")

    def test_non_finite_or_boolean_numbers_are_rejected(self):
        payload = {"patient": {"BPM": "NaN", "Temperature": True}, "ecg_live": {"ECG": "inf"}}
        reading = reading_from_payload(payload, source="test")
        self.assertIsNone(reading.bpm)
        self.assertIsNone(reading.temperature)
        self.assertIsNone(reading.ecg)

    def test_leaf_patch_updates_state(self):
        state = {"ECG": 4095, "Sequence": 169, "Timestamp": 332373}
        updated = apply_firebase_event(state, "put", "/Sequence", 170)
        self.assertEqual(updated["Sequence"], 170)
        self.assertEqual(updated["ECG"], 4095)
        self.assertEqual(state["Sequence"], 169)

    @override_settings(
        FIREBASE_DATABASE_URL="https://example-default-rtdb.firebaseio.com",
        FIREBASE_PATIENT_PATH="patient",
        FIREBASE_ECG_PATH="ecg_live",
        FIREBASE_SERVICE_ACCOUNT_FILE="",
        FIREBASE_SERVICE_ACCOUNT_JSON="",
        FIREBASE_AUTH_TOKEN="",
        FIREBASE_TIMEOUT_SECONDS=2,
    )
    def test_fetches_both_firebase_nodes_not_database_root(self):
        from unittest.mock import Mock

        def fake_get(url, **kwargs):
            data = FIXTURE["patient"] if url.endswith("/patient.json") else FIXTURE["ecg_live"]
            result = Mock()
            result.json.return_value = data
            result.raise_for_status.return_value = None
            return result

        with patch("monitor.services.firebase.requests.get", side_effect=fake_get) as mock_get:
            result = FirebaseClient().fetch()
        self.assertAlmostEqual(result.bpm, 89.06799)
        self.assertEqual(result.ecg_sequence, 169)
        urls = {call.args[0] for call in mock_get.call_args_list}
        self.assertEqual(
            urls,
            {
                "https://example-default-rtdb.firebaseio.com/patient.json",
                "https://example-default-rtdb.firebaseio.com/ecg_live.json",
            },
        )
        self.assertEqual(mock_get.call_count, 2)


@override_settings(
    DASHBOARD_USERNAME="doctor",
    DASHBOARD_PASSWORD="test-password-ONLY-123!",
    SECRET_KEY="strong-demo-secret-for-testing-123456789",
    ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
    SESSION_ENGINE="django.contrib.sessions.backends.signed_cookies",
    ENABLE_SSE=False,
    POLL_INTERVAL_MS=1000,
    FIREBASE_PATIENT_PATH="patient",
    FIREBASE_ECG_PATH="ecg_live",
)
class NoDatabaseTests(SimpleTestCase):
    def _login(self):
        return self.client.post(
            reverse("login"), {"username": "doctor", "password": "test-password-ONLY-123!"}
        )

    def test_no_sql_database_configured(self):
        self.assertIn(settings.DATABASES.get("default", {}).get("ENGINE"),
                      (None, "django.db.backends.dummy"))
        self.assertNotIn("django.contrib.auth", settings.INSTALLED_APPS)
        self.assertEqual(settings.SESSION_ENGINE, "django.contrib.sessions.backends.signed_cookies")

    def test_login_required(self):
        self.assertEqual(self.client.get(reverse("dashboard")).status_code, 302)
        result = self.client.get(reverse("live-vitals"))
        self.assertEqual(result.status_code, 401)
        self.assertEqual(result.json()["error"], "Login required")

    def test_wrong_password_rejected(self):
        result = self.client.post(reverse("login"), {"username": "doctor", "password": "incorrect"})
        self.assertEqual(result.status_code, 200)
        self.assertContains(result, "Invalid username or password")
        self.assertEqual(self.client.get(reverse("dashboard")).status_code, 302)

    def test_stateless_login_dashboard_and_logout(self):
        self.assertRedirects(self._login(), reverse("dashboard"), fetch_redirect_response=False)
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "ECG Sequence")
        self.assertContains(response, "/ecg_live")
        self.assertContains(response, 'data-poll-interval="1000"')
        self.assertEqual(self.client.get(reverse("stream-vitals")).status_code, 404)
        self.assertRedirects(
            self.client.post(reverse("logout")), reverse("login"), fetch_redirect_response=False
        )
        self.assertEqual(self.client.get(reverse("dashboard")).status_code, 302)

    def test_credentials_rotation_revokes_session(self):
        self._login()
        with override_settings(DASHBOARD_PASSWORD="changed-secret-123456"):
            self.assertEqual(self.client.get(reverse("dashboard")).status_code, 302)

    def test_open_redirect_is_blocked(self):
        result = self.client.post(
            reverse("login") + "?next=https://evil.example/",
            {"username": "doctor", "password": "test-password-ONLY-123!", "next": "//evil.example/"},
        )
        self.assertRedirects(result, reverse("dashboard"), fetch_redirect_response=False)

    def test_live_api_has_all_six_fields_and_no_sql_history(self):
        self._login()
        reading = reading_from_payload(FIXTURE, source="test")
        with patch("monitor.views.FirebaseClient") as client:
            client.return_value.fetch.return_value = reading
            response = self.client.get(reverse("live-vitals"))
            client.return_value.fetch.assert_called_once_with()
        self.assertEqual(response.status_code, 200)
        packet = response.json()
        self.assertAlmostEqual(packet["reading"]["bpm"], 89.06799)
        self.assertEqual(packet["reading"]["ecg_sequence"], 169)
        self.assertEqual(packet["reading"]["ecg_timestamp"], 332373)
        self.assertEqual(packet["reading"]["patient_timestamp"], 46295)
        self.assertEqual(packet["reading"]["sensor_timestamp"], 46295)
        self.assertEqual(packet["firebase"]["paths"], ["/patient", "/ecg_live"])
        self.assertNotIn("history", packet)
        self.assertEqual(response["Cache-Control"], "no-store")

    def test_live_api_handles_missing_firebase_node(self):
        self._login()
        with patch("monitor.views.FirebaseClient") as client:
            client.return_value.fetch.side_effect = FirebaseError("No Firebase data found at '/ecg_live'")
            response = self.client.get(reverse("live-vitals"))
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response["Cache-Control"], "no-store")
