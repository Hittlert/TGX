"""Tests for the project-local tdl daemon client."""

import unittest

from module.tdl_client import TDLClient, TDLUnavailable


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class TargetProgressClientTestCase(unittest.TestCase):
    def test_reads_target_progress_from_control_api(self):
        calls = []

        def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            return FakeResponse(
                {
                    "progress": [
                        {
                            "chat_id": "-1001",
                            "total_files": 4,
                            "downloaded_files": 1,
                        },
                        "invalid",
                    ]
                }
            )

        client = TDLClient("http://tdl-daemon:18080", request=request)

        self.assertEqual(
            [
                {
                    "chat_id": "-1001",
                    "total_files": 4,
                    "downloaded_files": 1,
                }
            ],
            client.get_target_progress(),
        )
        self.assertEqual("GET", calls[0][0])
        self.assertEqual(
            "http://tdl-daemon:18080/api/v1/target-progress", calls[0][1]
        )
        self.assertEqual(client.request_timeout, calls[0][2]["timeout"])

    def test_refresh_dialogs_calls_post_endpoint(self):
        calls = []

        def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            return FakeResponse({"dialogs": []})

        client = TDLClient("http://tdl-daemon:18080", request=request)
        result = client.refresh_dialogs()
        self.assertEqual({"dialogs": []}, result)
        self.assertEqual("POST", calls[0][0])
        self.assertEqual("http://tdl-daemon:18080/api/v1/dialogs/refresh", calls[0][1])


if __name__ == "__main__":
    unittest.main()
