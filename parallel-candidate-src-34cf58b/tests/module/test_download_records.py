"""test download record store"""

import os
import sqlite3
import tempfile
import unittest

from module.download_records import DownloadRecordStore


class DownloadRecordStoreTestCase(unittest.TestCase):
    def test_retry_ids_include_pending_and_failed_only(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            store.mark_pending(-100123, 11)
            store.mark_failed(-100123, 12, "proxy error")
            store.mark_success(
                -100123,
                13,
                file_name="13 - ok.jpg",
                save_path="/app/downloads/chat/13 - ok.jpg",
                media_type="photo",
                file_size=100,
            )
            store.mark_skipped(-100123, 14)

            self.assertEqual([11, 12], store.get_retry_ids(-100123))
            self.assertTrue(store.has_success(-100123, 13))
            self.assertEqual(
                {"failed": 1, "pending": 1, "skipped": 1, "success": 1},
                store.count_by_status(-100123),
            )

    def test_success_replaces_pending_retry(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            store.mark_pending("chat", 21)
            self.assertEqual([21], store.get_retry_ids("chat"))

            store.mark_success("chat", 21, file_name="21 - done.mp4")

            self.assertEqual([], store.get_retry_ids("chat"))
            self.assertTrue(store.has_success("chat", 21))

    def test_pending_does_not_replace_successful_download_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            store.mark_success("chat", 22, file_name="22 - moved.mp4")
            store.mark_pending("chat", 22)

            self.assertTrue(store.has_success("chat", 22))
            self.assertEqual([], store.get_retry_ids("chat"))

    def test_enqueue_and_cursor_advance_are_durable(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            next_cursor = store.enqueue_and_advance_cursor("chat", 41)

            self.assertEqual(42, next_cursor)
            self.assertEqual(42, store.get_cursor("chat"))
            self.assertEqual("pending", store.get_record("chat", 41)["status"])

    def test_rescan_does_not_requeue_successful_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.mark_success("chat", 41, file_name="41 - moved.mp4")

            store.enqueue_and_advance_cursor("chat", 41)

            self.assertEqual("success", store.get_record("chat", 41)["status"])
            self.assertEqual(42, store.get_cursor("chat"))

    def test_rescan_requeues_previously_skipped_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.mark_skipped("chat", 41)

            store.enqueue_and_advance_cursor("chat", 41)

            self.assertEqual("pending", store.get_record("chat", 41)["status"])

    def test_rescan_does_not_release_a_claimed_record(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "records.sqlite3")
            store = DownloadRecordStore(db_path)
            store.enqueue_and_advance_cursor("chat", 41)
            store.claim_next(["chat"])

            store.override_cursor("chat", 0)
            store.enqueue_and_advance_cursor("chat", 41)

            self.assertEqual("processing", store.get_record("chat", 41)["status"])
            self.assertIsNone(store.claim_next(["chat"]))

    def test_rescan_preserves_existing_pending_queue_position(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "records.sqlite3")
            store = DownloadRecordStore(db_path)
            store.mark_pending("chat", 41)
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    "UPDATE download_records SET updated_at = 1 "
                    "WHERE chat_id = 'chat' AND message_id = 41"
                )

            store.enqueue_and_advance_cursor("chat", 41)

            record = store.get_record("chat", 41)
            self.assertEqual("pending", record["status"])
            self.assertEqual(1, record["updated_at"])

    def test_rescan_preserves_failed_retry_state(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.mark_failed("chat", 41, "proxy error", retry_delay=60)
            before = store.get_record("chat", 41)

            store.enqueue_and_advance_cursor("chat", 41)

            after = store.get_record("chat", 41)
            self.assertEqual("failed", after["status"])
            self.assertEqual(before["next_retry_at"], after["next_retry_at"])

    def test_failed_update_does_not_downgrade_success(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.mark_success("chat", 41, file_name="41 - done.mp4")

            store.mark_failed("chat", 41, "late upload error", retry_delay=60)

            record = store.get_record("chat", 41)
            self.assertEqual("success", record["status"])
            self.assertEqual("", record["error"])
            self.assertEqual(0, record["next_retry_at"])

    def test_claim_and_restart_recovery(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.enqueue_and_advance_cursor("chat", 41)

            first_claim = store.claim_next(["chat"])

            self.assertEqual(41, first_claim["message_id"])
            self.assertEqual("processing", store.get_record("chat", 41)["status"])
            self.assertIsNone(store.claim_next(["chat"]))
            self.assertEqual(1, store.recover_processing())

            second_claim = store.claim_next(["chat"])
            self.assertEqual(41, second_claim["message_id"])
            self.assertEqual(2, second_claim["attempts"])

    def test_claims_in_message_id_order_not_queue_timestamp_order(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "records.sqlite3")
            store = DownloadRecordStore(db_path)
            store.mark_pending("chat", 200)
            store.mark_pending("chat", 100)
            with sqlite3.connect(db_path) as connection:
                connection.execute(
                    "UPDATE download_records SET updated_at = 1 "
                    "WHERE chat_id = 'chat' AND message_id = 200"
                )
                connection.execute(
                    "UPDATE download_records SET updated_at = 2, attempts = 5 "
                    "WHERE chat_id = 'chat' AND message_id = 100"
                )

            claimed = store.claim_next(["chat"])

            self.assertEqual(100, claimed["message_id"])

    def test_failed_record_waits_for_retry_delay(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.enqueue_and_advance_cursor("chat", 41)
            store.mark_failed("chat", 41, "proxy error", retry_delay=60)

            self.assertIsNone(store.claim_next(["chat"]))

            store.mark_failed("chat", 41, "retry now", retry_delay=0)
            self.assertEqual(41, store.claim_next(["chat"])["message_id"])

    def test_config_cursor_override_is_distinguished_from_stale_mirror(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            self.assertEqual(10, store.resolve_cursor("chat", 10))
            store.enqueue_and_advance_cursor("chat", 20)

            self.assertEqual(21, store.resolve_cursor("chat", 10))
            self.assertEqual(5, store.resolve_cursor("chat", 5))
            self.assertEqual(5, store.get_cursor("chat"))


if __name__ == "__main__":
    unittest.main()
