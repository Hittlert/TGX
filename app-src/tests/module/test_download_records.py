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

    def test_record_message_and_get_context(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))

            for i in range(1, 21):
                store.record_message(
                    chat_id="-1009999",
                    message_id=i,
                    text=f"Message {i}",
                    sender_id="1001",
                    sender_name="Alice",
                    media_type="text" if i % 2 == 0 else "photo",
                    date=1700000000 + i,
                )

            # Retrieve context for message 10
            context = store.get_message_context("-1009999", 10, limit_before=3, limit_after=3)
            msg_ids = [m["message_id"] for m in context]
            self.assertEqual([7, 8, 9, 10, 11, 12, 13], msg_ids)

            # Target message details
            target_msg = next(m for m in context if m["message_id"] == 10)
            self.assertEqual("Message 10", target_msg["text"])
            self.assertEqual("Alice", target_msg["sender_name"])
    def test_invalid_message_marked_skipped_after_10_attempts(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.enqueue_and_advance_cursor("chat", 1)

            # Retry 9 times with invalid message error
            for i in range(1, 10):
                store.mark_failed("chat", 1, "resolve Telegram media: invalid message 1")
                rec = store.get_record("chat", 1)
                self.assertEqual("failed", rec["status"])

            # 10th time: should transition to skipped
            store.mark_failed("chat", 1, "resolve Telegram media: invalid message 1")
            rec = store.get_record("chat", 1)
            self.assertEqual("skipped", rec["status"])
            self.assertIn("message does not exist", rec["error"])

    def test_clean_invalid_failed_records(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = DownloadRecordStore(os.path.join(tmp_dir, "records.sqlite3"))
            store.enqueue_and_advance_cursor("chat", 100)
            store.mark_failed("chat", 100, "resolve message: invalid message 100")

            cleaned = store.clean_invalid_failed_records()
            self.assertEqual(1, cleaned)
            rec = store.get_record("chat", 100)
            self.assertEqual("skipped", rec["status"])


if __name__ == "__main__":
    unittest.main()
