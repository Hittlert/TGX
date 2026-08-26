import unittest

from module.download_stripe_scheduler import DownloadStripeScheduler


class DownloadStripeSchedulerTest(unittest.TestCase):
    def test_round_robins_files_within_one_dc(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(4, "a", "a1")
        scheduler.enqueue(4, "a", "a2")
        scheduler.enqueue(4, "b", "b1")
        scheduler.enqueue(4, "b", "b2")

        self.assertEqual(
            ["a1", "b1", "a2", "b2"],
            [scheduler.pop_next(4) for _ in range(4)],
        )

    def test_single_file_consumes_all_available_turns(self):
        scheduler = DownloadStripeScheduler()
        for number in range(4):
            scheduler.enqueue(2, "only", number)

        self.assertEqual([0, 1, 2, 3], [scheduler.pop_next(2) for _ in range(4)])

    def test_never_pops_another_dc(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(2, "a", "dc2")
        scheduler.enqueue(5, "b", "dc5")

        self.assertEqual("dc2", scheduler.pop_next(2))
        self.assertIsNone(scheduler.pop_next(2))
        self.assertEqual("dc5", scheduler.pop_next(5))

    def test_cancel_and_remove_transfer_preserve_rotation(self):
        scheduler = DownloadStripeScheduler()
        scheduler.enqueue(4, "a", "a1")
        scheduler.enqueue(4, "a", "a2")
        scheduler.enqueue(4, "b", "b1")

        self.assertTrue(scheduler.cancel("a1"))
        self.assertEqual(["a2"], scheduler.remove_transfer(4, "a"))
        self.assertEqual("b1", scheduler.pop_next(4))
        self.assertEqual(0, scheduler.pending_count())
