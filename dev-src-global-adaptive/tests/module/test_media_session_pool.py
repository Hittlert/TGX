import asyncio
import collections
import dataclasses
import unittest

from pyrogram.errors import AuthBytesInvalid

from module.media_session_pool import (
    AdaptivePoolController,
    GlobalMediaSessionPool,
    KurigramMediaSessionFactory,
    MediaSessionPoolConfig,
    PoolWindow,
    ReusableMediaSession,
    ScaleDecision,
    _LeaseWaiter,
)


class FakeSession:
    def __init__(self, dc_id, number):
        self.dc_id = dc_id
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1


class RecordingInvokeSession(FakeSession):
    def __init__(self, dc_id, number):
        super().__init__(dc_id, number)
        self.invocations = []

    async def invoke(self, request, **kwargs):
        self.invocations.append((request, kwargs))
        return "response"


class BlockingStopSession(FakeSession):
    def __init__(self, dc_id, number):
        super().__init__(dc_id, number)
        self.stop_started = asyncio.Event()
        self.allow_stop = asyncio.Event()
        self.stop_completed = asyncio.Event()

    async def stop(self):
        self.stop_calls += 1
        self.stop_started.set()
        await self.allow_stop.wait()
        self.stop_completed.set()


class FailingStopSession(FakeSession):
    async def stop(self):
        self.stop_calls += 1
        raise RuntimeError("stale session stop failed")


class CancellingStopSession(FakeSession):
    async def stop(self):
        self.stop_calls += 1
        raise asyncio.CancelledError()


class FakeFactory:
    def __init__(self):
        self.sessions = []

    async def __call__(self, dc_id):
        session = FakeSession(dc_id, len(self.sessions))
        self.sessions.append(session)
        return session


class FirstBlockingStopFactory(FakeFactory):
    async def __call__(self, dc_id):
        if not self.sessions:
            session = BlockingStopSession(dc_id, 0)
            self.sessions.append(session)
            return session
        return await super().__call__(dc_id)


class BlockingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, dc_id):
        self.started.set()
        await self.release.wait()
        return await super().__call__(dc_id)


class HardLimitFactory(FakeFactory):
    def __init__(self, expected_creating):
        super().__init__()
        self.expected_creating = expected_creating
        self.started_dcs = []
        self.limit_reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, dc_id):
        self.started_dcs.append(dc_id)
        if len(self.started_dcs) == self.expected_creating:
            self.limit_reached.set()
        await self.release.wait()
        return await super().__call__(dc_id)


class TrackingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.active_by_dc = collections.Counter()
        self.max_active_by_dc = collections.Counter()
        self.active = 0
        self.max_active = 0

    async def __call__(self, dc_id):
        self.active_by_dc[dc_id] += 1
        self.max_active_by_dc[dc_id] = max(
            self.max_active_by_dc[dc_id],
            self.active_by_dc[dc_id],
        )
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        try:
            return await super().__call__(dc_id)
        finally:
            self.active -= 1
            self.active_by_dc[dc_id] -= 1


class RecoveringFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.error = RuntimeError("DC authorization exhausted")
        self.calls = 0
        self.started = asyncio.Event()
        self.release_failure = asyncio.Event()

    async def __call__(self, dc_id):
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            await self.release_failure.wait()
            raise self.error
        return await super().__call__(dc_id)


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class BlockingTick:
    def __init__(self):
        self.delays = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __call__(self, seconds):
        self.delays.append(seconds)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class RecordingController:
    def __init__(self, target, reason="recorded"):
        self.target = target
        self.reason = reason
        self.observations = []

    def observe(self, window, now):
        self.observations.append((window, now))
        return ScaleDecision(self.target, self.reason)


class FakeKurigramClient:
    def __init__(self, error):
        self.sessions_lock = asyncio.Lock()
        self.cached = FakeSession(4, "cached")
        self.media_sessions = {4: self.cached}
        self.error = error
        self.calls = 0

    async def get_session(self, dc_id, **kwargs):
        self.calls += 1
        raise self.error


class SerialKurigramClient:
    def __init__(self):
        self.sessions_lock = asyncio.Lock()
        self.media_sessions = {}
        self.active = 0
        self.max_active = 0
        self.session_kwargs = []

    async def get_session(self, dc_id, **kwargs):
        self.session_kwargs.append(kwargs)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        self.active -= 1
        return FakeSession(dc_id, dc_id)


class CdnKurigramClient:
    def __init__(self):
        self.sessions_lock = asyncio.Lock()
        self.media_sessions = {}
        self.sessions = {}
        self.media_session = FakeSession(5, "media")
        self.cdn_session = FakeSession(5, "cdn")
        self.cdn_calls = 0

    async def get_session(
        self,
        dc_id,
        is_media=False,
        is_cdn=False,
        temporary=False,
        **_kwargs,
    ):
        if is_cdn:
            self.asserted_temporary = temporary
            self.cdn_calls += 1
            return self.cdn_session
        if is_media:
            return self.media_session
        raise AssertionError(f"unexpected session request for DC {dc_id}")


class RecoveringAuthorizationKurigramClient:
    def __init__(self):
        self.sessions_lock = asyncio.Lock()
        self.stale_dc_session = FakeSession(5, "stale-dc")
        self.stale_media_session = FakeSession(5, "stale-media")
        self.fresh_session = FakeSession(5, "fresh")
        self.sessions = {5: self.stale_dc_session}
        self.media_sessions = {5: self.stale_media_session}
        self.calls = 0

    async def get_session(self, dc_id, **kwargs):
        del kwargs
        self.calls += 1
        if self.calls == 1:
            raise AuthBytesInvalid()
        if dc_id in self.sessions:
            raise AssertionError("stale ordinary DC session was reused")
        return self.fresh_session


class AdaptivePoolControllerTest(unittest.TestCase):
    def stable(self, goodput=10.0):
        return PoolWindow(
            pending=100,
            utilization=0.9,
            retry_rate=0.0,
            unhealthy_fraction=0.0,
            flood_wait=False,
            committed_bytes_per_second=goodput,
        )

    def test_scales_16_to_48_in_steps_of_eight(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        targets = []
        now = 0.0
        for goodput in (10, 12, 12, 15, 15, 18, 18, 21, 21, 24):
            decision = controller.observe(self.stable(goodput), now)
            targets.append(decision.target)
            now += 60

        self.assertEqual(48, max(targets))
        self.assertTrue(all(target <= 48 for target in targets))
        self.assertTrue(
            all(target in (16, 24, 32, 40, 48) for target in targets)
        )

    def test_absolute_cap_applies_to_direct_controller_use(self):
        controller = AdaptivePoolController(soft_target=40, hard_limit=64)
        targets = []
        now = 0.0
        for goodput in (10, 12, 12, 15, 15, 18):
            targets.append(controller.observe(self.stable(goodput), now).target)
            now += 60

        self.assertEqual(48, max(targets))

    def test_reverts_expansion_after_two_plateau_windows(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        self.assertEqual(24, controller.observe(self.stable(10), 0).target)
        controller.observe(self.stable(10.1), 60)

        decision = controller.observe(self.stable(10.2), 120)

        self.assertEqual(16, decision.target)
        self.assertEqual("plateau", decision.reason)
        self.assertGreater(decision.hold_until, 120)

    def test_accepts_expansion_when_two_window_average_meets_threshold(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        controller.observe(self.stable(100), 0)
        controller.observe(self.stable(110), 60)

        decision = controller.observe(self.stable(100), 120)

        self.assertEqual(24, decision.target)
        self.assertEqual("goodput_growth", decision.reason)

    def test_reverts_expansion_when_two_window_average_is_below_threshold(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        controller.observe(self.stable(100), 0)
        controller.observe(self.stable(80), 60)

        decision = controller.observe(self.stable(110), 120)

        self.assertEqual(16, decision.target)
        self.assertEqual("plateau", decision.reason)

    def test_flood_wait_freezes_growth(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        window = dataclasses.replace(self.stable(), flood_wait=True)

        decision = controller.observe(window, 0)

        self.assertEqual(16, decision.target)
        self.assertEqual("flood_wait", decision.reason)

    def test_unhealthy_window_reduces_target_by_eight(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        controller._target = 32
        window = dataclasses.replace(self.stable(), unhealthy_fraction=0.2)

        decision = controller.observe(window, 0)

        self.assertEqual(24, decision.target)
        self.assertEqual("unhealthy", decision.reason)

    def test_unhealthy_window_can_reduce_below_default_demand_seed(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        window = dataclasses.replace(self.stable(), unhealthy_fraction=0.2)

        decision = controller.observe(window, 0)

        self.assertEqual(8, decision.target)
        self.assertEqual("unhealthy", decision.reason)

    def test_no_pending_work_sets_demand_target_to_zero(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        window = dataclasses.replace(self.stable(), pending=0, utilization=0.0)

        decision = controller.observe(window, 0)

        self.assertEqual(0, decision.target)
        self.assertEqual("idle", decision.reason)

    def test_demand_after_idle_restarts_at_soft_target(self):
        controller = AdaptivePoolController(soft_target=32, hard_limit=48)
        idle = dataclasses.replace(self.stable(), pending=0, utilization=0.0)
        controller.observe(idle, 0)

        decision = controller.observe(self.stable(), 60)

        self.assertEqual(32, decision.target)
        self.assertEqual("demand", decision.reason)

    def test_retry_or_low_utilization_blocks_expansion(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        retrying = dataclasses.replace(self.stable(), retry_rate=0.02)
        underused = dataclasses.replace(self.stable(), utilization=0.79)

        self.assertEqual("retry_rate", controller.observe(retrying, 0).reason)
        self.assertEqual("underutilized", controller.observe(underused, 60).reason)
        self.assertEqual(16, controller.observe(underused, 60).target)

    def test_plateau_hold_blocks_growth_until_deadline(self):
        controller = AdaptivePoolController(soft_target=16, hard_limit=48)
        controller.observe(self.stable(10), 0)
        controller.observe(self.stable(10.1), 60)
        plateau = controller.observe(self.stable(10.2), 120)

        held = controller.observe(self.stable(20), plateau.hold_until - 1)
        resumed = controller.observe(self.stable(20), plateau.hold_until)

        self.assertEqual(16, held.target)
        self.assertEqual("plateau_hold", held.reason)
        self.assertEqual(24, resumed.target)
        self.assertEqual("expand", resumed.reason)


class GlobalMediaSessionPoolTest(unittest.IsolatedAsyncioTestCase):
    def test_config_allows_lower_fixed_benchmark_target(self):
        config = MediaSessionPoolConfig(
            soft_sessions=8,
            max_sessions=8,
            pipeline_depth=1,
            adaptive=False,
        )

        self.assertEqual(8, config.soft_sessions)
        self.assertEqual(8, config.max_sessions)
        self.assertFalse(config.adaptive)

    def test_config_rejects_session_limit_above_absolute_cap(self):
        with self.assertRaisesRegex(ValueError, "max_sessions.*48"):
            MediaSessionPoolConfig(max_sessions=49)

    def test_config_rejects_pipeline_depth_above_absolute_cap(self):
        with self.assertRaisesRegex(ValueError, "pipeline_depth.*2"):
            MediaSessionPoolConfig(pipeline_depth=3)

    async def test_never_exceeds_live_plus_creating_hard_limit(self):
        factory = BlockingFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=4, max_sessions=4, pipeline_depth=1),
        )
        tasks = [asyncio.create_task(pool.acquire(4, f"file-{i}")) for i in range(8)]
        await factory.started.wait()
        self.assertLessEqual(pool.snapshot().live + pool.snapshot().creating, 4)
        factory.release.set()
        leases = await asyncio.gather(*tasks[:4])
        for task in tasks[4:]:
            task.cancel()
        cancelled = await asyncio.gather(*tasks[4:], return_exceptions=True)
        self.assertTrue(
            all(isinstance(result, asyncio.CancelledError) for result in cancelled),
            cancelled,
        )
        self.assertEqual(0, pool.snapshot().pending)
        for lease in leases:
            await lease.release()
        await asyncio.wait_for(pool.close(), 1)

    async def test_hard_limit_counts_parallel_creating_sessions(self):
        factory = HardLimitFactory(expected_creating=4)
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=4, max_sessions=4, pipeline_depth=1),
        )
        tasks = [
            asyncio.create_task(pool.acquire(dc_id, f"dc-{dc_id}"))
            for dc_id in range(1, 9)
        ]

        await asyncio.wait_for(factory.limit_reached.wait(), 1)
        await asyncio.sleep(0)
        snapshot = pool.snapshot()
        self.assertEqual(4, snapshot.creating)
        self.assertEqual(4, snapshot.live + snapshot.creating)
        self.assertEqual(4, len(factory.started_dcs))

        factory.release.set()
        leases = await asyncio.gather(*tasks[:4])
        for task in tasks[4:]:
            task.cancel()
        cancelled = await asyncio.gather(*tasks[4:], return_exceptions=True)
        self.assertTrue(
            all(isinstance(result, asyncio.CancelledError) for result in cancelled),
            cancelled,
        )
        self.assertEqual(0, pool.snapshot().pending)
        for lease in leases:
            await lease.release()
        await asyncio.wait_for(pool.close(), 1)

    async def test_sessions_never_cross_dc_boundaries(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=2, max_sessions=2, pipeline_depth=1),
        )
        lease2, lease5 = await asyncio.gather(
            pool.acquire(2, "dc2-file"),
            pool.acquire(5, "dc5-file"),
        )
        self.assertEqual(2, lease2.dc_id)
        self.assertEqual(5, lease5.dc_id)
        self.assertEqual(2, lease2.session.dc_id)
        self.assertEqual(5, lease5.session.dc_id)
        await lease2.release()
        await lease5.release()
        await pool.close()

    async def test_pipeline_depth_allows_two_leases_per_session(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=2),
        )
        first, second = await asyncio.gather(
            pool.acquire(4, "a"),
            pool.acquire(4, "b"),
        )
        self.assertIs(first.session, second.session)
        self.assertEqual(2, pool.snapshot().pipeline_depth)
        self.assertEqual(2, pool.snapshot().active_slots)
        await first.release()
        await second.release()
        await pool.close()

    async def test_snapshot_returns_defensive_by_dc_copy(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
        )
        lease = await pool.acquire(4, "snapshot")
        await lease.release()

        first = pool.snapshot()
        first.by_dc[4]["live"] = 999
        first.by_dc[4]["caller_only"] = 1
        first.by_dc[99] = {"live": 1}
        second = pool.snapshot()

        self.assertEqual(1, second.by_dc[4]["live"])
        self.assertNotIn("caller_only", second.by_dc[4])
        self.assertNotIn(99, second.by_dc)
        self.assertEqual(second.by_dc, dataclasses.asdict(second)["by_dc"])
        await pool.close()

    async def test_snapshot_is_readable_from_a_worker_thread(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1),
        )
        lease = await pool.acquire(4, "thread-reader")

        snapshots = await asyncio.to_thread(
            lambda: [pool.snapshot() for _index in range(100)]
        )

        self.assertTrue(all(snapshot.live == 1 for snapshot in snapshots))
        self.assertTrue(all(snapshot.by_dc[4]["active"] == 1 for snapshot in snapshots))
        await lease.release()
        await pool.close()

    async def test_start_creates_one_control_task_and_close_awaits_it(self):
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(control_interval=17),
            tick=tick,
        )

        pool.start()
        control_task = pool._control_task
        pool.start()
        await tick.started.wait()

        self.assertIs(control_task, pool._control_task)
        self.assertEqual([17], tick.delays)
        await pool.close()
        self.assertTrue(control_task.done())
        self.assertTrue(tick.cancelled.is_set())

    async def test_fixed_target_start_does_not_create_control_task(self):
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=2,
                max_sessions=4,
                pipeline_depth=1,
                adaptive=False,
            ),
            tick=tick,
        )

        pool.start()
        first, second = await asyncio.gather(
            pool.acquire(4, "fixed-a"),
            pool.acquire(4, "fixed-b"),
        )

        self.assertIsNone(pool._control_task)
        self.assertEqual(2, pool.snapshot().desired)
        self.assertEqual("fixed_target", pool.snapshot().last_scale_reason)
        self.assertFalse(tick.started.is_set())
        await first.release()
        await second.release()
        await pool.close()

    async def test_control_window_uses_metric_deltas_and_runtime_inputs(self):
        clock = FakeClock()
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
            clock=clock,
            tick=tick,
        )
        controller = RecordingController(target=1)
        pool._controller = controller
        lease = await pool.acquire(4, "metrics")
        pool._entries[0].force_unhealthy = True
        pool.record_committed(600)
        pool.record_stripe_attempt()
        pool.record_stripe_attempt()
        pool.record_retry()
        pool.pause_dc(4, 60)

        clock.advance(10)
        await pool._control_once()
        first, first_now = controller.observations[-1]
        clock.advance(10)
        await pool._control_once()
        second, second_now = controller.observations[-1]

        self.assertEqual(10, first_now)
        self.assertEqual(60.0, first.committed_bytes_per_second)
        self.assertEqual(1.0, first.utilization)
        self.assertEqual(0.5, first.retry_rate)
        self.assertEqual(1.0, first.unhealthy_fraction)
        self.assertTrue(first.flood_wait)
        self.assertEqual(20, second_now)
        self.assertEqual(0.0, second.committed_bytes_per_second)
        self.assertEqual(0.0, second.retry_rate)
        await lease.release()
        await pool.close()

    async def test_retry_rate_uses_attempt_delta_after_work_disappears(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
            clock=clock,
        )
        controller = RecordingController(target=0)
        pool._controller = controller
        lease = await pool.acquire(4, "completed")
        for _attempt in range(4):
            pool.record_stripe_attempt()
        pool.record_retry()
        await lease.release()

        clock.advance(10)
        await pool._control_once()
        first, _first_now = controller.observations[-1]
        pool.record_retry()
        clock.advance(10)
        await pool._control_once()
        second, _second_now = controller.observations[-1]

        self.assertEqual(0, first.pending)
        self.assertEqual(0.25, first.retry_rate)
        self.assertEqual(0.0, second.retry_rate)
        await pool.close()

    async def test_full_utilization_without_queued_waiters_does_not_grow(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=2,
                pipeline_depth=1,
            ),
            clock=clock,
        )
        lease = await pool.acquire(4, "active-only")

        clock.advance(60)
        await pool._control_once()

        self.assertEqual(1, pool.snapshot().desired)
        self.assertEqual("idle", pool.snapshot().last_scale_reason)
        self.assertEqual(1, pool.snapshot().live)
        await lease.release()
        await pool.close()

    async def test_control_growth_starts_builder_for_waiting_dc(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=2,
                pipeline_depth=1,
            ),
        )
        pool._controller = RecordingController(target=2, reason="expand")
        first = await pool.acquire(4, "builder-a")
        waiting = asyncio.create_task(pool.acquire(4, "builder-b"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertFalse(waiting.done())

        await pool._control_once()
        second = await asyncio.wait_for(waiting, 1)

        self.assertIsNot(first.session, second.session)
        self.assertEqual(2, pool.snapshot().live)
        await first.release()
        await second.release()
        await pool.close()

    async def test_control_contraction_reaps_excess_idle_in_lru_order(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=2,
                max_sessions=2,
                pipeline_depth=1,
            ),
            clock=clock,
        )
        old, recent = await asyncio.gather(
            pool.acquire(2, "old"),
            pool.acquire(4, "recent"),
        )
        await old.release()
        clock.advance(1)
        await recent.release()
        pool._controller = RecordingController(target=1, reason="unhealthy")

        await pool._control_once()

        self.assertEqual(1, old.session.stop_calls)
        self.assertEqual(0, recent.session.stop_calls)
        self.assertEqual(1, pool.snapshot().live)
        self.assertEqual(1, pool.snapshot().evicted)
        await pool.close()

    async def test_control_contraction_drains_active_excess_session(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=2,
                max_sessions=2,
                pipeline_depth=1,
            ),
        )
        first, second = await asyncio.gather(
            pool.acquire(4, "first"),
            pool.acquire(4, "second"),
        )
        waiting = asyncio.create_task(pool.acquire(4, "waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        pool._controller = RecordingController(target=1, reason="plateau")

        await pool._control_once()

        self.assertEqual(1, pool.snapshot().desired)
        retiring = [entry for entry in pool._entries if entry.retiring]
        self.assertEqual(1, len(retiring))
        await pool._control_once()
        self.assertEqual(1, sum(entry.retiring for entry in pool._entries))
        retiring_session = retiring[0].session
        retiring_lease = first if first.session is retiring_session else second
        survivor_lease = second if retiring_lease is first else first
        survivor_session = survivor_lease.session

        await retiring_lease.release()
        self.assertEqual(1, retiring_session.stop_calls)
        self.assertFalse(waiting.done())

        await survivor_lease.release()
        reused = await asyncio.wait_for(waiting, 1)
        self.assertIs(survivor_session, reused.session)
        self.assertEqual(1, pool.snapshot().live)
        self.assertEqual(1, pool.snapshot().evicted)
        await reused.release()
        await pool.close()

    async def test_control_contraction_keeps_one_session_for_pending_work(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
        )
        held = await pool.acquire(4, "held")
        waiting = asyncio.create_task(pool.acquire(4, "waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        pool._controller = RecordingController(target=0, reason="unhealthy")

        await pool._control_once()

        self.assertEqual(1, pool.snapshot().desired)
        self.assertFalse(pool._entries[0].retiring)
        await held.release()
        reused = await asyncio.wait_for(waiting, 1)
        self.assertIs(held.session, reused.session)
        await reused.release()
        await pool.close()

    async def test_control_contraction_preserves_each_pending_dc(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=3,
                max_sessions=3,
                pipeline_depth=1,
            ),
            clock=clock,
        )
        dc1 = await pool.acquire(1, "dc1-active")
        clock.advance(1)
        dc5_first, dc5_second = await asyncio.gather(
            pool.acquire(5, "dc5-first"),
            pool.acquire(5, "dc5-second"),
        )
        waiting_dc1 = asyncio.create_task(pool.acquire(1, "dc1-waiting"))
        waiting_dc5 = asyncio.create_task(pool.acquire(5, "dc5-waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        pool._controller = RecordingController(target=2, reason="plateau")

        try:
            await pool._control_once()

            dc1_entry = next(
                entry for entry in pool._entries if entry.session is dc1.session
            )
            retiring_dc5 = [
                entry
                for entry in pool._entries
                if entry.dc_id == 5 and entry.retiring
            ]
            self.assertFalse(dc1_entry.retiring)
            self.assertEqual(1, len(retiring_dc5))
        finally:
            waiting_dc1.cancel()
            waiting_dc5.cancel()
            await asyncio.gather(
                waiting_dc1,
                waiting_dc5,
                return_exceptions=True,
            )
            await dc1.release()
            await dc5_first.release()
            await dc5_second.release()
            await pool.close()

    async def test_builder_rebalance_preserves_pending_dc_session(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=2,
                max_sessions=2,
                pipeline_depth=1,
            ),
            clock=clock,
        )
        dc1 = await pool.acquire(1, "dc1-active")
        clock.advance(1)
        dc2 = await pool.acquire(2, "dc2-active")
        waiting_dc1 = asyncio.create_task(pool.acquire(1, "dc1-waiting"))
        await asyncio.sleep(0)
        waiting_dc5 = asyncio.create_task(pool.acquire(5, "dc5-waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        try:
            dc1_entry = next(
                entry for entry in pool._entries if entry.session is dc1.session
            )
            dc2_entry = next(
                entry for entry in pool._entries if entry.session is dc2.session
            )
            self.assertFalse(dc1_entry.retiring)
            self.assertTrue(dc2_entry.retiring)

            pool._controller = RecordingController(target=2, reason="plateau")
            await pool._control_once()
            self.assertEqual(1, sum(entry.retiring for entry in pool._entries))
        finally:
            waiting_dc1.cancel()
            waiting_dc5.cancel()
            await asyncio.gather(
                waiting_dc1,
                waiting_dc5,
                return_exceptions=True,
            )
            await dc1.release()
            await dc2.release()
            await pool.close()

    async def test_builder_rebalance_rotates_when_pending_dcs_exceed_capacity(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
        )
        dc1 = await pool.acquire(1, "dc1-active")
        waiting_dc1 = asyncio.create_task(pool.acquire(1, "dc1-waiting"))
        await asyncio.sleep(0)
        waiting_dc5 = asyncio.create_task(pool.acquire(5, "dc5-waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        dc5 = None
        reused_dc1 = None
        try:
            self.assertTrue(pool._entries[0].retiring)
            await dc1.release()
            dc5 = await asyncio.wait_for(waiting_dc5, 1)
            self.assertFalse(waiting_dc1.done())

            await dc5.release()
            reused_dc1 = await asyncio.wait_for(waiting_dc1, 1)
            self.assertEqual(1, reused_dc1.dc_id)
        finally:
            waiting_dc1.cancel()
            waiting_dc5.cancel()
            await asyncio.gather(
                waiting_dc1,
                waiting_dc5,
                return_exceptions=True,
            )
            await dc1.release()
            if dc5 is not None:
                await dc5.release()
            if reused_dc1 is not None:
                await reused_dc1.release()
            await pool.close()

    async def test_builder_rebalance_rotates_across_three_pending_dcs(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
        )
        current = await pool.acquire(1, "dc1-active")
        waiters = {
            dc_id: [
                asyncio.create_task(pool.acquire(dc_id, f"dc{dc_id}-{index}"))
                for index in range(3)
            ]
            for dc_id in (1, 5, 6)
        }
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        targets = []
        try:
            for _ in range(3):
                target_dc = pool._entries[0].replacement_dc_id
                self.assertIsNotNone(target_dc)
                targets.append(target_dc)
                await current.release()
                current = await asyncio.wait_for(waiters[target_dc].pop(0), 1)

            self.assertEqual([5, 6, 1], targets)
        finally:
            for tasks in waiters.values():
                for task in tasks:
                    task.cancel()
            await asyncio.gather(
                *(task for tasks in waiters.values() for task in tasks),
                return_exceptions=True,
            )
            await current.release()
            await pool.close()

    async def test_cancelled_replacement_keeps_healthy_session(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
        )
        current = await pool.acquire(1, "dc1-active")
        session = current.session
        waiting_dc1 = asyncio.create_task(pool.acquire(1, "dc1-waiting"))
        await asyncio.sleep(0)
        waiting_dc5 = asyncio.create_task(pool.acquire(5, "dc5-waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        reused = None
        try:
            self.assertEqual(5, pool._entries[0].replacement_dc_id)
            waiting_dc5.cancel()
            await asyncio.gather(waiting_dc5, return_exceptions=True)

            self.assertFalse(pool._entries[0].retiring)
            self.assertIsNone(pool._entries[0].replacement_dc_id)
            await current.release()
            reused = await asyncio.wait_for(waiting_dc1, 1)
            self.assertIs(session, reused.session)
            self.assertEqual(0, session.stop_calls)
        finally:
            waiting_dc1.cancel()
            waiting_dc5.cancel()
            await asyncio.gather(
                waiting_dc1,
                waiting_dc5,
                return_exceptions=True,
            )
            await current.release()
            if reused is not None:
                await reused.release()
            await pool.close()

    async def test_builder_rebalance_does_not_protect_paused_dc(self):
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
            ),
            tick=tick,
        )
        dc1 = await pool.acquire(1, "dc1-active")
        pool.pause_dc(1, 60)
        waiting_dc1 = asyncio.create_task(pool.acquire(1, "dc1-paused"))
        await asyncio.sleep(0)
        waiting_dc5 = asyncio.create_task(pool.acquire(5, "dc5-waiting"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        dc5 = None
        try:
            self.assertTrue(pool._entries[0].retiring)
            await dc1.release()
            dc5 = await asyncio.wait_for(waiting_dc5, 1)
            self.assertFalse(waiting_dc1.done())
        finally:
            waiting_dc1.cancel()
            waiting_dc5.cancel()
            await asyncio.gather(
                waiting_dc1,
                waiting_dc5,
                return_exceptions=True,
            )
            await dc1.release()
            if dc5 is not None:
                await dc5.release()
            await pool.close()

    async def test_control_tick_does_not_expire_pending_dc_session(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
                idle_ttl=10,
            ),
            clock=clock,
        )
        initial = await pool.acquire(4, "initial")
        session = initial.session
        await initial.release()
        clock.advance(11)
        waiting = asyncio.create_task(pool.acquire(4, "pending"))
        await asyncio.sleep(0)
        pool._controller = RecordingController(target=1, reason="demand")

        reused = None
        try:
            await pool._control_once()

            reused = await asyncio.wait_for(waiting, 1)
            self.assertIs(session, reused.session)
            self.assertEqual(0, session.stop_calls)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
            if reused is not None:
                await reused.release()
            await pool.close()

    async def test_control_tick_reaps_only_expired_idle_sessions(self):
        factory = FakeFactory()
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
                idle_ttl=600,
            ),
            clock=clock,
        )
        lease = await pool.acquire(4, "idle")
        session = lease.session
        await lease.release()

        clock.advance(599)
        await pool._control_once()
        self.assertEqual(0, session.stop_calls)

        clock.advance(2)
        await pool._control_once()
        self.assertEqual(1, session.stop_calls)
        self.assertEqual(0, pool.snapshot().live)
        await pool.close()

    async def test_control_tick_preserves_idle_session_while_dc_is_paused(self):
        clock = FakeClock()
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
                idle_ttl=10,
            ),
            clock=clock,
            tick=tick,
        )
        lease = await pool.acquire(4, "paused-idle")
        session = lease.session
        await lease.release()
        pool.pause_dc(4, 60)
        await tick.started.wait()

        clock.advance(11)
        await pool._control_once()

        self.assertEqual(0, session.stop_calls)
        self.assertEqual(1, pool.snapshot().live)
        await pool.close()

    async def test_close_awaits_control_task_idle_stop_exactly_once(self):
        factory = FirstBlockingStopFactory()
        clock = FakeClock()
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=1,
                idle_ttl=10,
                control_interval=5,
            ),
            clock=clock,
            tick=tick,
        )
        lease = await pool.acquire(4, "control-stop")
        session = lease.session
        await lease.release()
        clock.advance(11)
        tick.release.set()
        pool.start()
        await session.stop_started.wait()

        close_task = asyncio.create_task(pool.close())
        await asyncio.sleep(0)
        self.assertFalse(close_task.done())
        self.assertEqual(1, session.stop_calls)
        session.allow_stop.set()
        await asyncio.wait_for(close_task, 1)

        self.assertEqual(1, session.stop_calls)
        self.assertEqual(0, pool.snapshot().live)

    async def test_pause_dc_uses_injected_tick_and_wakes_only_that_dc(self):
        clock = FakeClock()
        tick = BlockingTick()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=2,
                max_sessions=2,
                pipeline_depth=1,
            ),
            clock=clock,
            tick=tick,
        )
        dc4, dc5 = await asyncio.gather(
            pool.acquire(4, "dc4-initial"),
            pool.acquire(5, "dc5-initial"),
        )
        await dc4.release()
        await dc5.release()
        pool.pause_dc(4, 60)
        await tick.started.wait()
        waiting4 = asyncio.create_task(pool.acquire(4, "dc4-waiting"))
        ready5 = asyncio.create_task(pool.acquire(5, "dc5-ready"))
        lease5 = await asyncio.wait_for(ready5, 1)
        self.assertFalse(waiting4.done())

        clock.advance(60)
        tick.release.set()
        lease4 = await asyncio.wait_for(waiting4, 1)

        self.assertEqual(4, lease4.dc_id)
        self.assertEqual(5, lease5.dc_id)
        await lease4.release()
        await lease5.release()
        await pool.close()

    async def test_control_tick_logs_exactly_one_complete_snapshot(self):
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(
                soft_sessions=1,
                max_sessions=1,
                pipeline_depth=2,
            ),
            clock=clock,
        )
        lease = await pool.acquire(4, "logged")
        pool.record_committed(120)
        pool.record_retry()
        pool.record_fallback()
        clock.advance(60)

        with self.assertLogs("module.media_session_pool", level="INFO") as logs:
            await pool._control_once()

        self.assertEqual(1, len(logs.output))
        message = logs.output[0]
        for field in (
            "pipeline_depth=2",
            "by_dc={4:",
            "committed_bytes_per_second=2.0",
            "created=1",
            "evicted=0",
            "retries=1",
            "flood_waits=0",
            "fallbacks=1",
            "last_scale_reason=",
        ):
            self.assertIn(field, message)
        await lease.release()
        await pool.close()

    async def test_each_waiting_dc_receives_capacity(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=3, max_sessions=3, pipeline_depth=1),
        )
        leases = await asyncio.gather(
            pool.acquire(2, "dc2-a"),
            pool.acquire(4, "dc4-a"),
            pool.acquire(5, "dc5-a"),
        )
        self.assertEqual({2, 4, 5}, {lease.dc_id for lease in leases})
        for lease in leases:
            await lease.release()
        await pool.close()

    async def test_session_creation_is_serial_within_each_dc(self):
        factory = TrackingFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=4, max_sessions=4, pipeline_depth=1),
        )
        leases = await asyncio.gather(
            *(pool.acquire(4, f"file-{number}") for number in range(4))
        )
        self.assertEqual(1, factory.max_active_by_dc[4])
        for lease in leases:
            await lease.release()
        await pool.close()

    async def test_session_creation_for_different_dcs_can_overlap(self):
        factory = TrackingFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=2, max_sessions=2, pipeline_depth=1),
        )
        first, second = await asyncio.gather(
            pool.acquire(2, "dc2"),
            pool.acquire(5, "dc5"),
        )
        self.assertEqual(2, factory.max_active)
        await first.release()
        await second.release()
        await pool.close()

    async def test_lru_idle_session_is_evicted_for_waiting_dc(self):
        factory = FakeFactory()
        clock = FakeClock()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=2, max_sessions=2, pipeline_depth=1),
            clock=clock,
        )
        old = await pool.acquire(2, "old")
        await old.release()
        clock.advance(1)
        recent = await pool.acquire(4, "recent")
        await recent.release()

        dc5 = await pool.acquire(5, "new-dc")

        self.assertEqual(5, dc5.dc_id)
        self.assertEqual(1, old.session.stop_calls)
        self.assertEqual(0, recent.session.stop_calls)
        await dc5.release()
        await pool.close()

    async def test_waiting_files_receive_round_robin_leases(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        held = await pool.acquire(4, "held")
        order = []

        async def waiter(owner):
            lease = await pool.acquire(4, owner)
            order.append(owner)
            await lease.release()

        tasks = [
            asyncio.create_task(waiter("a")),
            asyncio.create_task(waiter("a")),
            asyncio.create_task(waiter("b")),
            asyncio.create_task(waiter("b")),
        ]
        await held.release()
        await asyncio.gather(*tasks)
        self.assertEqual(["a", "b", "a", "b"], order)
        await pool.close()

    async def test_unhealthy_session_is_stopped_and_replaced(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        bad = await pool.acquire(4, "a")
        bad_session = bad.session
        bad.mark_unhealthy()
        await bad.release()
        replacement = await pool.acquire(4, "a")
        self.assertIsNot(bad_session, replacement.session)
        self.assertEqual(1, bad_session.stop_calls)
        await replacement.release()
        await pool.close()

    async def test_two_transport_failures_retire_a_session(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        first = await pool.acquire(4, "a")
        original = first.session
        first.mark_transport_failure()
        await first.release()
        second = await pool.acquire(4, "a")
        self.assertIs(original, second.session)
        second.mark_transport_failure()
        await second.release()
        replacement = await pool.acquire(4, "a")
        self.assertIsNot(original, replacement.session)
        await replacement.release()
        await pool.close()

    async def test_success_resets_transport_failure_streak(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        first = await pool.acquire(4, "a")
        original = first.session
        first.mark_transport_failure()
        await first.release()
        successful = await pool.acquire(4, "a")
        await successful.release()
        third = await pool.acquire(4, "a")
        third.mark_transport_failure()
        await third.release()
        fourth = await pool.acquire(4, "a")
        self.assertIs(original, fourth.session)
        await fourth.release()
        await pool.close()

    async def test_transfer_exit_cancels_queued_waiters_and_unregisters_file(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        held = await pool.acquire(4, "held")

        async with pool.transfer(4, "cancelled"):
            waiter = asyncio.create_task(pool.acquire(4, "cancelled"))
            await asyncio.sleep(0)
            self.assertEqual(1, pool.snapshot().active_files)
            self.assertEqual(1, pool.snapshot().pending)

        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(0, pool.snapshot().active_files)
        self.assertEqual(0, pool.snapshot().pending)
        await held.release()
        await pool.close()

    async def test_repeated_cancellation_defers_transfer_teardown(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        held = await pool.acquire(4, "held")
        leave_transfer = asyncio.Event()
        body_ready = asyncio.Event()
        waiter = None

        async def run_transfer():
            nonlocal waiter
            async with pool.transfer(4, "cancelled"):
                waiter = asyncio.create_task(pool.acquire(4, "cancelled"))
                await asyncio.sleep(0)
                body_ready.set()
                await leave_transfer.wait()

        transfer_task = asyncio.create_task(run_transfer())
        await asyncio.wait_for(body_ready.wait(), 1)
        await pool._lock.acquire()
        leave_transfer.set()
        await asyncio.sleep(0)

        transfer_task.cancel()
        await asyncio.sleep(0)
        transfer_task.cancel()
        await asyncio.sleep(0)
        pool._lock.release()
        result = await asyncio.gather(transfer_task, return_exceptions=True)
        snapshot = pool.snapshot()

        if waiter is not None and not waiter.done():
            waiter.cancel()
        if waiter is not None:
            await asyncio.gather(waiter, return_exceptions=True)
        await held.release()
        await pool.close()

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(0, snapshot.active_files)
        self.assertEqual(0, snapshot.pending)

    async def test_cancel_during_fair_batch_removes_scheduler_waiter(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        waiter = asyncio.create_task(pool.acquire(4, "cancelled-batch"))
        await asyncio.sleep(0)

        waiter.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiter

        self.assertEqual(0, pool.snapshot().pending)
        await asyncio.wait_for(pool.close(), 1)

    async def test_repeated_cancellation_releases_dispatched_orphan_lease(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        held = await pool.acquire(4, "held")
        acquire_task = asyncio.create_task(pool.acquire(4, "orphaned"))
        await asyncio.sleep(0)
        self.assertEqual(1, pool.snapshot().pending)

        await pool._lock.acquire()
        orphaned_lease = None
        try:
            held._entry.active_slots -= 1
            held._released = True
            waiter = pool._waiters._items[4]["orphaned"][0]
            pool._dispatch_locked(4)
            orphaned_lease = waiter.future.result()

            acquire_task.cancel()
            await asyncio.sleep(0)
            acquire_task.cancel()
            await asyncio.sleep(0)
        finally:
            pool._lock.release()

        result = await asyncio.gather(acquire_task, return_exceptions=True)
        snapshot = pool.snapshot()
        if orphaned_lease is not None:
            await orphaned_lease.release()
        await pool.close()

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(0, snapshot.active_slots)
        self.assertEqual(0, snapshot.pending)

    async def test_pause_dc_delays_new_lease_until_wake_up(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        initial = await pool.acquire(4, "initial")
        await initial.release()

        pool.pause_dc(4, 0.02)
        waiter = asyncio.create_task(pool.acquire(4, "paused"))
        await asyncio.sleep(0)
        self.assertFalse(waiter.done())
        self.assertEqual(1, pool.snapshot().flood_waits)

        lease = await asyncio.wait_for(waiter, 1)
        self.assertIs(initial.session, lease.session)
        await lease.release()
        await pool.close()

    async def test_shorter_repeated_pause_does_not_cancel_later_wake_up(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        initial = await pool.acquire(4, "initial")
        await initial.release()

        pool.pause_dc(4, 0.03)
        pool.pause_dc(4, 0.01)
        waiter = asyncio.create_task(pool.acquire(4, "paused"))

        lease = await asyncio.wait_for(waiter, 1)
        await lease.release()
        await pool.close()

    async def test_creation_failure_reaches_current_dc_waiters_and_later_recovers(self):
        factory = RecoveringFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        first = asyncio.create_task(pool.acquire(4, "a"))
        second = asyncio.create_task(pool.acquire(4, "b"))
        await factory.started.wait()
        await asyncio.sleep(0)
        self.assertEqual(2, pool.snapshot().pending)
        factory.release_failure.set()

        results = await asyncio.wait_for(
            asyncio.gather(first, second, return_exceptions=True),
            1,
        )
        self.assertEqual([factory.error, factory.error], results)
        self.assertEqual(0, pool.snapshot().creating)
        self.assertEqual(0, pool.snapshot().pending)

        recovered = await asyncio.wait_for(pool.acquire(4, "later"), 1)
        self.assertEqual(2, factory.calls)
        await recovered.release()
        await pool.close()

    async def test_close_stops_every_owned_session_once_and_cancels_waiters(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        lease = await pool.acquire(4, "a")
        waiter = asyncio.create_task(pool.acquire(4, "b"))
        close_task = asyncio.create_task(pool.close())
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertFalse(close_task.done())
        await lease.release()
        await close_task
        await pool.close()
        self.assertEqual(1, lease.session.stop_calls)
        with self.assertRaises(asyncio.CancelledError):
            await pool.acquire(4, "after-close")

    async def test_close_cleans_builder_cancelled_before_first_instruction(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        old = await pool.acquire(2, "old")
        await old.release()

        loop = asyncio.get_running_loop()
        waiter = _LeaseWaiter(5, "new-dc", loop.create_future())
        async with pool._lock:
            pool._waiters.enqueue(5, "new-dc", waiter)
            pool._ensure_builder_locked(5)
            builder = pool._builders[5]
            self.assertEqual(1, pool._creating)

        await pool.close()

        self.assertTrue(builder.cancelled())
        self.assertEqual(1, old.session.stop_calls)
        self.assertEqual(0, pool.snapshot().creating)
        self.assertEqual(0, pool.snapshot().live)

    async def test_close_awaits_one_inflight_lru_stop_after_builder_cancellation(self):
        factory = FirstBlockingStopFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        old = await pool.acquire(2, "old")
        await old.release()

        waiter = asyncio.create_task(pool.acquire(5, "new-dc"))
        await old.session.stop_started.wait()
        builder = pool._builders[5]
        close_task = asyncio.create_task(pool.close())
        while not builder.cancelling():
            await asyncio.sleep(0)
        await asyncio.sleep(0)

        self.assertFalse(close_task.done())
        self.assertEqual(1, old.session.stop_calls)

        old.session.allow_stop.set()
        await close_task
        with self.assertRaises(asyncio.CancelledError):
            await waiter
        self.assertEqual(1, old.session.stop_calls)

    async def test_pause_after_close_creates_no_wake_or_counter_state(self):
        pool = GlobalMediaSessionPool(
            FakeFactory(),
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        await pool.close()
        before = pool.snapshot()

        pool.pause_dc(4, 60)

        self.assertEqual(before, pool.snapshot())
        self.assertEqual({}, pool._paused_until)
        self.assertEqual({}, pool._wake_by_dc)
        self.assertEqual(set(), pool._wake_tasks)

    async def test_cancelled_release_still_returns_its_active_slot(self):
        factory = FakeFactory()
        pool = GlobalMediaSessionPool(
            factory,
            MediaSessionPoolConfig(soft_sessions=1, max_sessions=1, pipeline_depth=1),
        )
        lease = await pool.acquire(4, "cancelled-release")

        await pool._lock.acquire()
        release_task = asyncio.create_task(lease.release())
        await asyncio.sleep(0)
        release_task.cancel()
        pool._lock.release()
        with self.assertRaises(asyncio.CancelledError):
            await release_task

        await asyncio.wait_for(pool.close(), 1)
        self.assertEqual(1, lease.session.stop_calls)


class KurigramMediaSessionFactoryTest(unittest.IsolatedAsyncioTestCase):
    async def test_media_wrapper_bounds_hidden_file_rpc_retries(self):
        raw_session = RecordingInvokeSession(5, "media")
        session = ReusableMediaSession(object(), raw_session)

        result = await session.invoke("get-file", sleep_threshold=30)

        self.assertEqual("response", result)
        self.assertEqual(
            [
                (
                    "get-file",
                    {
                        "sleep_threshold": 30,
                        "retries": 2,
                        "timeout": 10,
                    },
                )
            ],
            raw_session.invocations,
        )
        await session.stop()

    async def test_media_wrapper_preserves_explicit_rpc_retry_limits(self):
        raw_session = RecordingInvokeSession(5, "media")
        session = ReusableMediaSession(object(), raw_session)

        await session.invoke("get-file", retries=2, timeout=4)

        self.assertEqual(
            [("get-file", {"retries": 2, "timeout": 4})],
            raw_session.invocations,
        )
        await session.stop()

    async def test_persistent_cdn_view_uses_same_bounded_rpc_policy(self):
        client = CdnKurigramClient()
        client.cdn_session = RecordingInvokeSession(5, "cdn")
        session = ReusableMediaSession(client, client.media_session)
        cdn = await session.get_cdn_session(5, is_cdn=True, temporary=True)

        result = await cdn.invoke("get-cdn-file")

        self.assertEqual("response", result)
        self.assertEqual(
            [
                (
                    "get-cdn-file",
                    {"retries": 2, "timeout": 10},
                )
            ],
            client.cdn_session.invocations,
        )
        await session.stop()

    async def test_factory_session_reuses_cdn_until_media_session_stops(self):
        client = CdnKurigramClient()
        session = await KurigramMediaSessionFactory(client)(5)

        first = await session.get_cdn_session(
            5,
            is_cdn=True,
            temporary=True,
        )
        await first.stop()
        second = await session.get_cdn_session(
            5,
            is_cdn=True,
            temporary=True,
        )
        await second.stop()

        self.assertIs(first, second)
        self.assertEqual(1, client.cdn_calls)
        self.assertEqual(0, client.cdn_session.stop_calls)

        await session.stop()
        await session.stop()

        self.assertEqual(1, client.media_session.stop_calls)
        self.assertEqual(1, client.cdn_session.stop_calls)

    async def test_kurigram_factory_retries_auth_and_evicts_stale_cache(self):
        error = AuthBytesInvalid()
        client = FakeKurigramClient(error)
        factory = KurigramMediaSessionFactory(client, attempts=3)

        with self.assertRaises(AuthBytesInvalid):
            await factory(4)

        self.assertEqual(3, client.calls)
        self.assertNotIn(4, client.media_sessions)
        self.assertEqual(1, client.cached.stop_calls)

    async def test_kurigram_factory_retries_when_stale_session_stop_fails(self):
        error = AuthBytesInvalid()
        client = FakeKurigramClient(error)
        client.cached = FailingStopSession(4, "cached")
        client.media_sessions[4] = client.cached
        factory = KurigramMediaSessionFactory(client, attempts=3)

        with self.assertLogs("module.media_session_pool", level="ERROR"):
            with self.assertRaises(AuthBytesInvalid):
                await factory(4)

        self.assertEqual(3, client.calls)
        self.assertNotIn(4, client.media_sessions)
        self.assertEqual(1, client.cached.stop_calls)

    async def test_kurigram_factory_preserves_stale_stop_cancellation(self):
        client = FakeKurigramClient(AuthBytesInvalid())
        client.cached = CancellingStopSession(4, "cached")
        client.media_sessions[4] = client.cached
        factory = KurigramMediaSessionFactory(client, attempts=3)

        with self.assertRaises(asyncio.CancelledError):
            await factory(4)

        self.assertEqual(1, client.calls)
        self.assertEqual(1, client.cached.stop_calls)

    async def test_kurigram_factory_finishes_dc_cleanup_during_cancellation(self):
        client = FakeKurigramClient(AuthBytesInvalid())
        client.cached = BlockingStopSession(4, "cached-media")
        client.media_sessions = {4: client.cached}
        stale_dc_session = FakeSession(4, "cached-dc")
        client.sessions = {4: stale_dc_session}
        factory = KurigramMediaSessionFactory(client, attempts=3)

        factory_task = asyncio.create_task(factory(4))
        await asyncio.wait_for(client.cached.stop_started.wait(), 1)
        factory_task.cancel()
        await asyncio.sleep(0)

        self.assertFalse(factory_task.done())
        client.cached.allow_stop.set()

        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(factory_task, 1)

        self.assertTrue(client.cached.stop_completed.is_set())
        self.assertEqual(1, client.cached.stop_calls)
        self.assertEqual(1, stale_dc_session.stop_calls)

    async def test_kurigram_factory_serializes_shared_client_session_creation(self):
        client = SerialKurigramClient()
        factory = KurigramMediaSessionFactory(client)

        await asyncio.gather(factory(2), factory(5))

        self.assertEqual(1, client.max_active)

    async def test_kurigram_factory_reuses_existing_dc_authorization(self):
        client = SerialKurigramClient()
        factory = KurigramMediaSessionFactory(client)

        await factory(5)

        self.assertFalse(client.session_kwargs[0]["export_authorization"])

    async def test_kurigram_factory_evicts_failed_ordinary_dc_authorization(self):
        client = RecoveringAuthorizationKurigramClient()
        factory = KurigramMediaSessionFactory(client)

        session = await factory(5)

        self.assertIs(client.fresh_session, session.raw_session)
        self.assertNotIn(5, client.sessions)
        self.assertEqual(1, client.stale_dc_session.stop_calls)
        self.assertEqual(1, client.stale_media_session.stop_calls)
