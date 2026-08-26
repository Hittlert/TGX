import asyncio
import importlib
import importlib.util
import unittest
from dataclasses import dataclass

MODULE_NAME = "module.file_media_session_pool"


def window(**overrides):
    values = {
        "pending": 100,
        "utilization": 1.0,
        "retry_rate": 0.0,
        "unhealthy_fraction": 0.0,
        "flood_wait": False,
        "committed_bytes_per_second": 8 * 1024 * 1024,
        "stable_windows": 2,
    }
    values.update(overrides)
    module = importlib.import_module(MODULE_NAME)
    return module.FilePoolWindow(**values)


def controller_types(test_case):
    module = importlib.import_module(MODULE_NAME)
    for name in (
        "FilePoolConfig",
        "FilePoolWindow",
        "FileScaleDecision",
        "FilePoolController",
    ):
        test_case.assertTrue(
            hasattr(module, name),
            f"{MODULE_NAME} must expose {name}",
        )
    return (
        module.FilePoolConfig,
        module.FilePoolWindow,
        module.FileScaleDecision,
        module.FilePoolController,
    )


def coordinator_types(test_case):
    module = importlib.import_module(MODULE_NAME)
    for name in (
        "CoordinatorConfig",
        "CoordinatorSnapshot",
        "OwnedMediaSession",
        "MediaTransferCoordinator",
    ):
        test_case.assertTrue(
            hasattr(module, name),
            f"{MODULE_NAME} must expose {name}",
        )
    return (
        module.CoordinatorConfig,
        module.CoordinatorSnapshot,
        module.OwnedMediaSession,
        module.MediaTransferCoordinator,
    )


def file_pool_types(test_case):
    module = importlib.import_module(MODULE_NAME)
    for name in (
        "StripeAttemptError",
        "FilePoolSnapshot",
        "FileMediaSessionPool",
    ):
        test_case.assertTrue(
            hasattr(module, name),
            f"{MODULE_NAME} must expose {name}",
        )
    return (
        module.StripeAttemptError,
        module.FilePoolSnapshot,
        module.FileMediaSessionPool,
    )


async def wait_until(test_case, predicate, message):
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    test_case.fail(message)


@dataclass(eq=False)
class FakeStripe:
    stripe_id: int
    owner: str = "file-a"
    attempts: int = 0


class FakeSession:
    def __init__(self, dc_id, number):
        self.dc_id = dc_id
        self.number = number
        self.stop_calls = 0

    async def stop(self):
        self.stop_calls += 1


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
        raise RuntimeError("stop failed")


class CancellingStopSession(FakeSession):
    async def stop(self):
        self.stop_calls += 1
        raise asyncio.CancelledError()


class FakeFactory:
    def __init__(self, session_type=FakeSession):
        self.session_type = session_type
        self.sessions = []

    async def __call__(self, dc_id):
        session = self.session_type(dc_id, len(self.sessions))
        self.sessions.append(session)
        return session


class BlockingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def __call__(self, dc_id):
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return await super().__call__(dc_id)


class FirstReadyThenBlockingFactory(FakeFactory):
    def __init__(self):
        super().__init__()
        self.second_started = asyncio.Event()
        self.release_second = asyncio.Event()

    async def __call__(self, dc_id):
        if len(self.sessions) == 1:
            self.second_started.set()
            await self.release_second.wait()
        return await super().__call__(dc_id)


class PartiallyFailingFactory(FakeFactory):
    def __init__(self):
        super().__init__(BlockingStopSession)
        self.error = RuntimeError("factory failed")

    async def __call__(self, dc_id):
        if len(self.sessions) == 2:
            raise self.error
        return await super().__call__(dc_id)


class CancellationRaceFactory(FakeFactory):
    def __init__(self):
        super().__init__(BlockingStopSession)
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.physical_live = 0
        self.max_physical_live = 0

    async def __call__(self, dc_id):
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            await self.release.wait()
        session = await super().__call__(dc_id)
        original_stop = session.stop
        self.physical_live += 1
        self.max_physical_live = max(self.max_physical_live, self.physical_live)

        async def stop():
            try:
                await original_stop()
            finally:
                self.physical_live -= 1

        session.stop = stop
        return session


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class ControlledTick:
    def __init__(self):
        self.calls = []
        self.started = asyncio.Event()

    async def __call__(self, seconds):
        gate = asyncio.Event()
        self.calls.append((seconds, gate))
        self.started.set()
        await gate.wait()

    def release(self, seconds):
        for delay, gate in self.calls:
            if delay == seconds:
                gate.set()
                return
        raise AssertionError(f"no tick scheduled for {seconds} seconds")


class EarlyTick:
    def __init__(self):
        self.calls = []
        self.resleep_started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, seconds):
        self.calls.append(seconds)
        if len(self.calls) == 1:
            return
        self.resleep_started.set()
        await self.release.wait()


class FailingTick:
    async def __call__(self, _seconds):
        raise RuntimeError("tick failed")


class RecordingTick:
    def __init__(self):
        self.calls = []

    async def __call__(self, seconds):
        self.calls.append(seconds)


class ControlWindowTick:
    def __init__(self, interval=10):
        self.interval = interval
        self.calls = []
        self.window_calls = []
        self.window_started = asyncio.Event()

    async def __call__(self, seconds):
        self.calls.append(seconds)
        if seconds < self.interval:
            return
        gate = asyncio.Event()
        self.window_calls.append(gate)
        self.window_started.set()
        await gate.wait()

    def release_window(self, index=0):
        self.window_calls[index].set()


class SpinDetectTick:
    def __init__(self):
        self.calls = []

    async def __call__(self, seconds):
        self.calls.append(seconds)
        if len(self.calls) > 5:
            raise RuntimeError("control loop did not yield after an early tick")


class FileMediaSessionPoolAvailabilityTest(unittest.TestCase):
    def test_controller_module_is_available(self):
        self.assertIsNotNone(
            importlib.util.find_spec(MODULE_NAME),
            f"{MODULE_NAME} must exist before controller behavior is tested",
        )


class FilePoolControllerTest(unittest.TestCase):
    def test_exposes_immutable_default_configuration_and_window(self):
        FilePoolConfig, FilePoolWindow, FileScaleDecision, _ = controller_types(self)
        config = FilePoolConfig()
        sample = window()

        self.assertEqual(4, config.initial_sessions)
        self.assertEqual(12, config.max_sessions)
        self.assertEqual(10.0, config.control_interval)
        self.assertEqual(120.0, config.growth_hold)
        self.assertEqual(3, config.max_attempts)
        self.assertIsInstance(sample, FilePoolWindow)
        self.assertTrue(FilePoolConfig.__dataclass_params__.frozen)
        self.assertTrue(FilePoolWindow.__dataclass_params__.frozen)
        self.assertTrue(FileScaleDecision.__dataclass_params__.frozen)

    def test_rejects_invalid_configuration(self):
        FilePoolConfig, _, _, _ = controller_types(self)

        invalid = (
            {"initial_sessions": 8},
            {"max_sessions": 16},
            {"control_interval": 0},
            {"growth_hold": 0},
            {"max_attempts": 0},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    FilePoolConfig(**overrides)

    def test_max_sessions_four_reports_max_tier_without_evaluation(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig(max_sessions=4))

        first = controller.observe(window(), 10)
        second = controller.observe(
            window(committed_bytes_per_second=20 * 1024 * 1024),
            20,
        )

        self.assertEqual((4, "max_tier"), (first.target, first.reason))
        self.assertEqual((4, "max_tier"), (second.target, second.reason))

    def test_max_sessions_eight_evaluates_expansion_before_max_tier(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig(max_sessions=8))

        first = controller.observe(
            window(committed_bytes_per_second=8 * 1024 * 1024),
            10,
        )
        second = controller.observe(
            window(committed_bytes_per_second=8.4 * 1024 * 1024),
            20,
        )
        third = controller.observe(
            window(committed_bytes_per_second=8.8 * 1024 * 1024),
            30,
        )
        fourth = controller.observe(window(), 40)

        self.assertEqual((8, "expand"), (first.target, first.reason))
        self.assertEqual((8, "evaluating"), (second.target, second.reason))
        self.assertEqual((8, "goodput_growth"), (third.target, third.reason))
        self.assertEqual((8, "max_tier"), (fourth.target, fourth.reason))

    def test_tail_decision_is_bounded_by_zero_and_remaining_stripes(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())
        controller.observe(window(), 10)

        no_work = controller.observe(window(pending=0), 20)
        short_tail = controller.observe(window(pending=3), 30)

        self.assertEqual((0, "tail"), (no_work.target, no_work.reason))
        self.assertEqual((3, "tail"), (short_tail.target, short_tail.reason))

    def test_partial_tail_returns_pending_without_starting_expansion(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        partial = controller.observe(window(pending=6), 10)

        self.assertEqual((6, "tail"), (partial.target, partial.reason))
        self.assertEqual(4, controller.target)
        self.assertIsNone(controller._pre_growth_target)

    def test_equal_verified_target_is_tail_without_starting_expansion(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        equal_tail = controller.observe(window(pending=4), 10)
        self.assertEqual((4, "tail"), (equal_tail.target, equal_tail.reason))
        self.assertEqual(4, controller.target)
        self.assertIsNone(controller._pre_growth_target)
        self.assertEqual([], controller._evaluation)

        later_expansion = controller.observe(window(), 20)

        self.assertEqual(
            (8, "expand"), (later_expansion.target, later_expansion.reason)
        )

    def test_equal_proven_target_is_tail_without_starting_next_evaluation(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        controller.observe(window(committed_bytes_per_second=9 * 1024 * 1024), 20)
        controller.observe(
            window(committed_bytes_per_second=9 * 1024 * 1024),
            30,
        )

        equal_tail = controller.observe(window(pending=8), 40)
        later_expansion = controller.observe(window(), 50)

        self.assertEqual((8, "tail"), (equal_tail.target, equal_tail.reason))
        self.assertEqual(
            (12, "expand"), (later_expansion.target, later_expansion.reason)
        )
        self.assertEqual(12, controller.target)

    def test_proven_partial_tail_clamps_later_without_advancing_tier(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        controller.observe(window(committed_bytes_per_second=9 * 1024 * 1024), 20)
        controller.observe(
            window(committed_bytes_per_second=9 * 1024 * 1024),
            30,
        )

        partial = controller.observe(window(pending=10), 40)
        smaller = controller.observe(window(pending=3), 50)

        self.assertEqual((10, "tail"), (partial.target, partial.reason))
        self.assertEqual((3, "tail"), (smaller.target, smaller.reason))
        self.assertEqual(8, controller.target)
        self.assertIsNone(controller._pre_growth_target)

    def test_tail_restores_unproven_tier_and_clears_evaluation(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        initial_growth = controller.observe(window(), 10)
        first_evaluation = controller.observe(
            window(committed_bytes_per_second=8 * 1024 * 1024),
            20,
        )
        tail = controller.observe(window(pending=0), 20)
        new_expansion = controller.observe(
            window(committed_bytes_per_second=8 * 1024 * 1024),
            40,
        )
        fresh_evaluation = controller.observe(
            window(committed_bytes_per_second=9 * 1024 * 1024),
            50,
        )
        fresh_result = controller.observe(
            window(committed_bytes_per_second=9 * 1024 * 1024),
            60,
        )

        self.assertEqual((8, "expand"), (initial_growth.target, initial_growth.reason))
        self.assertEqual("evaluating", first_evaluation.reason)
        self.assertEqual(0, tail.target)
        self.assertEqual((8, "expand"), (new_expansion.target, new_expansion.reason))
        self.assertEqual("evaluating", fresh_evaluation.reason)
        self.assertEqual(
            (8, "goodput_growth"), (fresh_result.target, fresh_result.reason)
        )

    def test_tail_clamps_proven_tier_without_reverting_internal_target(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        controller.observe(window(committed_bytes_per_second=9 * 1024 * 1024), 20)
        growth = controller.observe(
            window(committed_bytes_per_second=9 * 1024 * 1024),
            30,
        )
        tail = controller.observe(window(pending=2), 40)
        next_observation = controller.observe(window(), 50)

        self.assertEqual((8, "goodput_growth"), (growth.target, growth.reason))
        self.assertEqual((2, "tail"), (tail.target, tail.reason))
        self.assertEqual(
            (12, "expand"), (next_observation.target, next_observation.reason)
        )
        self.assertEqual(12, controller.target)

    def test_zero_baseline_and_zero_post_growth_goodput_plateaus(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(committed_bytes_per_second=0), 10)
        controller.observe(window(committed_bytes_per_second=0), 20)
        decision = controller.observe(window(committed_bytes_per_second=0), 30)

        self.assertEqual((4, "plateau"), (decision.target, decision.reason))
        self.assertEqual(150.0, decision.hold_until)

    def test_zero_baseline_retains_growth_when_post_growth_goodput_is_positive(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(committed_bytes_per_second=0), 10)
        controller.observe(window(committed_bytes_per_second=0), 20)
        decision = controller.observe(window(committed_bytes_per_second=1), 30)

        self.assertEqual(8, decision.target)
        self.assertEqual("goodput_growth", decision.reason)

    def test_exact_five_percent_post_growth_average_retains_tier(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())
        baseline = 10 * 1024 * 1024
        threshold = 11 * 1024 * 1024 - 524288

        controller.observe(window(committed_bytes_per_second=baseline), 10)
        controller.observe(window(committed_bytes_per_second=threshold), 20)
        decision = controller.observe(
            window(committed_bytes_per_second=threshold),
            30,
        )

        self.assertEqual((8, "goodput_growth"), (decision.target, decision.reason))

    def test_just_below_five_percent_post_growth_average_rolls_back(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())
        baseline = 10 * 1024 * 1024
        below_threshold = 11 * 1024 * 1024 - 524289

        controller.observe(window(committed_bytes_per_second=baseline), 10)
        controller.observe(window(committed_bytes_per_second=below_threshold), 20)
        decision = controller.observe(
            window(committed_bytes_per_second=below_threshold),
            30,
        )

        self.assertEqual((4, "plateau"), (decision.target, decision.reason))
        self.assertEqual(150.0, decision.hold_until)

    def test_initial_target_and_growth_require_two_stable_windows(self):
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(
            importlib.import_module(MODULE_NAME).FilePoolConfig()
        )

        self.assertEqual(4, controller.target)
        self.assertEqual(4, controller.observe(window(stable_windows=1), 10).target)
        self.assertEqual("expand", controller.observe(window(), 20).reason)
        self.assertEqual(8, controller.target)

    def test_grows_four_eight_twelve_only_after_stable_windows(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        self.assertEqual(4, controller.target)
        self.assertEqual(8, controller.observe(window(), 10).target)
        self.assertEqual("evaluating", controller.observe(window(), 20).reason)
        self.assertEqual(
            8,
            controller.observe(
                window(committed_bytes_per_second=9 * 1024 * 1024),
                30,
            ).target,
        )
        self.assertEqual(12, controller.observe(window(), 40).target)

    def test_keeps_growth_after_two_post_growth_windows_with_five_percent_gain(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(committed_bytes_per_second=8 * 1024 * 1024), 10)
        self.assertEqual(
            "evaluating",
            controller.observe(
                window(committed_bytes_per_second=8.4 * 1024 * 1024),
                20,
            ).reason,
        )
        decision = controller.observe(
            window(committed_bytes_per_second=8.8 * 1024 * 1024),
            30,
        )

        self.assertEqual(8, decision.target)
        self.assertEqual("goodput_growth", decision.reason)

    def test_rolls_back_plateau_and_holds_growth_for_two_minutes(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        controller.observe(window(committed_bytes_per_second=8 * 1024 * 1024), 20)
        decision = controller.observe(
            window(committed_bytes_per_second=8 * 1024 * 1024),
            30,
        )

        self.assertEqual(4, decision.target)
        self.assertEqual("plateau", decision.reason)
        self.assertEqual(150.0, decision.hold_until)
        held = controller.observe(window(), 149.0)
        self.assertEqual(4, held.target)
        self.assertEqual("growth_hold", held.reason)
        self.assertEqual(8, controller.observe(window(), 150.0).target)

    def test_retry_or_unhealthy_conditions_contract_one_tier_and_hold_growth(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        decision = controller.observe(window(retry_rate=0.02), 20)
        self.assertEqual(4, decision.target)
        self.assertEqual("unhealthy", decision.reason)
        self.assertEqual(140.0, decision.hold_until)

        controller = FilePoolController(module.FilePoolConfig())
        controller.observe(window(), 10)
        decision = controller.observe(window(unhealthy_fraction=0.11), 20)
        self.assertEqual(4, decision.target)
        self.assertEqual("unhealthy", decision.reason)

    def test_flood_wait_freezes_growth(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        decision = controller.observe(window(flood_wait=True), 10)

        self.assertEqual(4, decision.target)
        self.assertEqual("flood_wait", decision.reason)
        self.assertEqual(0.0, decision.hold_until)

    def test_file_tail_stops_growth_evaluation(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, _, FilePoolController = controller_types(self)
        controller = FilePoolController(module.FilePoolConfig())

        controller.observe(window(), 10)
        decision = controller.observe(window(pending=0), 20)

        self.assertEqual(0, decision.target)
        self.assertEqual("tail", decision.reason)
        self.assertEqual(0.0, decision.hold_until)
        self.assertEqual(4, controller.target)


class MediaTransferCoordinatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_exposes_immutable_validated_configuration(self):
        CoordinatorConfig, CoordinatorSnapshot, _, _ = coordinator_types(self)

        config = CoordinatorConfig()

        self.assertEqual(60, config.max_sessions)
        self.assertEqual(10.0, config.expansion_interval)
        self.assertFalse(config.warm_session_handoff)
        self.assertEqual(20, config.warm_session_limit)
        self.assertEqual(120.0, config.warm_session_ttl)
        self.assertTrue(CoordinatorConfig.__dataclass_params__.frozen)
        self.assertTrue(CoordinatorSnapshot.__dataclass_params__.frozen)
        for overrides in (
            {"max_sessions": 0},
            {"max_sessions": 61},
            {"expansion_interval": 0},
            {"warm_session_limit": -1},
            {"warm_session_limit": 61},
            {"warm_session_ttl": 0},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(ValueError):
                    CoordinatorConfig(**overrides)

    async def test_successful_handoff_reuses_same_dc_sessions_without_reconnect(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(
                max_sessions=4,
                warm_session_handoff=True,
                warm_session_limit=4,
            ),
        )

        first = await coordinator.create_sessions("file-a", 4, 4)
        await asyncio.gather(*(owned.release() for owned in first))
        idle = coordinator.snapshot()

        self.assertEqual(4, idle.used)
        self.assertEqual(4, idle.live)
        self.assertEqual(4, idle.idle)
        self.assertEqual(4, idle.created)
        self.assertEqual(0, idle.reused)
        self.assertEqual(0, idle.active_files)
        self.assertEqual([0, 0, 0, 0], [item.stop_calls for item in factory.sessions])

        second = await coordinator.create_sessions("file-b", 4, 4)
        active = coordinator.snapshot()

        self.assertEqual(first, second)
        self.assertEqual(4, len(factory.sessions))
        self.assertEqual(0, active.idle)
        self.assertEqual(4, active.created)
        self.assertEqual(4, active.reused)
        self.assertEqual(1, active.active_files)
        await asyncio.gather(*(owned.release() for owned in second))
        await coordinator.close()
        self.assertEqual([1, 1, 1, 1], [item.stop_calls for item in factory.sessions])

    async def test_warm_handoff_never_assigns_session_across_datacenters(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(
                max_sessions=2,
                warm_session_handoff=True,
                warm_session_limit=1,
            ),
        )

        first = (await coordinator.create_sessions("file-a", 4, 1))[0]
        await first.release()
        second = (await coordinator.create_sessions("file-b", 5, 1))[0]

        self.assertIsNot(first, second)
        self.assertEqual([4, 5], [item.dc_id for item in factory.sessions])
        self.assertEqual(1, coordinator.snapshot().idle)
        await second.release()
        self.assertEqual(1, factory.sessions[1].stop_calls)
        await coordinator.close()

    async def test_partial_same_dc_handoff_only_creates_missing_sessions(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(
                max_sessions=4,
                warm_session_handoff=True,
                warm_session_limit=4,
            ),
        )

        first = await coordinator.create_sessions("file-a", 4, 2)
        await asyncio.gather(*(owned.release() for owned in first))
        second = await coordinator.create_sessions("file-b", 4, 4)

        self.assertEqual(4, len(factory.sessions))
        self.assertTrue(set(first).issubset(second))
        self.assertEqual(4, coordinator.snapshot().live)
        self.assertEqual(0, coordinator.snapshot().idle)
        await asyncio.gather(*(owned.release() for owned in second))
        await coordinator.close()

    async def test_idle_other_dc_is_evicted_when_it_blocks_hard_cap(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(
                max_sessions=1,
                warm_session_handoff=True,
                warm_session_limit=1,
            ),
        )

        first = (await coordinator.create_sessions("file-a", 4, 1))[0]
        await first.release()
        second = (await coordinator.create_sessions("file-b", 5, 1))[0]

        self.assertIsNot(first, second)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().idle)
        self.assertEqual(1, coordinator.snapshot().used)
        await second.release()
        await coordinator.close()

    async def test_expired_warm_session_is_stopped_instead_of_reused(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(
                max_sessions=1,
                warm_session_handoff=True,
                warm_session_limit=1,
                warm_session_ttl=10,
            ),
            clock=clock,
        )

        first = (await coordinator.create_sessions("file-a", 4, 1))[0]
        await first.release()
        clock.advance(11)
        second = (await coordinator.create_sessions("file-b", 4, 1))[0]

        self.assertIsNot(first, second)
        self.assertEqual(2, len(factory.sessions))
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await second.release()
        await coordinator.close()

    async def test_concurrent_creating_reservations_observe_one_hard_cap(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = BlockingFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=4),
        )

        creating = asyncio.create_task(coordinator.create_sessions("a", 4, 4))
        await factory.started.wait()
        snapshot = coordinator.snapshot()

        self.assertEqual(
            (4, 4, 0, 0),
            (
                snapshot.used,
                snapshot.creating,
                snapshot.live,
                snapshot.draining,
            ),
        )
        self.assertEqual([], await coordinator.create_sessions("b", 4, 1))
        factory.release.set()
        sessions = await creating
        self.assertEqual(4, len(sessions))
        await coordinator.close()

    async def test_group_reservations_are_all_or_none(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=3),
        )

        self.assertEqual([], await coordinator.create_sessions("a", 4, 4))
        self.assertEqual([], factory.sessions)
        self.assertEqual(0, coordinator.snapshot().used)
        with self.assertRaises(ValueError):
            await coordinator.create_sessions("a", 4, -1)
        self.assertEqual([], await coordinator.create_sessions("a", 4, 0))
        await coordinator.close()

    async def test_creating_live_and_draining_share_one_budget(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory(BlockingStopSession)
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=3),
        )
        owned = await coordinator.create_sessions("a", 4, 3)

        close_task = asyncio.create_task(owned[0].close())
        await factory.sessions[0].stop_started.wait()
        snapshot = coordinator.snapshot()

        self.assertEqual(
            (3, 0, 2, 1),
            (
                snapshot.used,
                snapshot.creating,
                snapshot.live,
                snapshot.draining,
            ),
        )
        self.assertEqual([], await coordinator.create_sessions("b", 4, 1))
        factory.sessions[0].allow_stop.set()
        await close_task
        self.assertEqual(2, coordinator.snapshot().used)
        for session in factory.sessions[1:]:
            session.allow_stop.set()
        await coordinator.close()

    async def test_only_one_expansion_is_granted_per_interval(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        clock = FakeClock()
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=8, expansion_interval=10),
            clock=clock,
        )

        initial = await coordinator.create_sessions("initial", 2, 1)
        first = await coordinator.create_sessions("a", 2, 2, expansion=True)
        rejected = await coordinator.create_sessions("b", 2, 2, expansion=True)
        clock.advance(10)
        second = await coordinator.create_sessions("b", 2, 2, expansion=True)

        self.assertEqual(1, len(initial))
        self.assertEqual(2, len(first))
        self.assertEqual([], rejected)
        self.assertEqual(2, len(second))
        await coordinator.close()

    async def test_partial_factory_failure_stops_created_group_before_release(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = PartiallyFailingFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=4),
        )

        creation = asyncio.create_task(coordinator.create_sessions("a", 4, 4))
        while len(factory.sessions) < 2:
            await asyncio.sleep(0)
        await asyncio.gather(
            *(session.stop_started.wait() for session in factory.sessions)
        )

        self.assertFalse(creation.done())
        snapshot = coordinator.snapshot()
        self.assertEqual(
            snapshot.used, snapshot.creating + snapshot.live + snapshot.draining
        )
        self.assertEqual(
            (2, 0, 0, 2),
            (
                snapshot.used,
                snapshot.creating,
                snapshot.live,
                snapshot.draining,
            ),
        )
        for session in factory.sessions:
            session.allow_stop.set()
        with self.assertRaisesRegex(RuntimeError, "factory failed"):
            await creation
        self.assertEqual(0, coordinator.snapshot().used)
        self.assertEqual([1, 1], [session.stop_calls for session in factory.sessions])
        await coordinator.close()

    async def test_creator_cancellation_promptly_cancels_cooperative_factory(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = BlockingFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        creation = asyncio.create_task(coordinator.create_sessions("a", 4, 1))
        await factory.started.wait()

        creation.cancel()
        try:
            for _ in range(10):
                await asyncio.sleep(0)
            self.assertTrue(factory.cancelled.is_set())

            result = await asyncio.gather(creation, return_exceptions=True)
            snapshot = coordinator.snapshot()
            self.assertIsInstance(result[0], asyncio.CancelledError)
            self.assertEqual(
                (0, 0, 0, 0),
                (
                    snapshot.used,
                    snapshot.creating,
                    snapshot.live,
                    snapshot.draining,
                ),
            )
        finally:
            if not creation.done():
                await coordinator.close()
                await asyncio.gather(creation, return_exceptions=True)

    async def test_factory_completion_at_cancel_handoff_cleans_result(self):
        module = importlib.import_module(MODULE_NAME)
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        original_shield = module.asyncio.shield
        handoff_cancelled = False

        async def cancel_after_factory_completion(task):
            nonlocal handoff_cancelled
            if not handoff_cancelled and task in coordinator._factory_tasks:
                handoff_cancelled = True
                await original_shield(task)
                current = asyncio.current_task()
                self.assertIsNotNone(current)
                current.cancel()
                await asyncio.sleep(0)
            return await original_shield(task)

        module.asyncio.shield = cancel_after_factory_completion
        try:
            creation = asyncio.create_task(coordinator.create_sessions("a", 4, 1))
            result = await asyncio.gather(creation, return_exceptions=True)
            snapshot = coordinator.snapshot()

            self.assertTrue(handoff_cancelled)
            self.assertIsInstance(result[0], asyncio.CancelledError)
            self.assertEqual(1, len(factory.sessions))
            self.assertEqual(1, factory.sessions[0].stop_calls)
            self.assertEqual(
                (0, 0, 0, 0),
                (
                    snapshot.used,
                    snapshot.creating,
                    snapshot.live,
                    snapshot.draining,
                ),
            )
        finally:
            module.asyncio.shield = original_shield
            await coordinator.close()

    async def test_factory_result_race_registers_and_closes_session_under_cap(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = CancellationRaceFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        creation = asyncio.create_task(coordinator.create_sessions("a", 4, 1))
        await factory.started.wait()

        creation.cancel()
        factory.release.set()
        for _ in range(10):
            await asyncio.sleep(0)

        self.assertEqual(1, len(factory.sessions))
        self.assertTrue(factory.sessions[0].stop_started.is_set())
        self.assertFalse(creation.done())
        self.assertEqual(
            (1, 0, 0, 1),
            (
                coordinator.snapshot().used,
                coordinator.snapshot().creating,
                coordinator.snapshot().live,
                coordinator.snapshot().draining,
            ),
        )
        self.assertEqual([], await coordinator.create_sessions("b", 4, 1))
        self.assertEqual(1, factory.max_physical_live)

        factory.sessions[0].allow_stop.set()
        result = await asyncio.gather(creation, return_exceptions=True)
        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, factory.physical_live)
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_cancellation_during_final_unregister_cleans_successful_group(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=2),
        )
        unregister_started = asyncio.Event()
        allow_unregister = asyncio.Event()
        original_unregister = coordinator._unregister_creator

        async def delayed_unregister(creator):
            unregister_started.set()
            await allow_unregister.wait()
            await original_unregister(creator)

        coordinator._unregister_creator = delayed_unregister
        creation = asyncio.create_task(coordinator.create_sessions("a", 4, 2))
        await unregister_started.wait()
        self.assertEqual(2, coordinator.snapshot().live)

        creation.cancel()
        allow_unregister.set()
        result = await asyncio.gather(creation, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual([1, 1], [session.stop_calls for session in factory.sessions])
        self.assertEqual(0, coordinator.snapshot().used)
        self.assertEqual(0, coordinator.snapshot().active_files)
        await coordinator.close()

    async def test_owned_close_is_idempotent_and_cancellation_shielded(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory(BlockingStopSession)
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        owned = (await coordinator.create_sessions("a", 4, 1))[0]
        close_task = asyncio.create_task(owned.close())
        await factory.sessions[0].stop_started.wait()

        close_task.cancel()
        await asyncio.sleep(0)
        close_task.cancel()
        factory.sessions[0].allow_stop.set()
        result = await asyncio.gather(close_task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertTrue(factory.sessions[0].stop_completed.is_set())
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)
        await owned.close()
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_owned_close_starts_cleanup_before_waiter_can_be_cancelled(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        owned = (await coordinator.create_sessions("a", 4, 1))[0]

        close_task = asyncio.create_task(owned.close())
        close_task.cancel()
        try:
            self.assertIsNotNone(owned._close_task)
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertEqual(1, factory.sessions[0].stop_calls)
            self.assertEqual(0, coordinator.snapshot().used)
        finally:
            await asyncio.gather(close_task, return_exceptions=True)
            await owned.close()
            await coordinator.close()

    async def test_owned_close_propagates_stop_failure_after_releasing_slot(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory(FailingStopSession)
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        owned = (await coordinator.create_sessions("a", 4, 1))[0]

        with self.assertRaisesRegex(RuntimeError, "stop failed"):
            await owned.close()

        self.assertEqual(0, coordinator.snapshot().used)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_dc_cooldowns_are_monotonic_isolated_and_cancellation_safe(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        clock = FakeClock()
        tick = ControlledTick()
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=1),
            clock=clock,
            tick=tick,
        )

        coordinator.pause_dc(4, 60)
        coordinator.pause_dc(4, 10)
        coordinator.pause_dc(5, 5)
        wait4 = asyncio.create_task(coordinator.wait_for_dc(4))
        wait5 = asyncio.create_task(coordinator.wait_for_dc(5))
        await tick.started.wait()
        await asyncio.sleep(0)
        first_wait4 = asyncio.create_task(coordinator.wait_for_dc(4))
        await asyncio.sleep(0)
        first_wait4.cancel()
        result = await asyncio.gather(first_wait4, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertTrue(coordinator.dc_is_paused(4))
        self.assertTrue(coordinator.dc_is_paused(5))
        self.assertEqual(60, coordinator.snapshot().dc_cooldowns[4])
        clock.advance(5)
        tick.release(5)
        await asyncio.wait_for(wait5, 1)
        self.assertFalse(coordinator.dc_is_paused(5))
        self.assertFalse(wait4.done())
        self.assertTrue(coordinator.dc_is_paused(4))
        clock.advance(55)
        tick.release(60)
        await asyncio.wait_for(wait4, 1)
        self.assertFalse(coordinator.dc_is_paused(4))
        await coordinator.close()

    async def test_early_cooldown_tick_resleeps_until_deadline(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        clock = FakeClock()
        tick = EarlyTick()
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=1),
            clock=clock,
            tick=tick,
        )
        coordinator.pause_dc(4, 10)
        waiter = asyncio.create_task(coordinator.wait_for_dc(4))
        try:
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertTrue(tick.resleep_started.is_set())
            self.assertEqual([10, 10], tick.calls)
            self.assertFalse(waiter.done())

            clock.advance(10)
            tick.release.set()
            await asyncio.wait_for(waiter, 1)
            self.assertFalse(coordinator.dc_is_paused(4))
        finally:
            if not waiter.done():
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
            await coordinator.close()

    async def test_failed_cooldown_tick_fails_open_and_wakes_waiters(self):
        module = importlib.import_module(MODULE_NAME)
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=1),
            tick=FailingTick(),
        )

        with self.assertLogs(module.__name__, level="ERROR") as logs:
            coordinator.pause_dc(4, 60)
            waiter = asyncio.create_task(coordinator.wait_for_dc(4))
            await asyncio.wait_for(waiter, 1)

        self.assertFalse(coordinator.dc_is_paused(4))
        self.assertTrue(any("cooldown wake failed" in line for line in logs.output))
        await coordinator.close()

    async def test_snapshot_is_immutable_defensive_and_tracks_active_pool_ids(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=3),
        )
        first = await coordinator.create_sessions("a", 4, 2)
        second = await coordinator.create_sessions("b", 5, 1)
        coordinator.record_committed("a", 1024)
        coordinator.update_pool({"pool_id": "a", "target": 4, "nested": {"live": 2}})

        snapshot = coordinator.snapshot()
        self.assertEqual(2, snapshot.active_files)
        self.assertEqual(
            snapshot.used, snapshot.creating + snapshot.live + snapshot.draining
        )
        self.assertEqual(1024, snapshot.pools["a"]["committed_bytes"])
        self.assertEqual(2, snapshot.pools["a"]["nested"]["live"])
        with self.assertRaises(TypeError):
            snapshot.pools["a"]["target"] = 8
        with self.assertRaises(TypeError):
            snapshot.pools["a"]["nested"]["live"] = 0
        self.assertIsNot(snapshot.pools, coordinator.snapshot().pools)
        self.assertIsNot(snapshot.pools["a"], coordinator.snapshot().pools["a"])
        await asyncio.gather(*(owned.close() for owned in first + second))
        self.assertEqual(0, coordinator.snapshot().active_files)
        await coordinator.close()

    async def test_committed_metrics_keep_raw_zeros_and_derive_five_second_stats(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        clock = FakeClock()
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=4),
            clock=clock,
        )
        mib = 1024 * 1024

        for _ in range(5):
            coordinator.record_committed("file-a", 8 * mib)
            clock.advance(1)
            coordinator._finalize_metric_buckets(clock())

        steady = coordinator.snapshot()
        self.assertEqual(8 * mib, steady.raw_bps)
        self.assertEqual(8 * mib, steady.rolling_5s_bps)
        self.assertEqual(8 * mib, steady.p10_5s_bps)
        self.assertEqual(0.0, steady.cv)
        self.assertEqual((8 * mib,) * 5, steady.raw_samples)
        self.assertEqual(8 * mib, steady.pools["file-a"]["rolling_5s_bps"])

        clock.advance(1)
        coordinator._finalize_metric_buckets(clock())
        stalled = coordinator.snapshot()

        self.assertEqual(0, stalled.raw_bps)
        self.assertEqual(6.4 * mib, stalled.rolling_5s_bps)
        self.assertEqual(0, stalled.raw_samples[-1])
        self.assertLess(stalled.p10_5s_bps, 8 * mib)
        self.assertGreater(stalled.cv, 0)
        self.assertEqual(6.4 * mib, stalled.pools["file-a"]["rolling_5s_bps"])
        await coordinator.close()

    async def test_metrics_loop_start_is_idempotent_and_close_stops_it(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        clock = FakeClock()
        tick = ControlledTick()
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=1),
            clock=clock,
            tick=tick,
        )

        coordinator.start()
        first_task = coordinator._metrics_task
        coordinator.start()
        await tick.started.wait()

        self.assertIs(first_task, coordinator._metrics_task)
        self.assertFalse(first_task.done())
        await coordinator.close()
        self.assertTrue(first_task.done())

    async def test_fallback_counter_is_exposed_in_snapshot(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        coordinator = MediaTransferCoordinator(
            FakeFactory(),
            CoordinatorConfig(max_sessions=1),
        )

        coordinator.record_fallback()
        coordinator.record_fallback()

        self.assertEqual(2, coordinator.snapshot().fallbacks)
        await coordinator.close()

    async def test_close_cancels_creation_rejects_new_work_and_is_idempotent(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = BlockingFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=2),
        )
        creation = asyncio.create_task(coordinator.create_sessions("a", 4, 2))
        await factory.started.wait()

        close_task = asyncio.create_task(coordinator.close())
        await factory.cancelled.wait()
        with self.assertRaises(RuntimeError):
            await coordinator.create_sessions("b", 4, 1)
        result = await asyncio.gather(creation, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        await close_task
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_close_is_repeated_cancellation_safe_and_stops_wrappers(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory(BlockingStopSession)
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        await coordinator.create_sessions("a", 4, 1)

        close_task = asyncio.create_task(coordinator.close())
        await factory.sessions[0].stop_started.wait()
        close_task.cancel()
        await asyncio.sleep(0)
        close_task.cancel()
        factory.sessions[0].allow_stop.set()
        result = await asyncio.gather(close_task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        await coordinator.close()
        self.assertTrue(factory.sessions[0].stop_completed.is_set())
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)

    async def test_coordinator_close_starts_before_waiter_can_be_cancelled(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory()
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        await coordinator.create_sessions("a", 4, 1)

        close_task = asyncio.create_task(coordinator.close())
        close_task.cancel()
        self.assertTrue(coordinator._closing)
        self.assertIsNotNone(coordinator._close_task)
        result = await asyncio.gather(close_task, return_exceptions=True)
        self.assertIsInstance(result[0], asyncio.CancelledError)
        await coordinator.close()
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)

    async def test_shutdown_propagates_cancelled_physical_stop_after_cleanup(self):
        CoordinatorConfig, _, _, MediaTransferCoordinator = coordinator_types(self)
        factory = FakeFactory(CancellingStopSession)
        coordinator = MediaTransferCoordinator(
            factory,
            CoordinatorConfig(max_sessions=1),
        )
        await coordinator.create_sessions("a", 4, 1)

        with self.assertRaises(asyncio.CancelledError):
            await coordinator.close()

        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)


class FileMediaSessionPoolTest(unittest.IsolatedAsyncioTestCase):
    async def test_first_ready_session_downloads_while_rest_of_batch_connects(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FirstReadyThenBlockingFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        first_download = asyncio.Event()
        release_downloads = asyncio.Event()

        async def download(_session, _stripe):
            first_download.set()
            await release_downloads.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(8)], download)
        )
        try:
            await asyncio.wait_for(factory.second_started.wait(), 1)
            await asyncio.wait_for(first_download.wait(), 1)

            self.assertFalse(run_task.done())
            self.assertEqual(1, pool.snapshot().live)
            self.assertEqual(1, pool.snapshot().active)
            self.assertEqual(1, coordinator.snapshot().live)
            self.assertEqual(3, coordinator.snapshot().creating)

            factory.release_second.set()
            release_downloads.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            factory.release_second.set()
            release_downloads.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

    async def test_later_factory_failure_closes_already_started_workers(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = PartiallyFailingFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        release_downloads = asyncio.Event()

        async def download(_session, _stripe):
            await release_downloads.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(8)], download)
        )
        try:
            await asyncio.gather(
                *(session.stop_started.wait() for session in factory.sessions)
            )
            await wait_until(
                self,
                lambda: len(factory.sessions) == 2
                and all(item.stop_started.is_set() for item in factory.sessions),
                "started workers were not closed after later factory failure",
            )
            self.assertFalse(run_task.done())

            for session in factory.sessions:
                session.allow_stop.set()
            with self.assertRaisesRegex(RuntimeError, "factory failed"):
                await asyncio.wait_for(run_task, 1)
            self.assertEqual(0, coordinator.snapshot().used)
            self.assertEqual([1, 1], [item.stop_calls for item in factory.sessions])
        finally:
            release_downloads.set()
            for session in factory.sessions:
                session.allow_stop.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

    async def test_one_session_never_executes_two_stripes_concurrently(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        active = {}
        max_active = {}

        async def download(session, _stripe):
            active[session.number] = active.get(session.number, 0) + 1
            max_active[session.number] = max(
                max_active.get(session.number, 0),
                active[session.number],
            )
            await asyncio.sleep(0)
            active[session.number] -= 1

        await pool.run([FakeStripe(index) for index in range(40)], download)

        self.assertEqual({0, 1, 2, 3}, set(max_active))
        self.assertEqual({1}, set(max_active.values()))
        await coordinator.close()

    async def test_tail_worker_closes_before_other_active_stripes_finish(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        release_others = asyncio.Event()
        all_started = asyncio.Event()
        started = set()

        async def download(session, _stripe):
            started.add(session.number)
            if len(started) == 4:
                all_started.set()
            await all_started.wait()
            if session.number == 0:
                await asyncio.sleep(0)
                return
            await release_others.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(5)], download)
        )
        try:
            await wait_until(
                self,
                lambda: len(factory.sessions) == 4
                and factory.sessions[0].stop_calls == 1
                and pool.snapshot().live == 3,
                "tail session close",
            )

            self.assertFalse(run_task.done())
            self.assertEqual(3, pool.snapshot().pending)
            self.assertEqual(3, pool.snapshot().live)
            self.assertEqual(3, pool.snapshot().target)

            release_others.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            release_others.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

    async def test_session_stop_failure_propagates_after_permit_release(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory(FailingStopSession)
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )

        async def download(_session, _stripe):
            return None

        raised = None
        try:
            await pool.run([FakeStripe(0)], download)
        except BaseException as error:
            raised = error

        self.assertIsInstance(raised, RuntimeError)
        self.assertEqual("stop failed", str(raised))
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_early_control_tick_does_not_starve_worker_cleanup(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        tick = SpinDetectTick()
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
            tick=tick,
        )

        async def download(_session, _stripe):
            return None

        raised = None
        try:
            await pool.run([FakeStripe(0)], download)
        except BaseException as error:
            raised = error

        self.assertIsNone(raised)
        self.assertLessEqual(len(tick.calls), 5)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_exposes_validated_attempt_error_and_frozen_snapshot(self):
        StripeAttemptError, FilePoolSnapshot, _ = file_pool_types(self)
        original = OSError("connection reset")

        failure = StripeAttemptError(
            original,
            "transport",
            wait_seconds=1.5,
            completed=True,
        )

        self.assertIs(original, failure.error)
        self.assertEqual("transport", failure.kind)
        self.assertEqual(1.5, failure.wait_seconds)
        self.assertTrue(failure.completed)
        self.assertTrue(FilePoolSnapshot.__dataclass_params__.frozen)
        for kind in ("retry", "", None):
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    StripeAttemptError(original, kind)
        with self.assertRaises(ValueError):
            StripeAttemptError(original, "flood_wait", wait_seconds=-0.1)

    async def test_worker_keeps_one_file_session_for_consecutive_stripes(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=8),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        calls = []

        async def download(session, stripe):
            calls.append((session.number, stripe.stripe_id, stripe.owner))
            await asyncio.sleep(0)

        await pool.run([FakeStripe(index) for index in range(12)], download)

        by_session = {}
        for session_id, stripe_id, owner in calls:
            by_session.setdefault(session_id, []).append(stripe_id)
            self.assertEqual("file-a", owner)
        self.assertEqual(4, len(factory.sessions))
        self.assertTrue(any(len(stripe_ids) > 1 for stripe_ids in by_session.values()))
        self.assertEqual(
            list(range(12)), sorted(stripe_id for _, stripe_id, _ in calls)
        )
        self.assertEqual(
            [1, 1, 1, 1], [session.stop_calls for session in factory.sessions]
        )
        self.assertEqual(0, coordinator.snapshot().used)

        probe = await coordinator.create_sessions("probe", 4, 1)
        self.assertEqual(1, len(probe))
        await probe[0].close()
        await coordinator.close()

    async def test_successive_file_pools_handoff_healthy_same_dc_workers(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(
                max_sessions=4,
                warm_session_handoff=True,
                warm_session_limit=4,
            ),
        )
        calls = []

        async def download(session, stripe):
            calls.append((stripe.owner, session.number, stripe.stripe_id))
            await asyncio.sleep(0)

        first = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        await first.run(
            [FakeStripe(index, owner="file-a") for index in range(8)],
            download,
        )
        self.assertEqual(4, len(factory.sessions))
        self.assertEqual(4, coordinator.snapshot().idle)
        self.assertEqual([0, 0, 0, 0], [item.stop_calls for item in factory.sessions])

        second = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-b",
            4,
        )
        await second.run(
            [FakeStripe(index, owner="file-b") for index in range(8)],
            download,
        )

        self.assertEqual(4, len(factory.sessions))
        self.assertEqual(4, coordinator.snapshot().idle)
        self.assertEqual({0, 1, 2, 3}, {number for _, number, _ in calls})
        self.assertEqual(
            {"file-a", "file-b"},
            {owner for owner, _, _ in calls},
        )
        await coordinator.close()
        self.assertEqual([1, 1, 1, 1], [item.stop_calls for item in factory.sessions])

    async def test_cancelled_file_pool_discards_session_instead_of_handoff(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(
                max_sessions=4,
                warm_session_handoff=True,
                warm_session_limit=4,
            ),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        started = asyncio.Event()
        blocker = asyncio.Event()

        async def download(_session, _stripe):
            started.set()
            await blocker.wait()

        run_task = asyncio.create_task(pool.run([FakeStripe(0)], download))
        await started.wait()
        run_task.cancel()
        result = await asyncio.gather(run_task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(0, coordinator.snapshot().idle)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_first_transport_failure_retries_same_stripe_on_same_session(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        tick = RecordingTick()
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
            tick=tick,
        )
        stripe = FakeStripe(0)
        calls = []
        original = ConnectionError("proxy reset")

        async def download(session, current):
            calls.append((session.number, current.stripe_id))
            if len(calls) == 1:
                raise StripeAttemptError(original, "transport")

        raised = None
        try:
            await pool.run([stripe], download)
        except BaseException as error:
            raised = error

        self.assertIsNone(raised)
        self.assertEqual([(0, 0), (0, 0)], calls)
        self.assertEqual(1, stripe.attempts)
        self.assertEqual(1.0, tick.calls[0])
        self.assertTrue(all(delay > 8 for delay in tick.calls[1:]))
        self.assertEqual(1, pool.snapshot().retries)
        self.assertEqual(0, pool.snapshot().resets)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_completed_attempt_error_counts_as_stripe_success(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        stripe = FakeStripe(0)

        async def download(_session, _stripe):
            raise StripeAttemptError(
                ConnectionError("reset after final commit"),
                "transport",
                completed=True,
            )

        raised = None
        try:
            await pool.run([stripe], download)
        except BaseException as error:
            raised = error

        self.assertIsNone(raised)
        self.assertEqual(0, stripe.attempts)
        self.assertEqual(0, pool.snapshot().retries)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        await coordinator.close()

    async def test_max_attempts_raises_original_error_after_cleanup(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4, max_attempts=2),
            "file-a",
            4,
            tick=RecordingTick(),
        )
        stripe = FakeStripe(0)
        original = ConnectionError("persistent reset")

        async def download(_session, _stripe):
            raise StripeAttemptError(original, "transport")

        raised = None
        try:
            await pool.run([stripe], download)
        except BaseException as error:
            raised = error

        self.assertIs(original, raised)
        self.assertEqual(2, stripe.attempts)
        self.assertEqual(2, pool.snapshot().retries)
        self.assertEqual(1, factory.sessions[0].stop_calls)
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_fatal_error_cancels_siblings_and_closes_every_session(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        all_started = asyncio.Event()
        blocker = asyncio.Event()
        started = set()
        cancelled = set()
        original = RuntimeError("file reference expired")

        async def download(_session, stripe):
            started.add(stripe.stripe_id)
            if len(started) == 4:
                all_started.set()
            await all_started.wait()
            if stripe.stripe_id == 0:
                raise StripeAttemptError(original, "fatal")
            try:
                await blocker.wait()
            except asyncio.CancelledError:
                cancelled.add(stripe.stripe_id)
                raise

        raised = None
        try:
            await pool.run([FakeStripe(index) for index in range(4)], download)
        except BaseException as error:
            raised = error

        self.assertIs(original, raised)
        self.assertEqual({1, 2, 3}, cancelled)
        self.assertEqual(
            [1, 1, 1, 1], [session.stop_calls for session in factory.sessions]
        )
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_fatal_error_cancels_sibling_already_draining_from_contraction(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        all_started = asyncio.Event()
        allow_fatal = asyncio.Event()
        release_draining = asyncio.Event()
        draining_cancelled = asyncio.Event()
        started = 0
        original = RuntimeError("file reference expired")

        async def download(session, _stripe):
            nonlocal started
            started += 1
            if started == 2:
                all_started.set()
            await all_started.wait()
            if session.number == 0:
                try:
                    await release_draining.wait()
                except asyncio.CancelledError:
                    draining_cancelled.set()
                    raise
                return
            await allow_fatal.wait()
            raise StripeAttemptError(original, "fatal")

        run_task = asyncio.create_task(
            pool.run([FakeStripe(0), FakeStripe(1)], download)
        )
        try:
            await all_started.wait()
            pool._mark_excess_draining(1)
            self.assertTrue(pool._workers[0].draining)
            allow_fatal.set()

            await wait_until(
                self,
                draining_cancelled.is_set,
                "fatal shutdown did not cancel draining sibling",
            )
            result = await asyncio.gather(run_task, return_exceptions=True)
            self.assertIs(original, result[0])
        finally:
            allow_fatal.set()
            release_draining.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

        self.assertEqual([1, 1], [session.stop_calls for session in factory.sessions])
        self.assertEqual(0, coordinator.snapshot().used)

    async def test_caller_cancellation_shields_all_session_cleanup(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory(BlockingStopSession)
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=60),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )
        all_started = asyncio.Event()
        started = 0
        blocker = asyncio.Event()

        async def download(_session, _stripe):
            nonlocal started
            started += 1
            if started == 4:
                all_started.set()
            await blocker.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(8)], download)
        )
        await all_started.wait()
        run_task.cancel()
        await asyncio.gather(
            *(session.stop_started.wait() for session in factory.sessions)
        )
        run_task.cancel()

        self.assertFalse(run_task.done())
        for session in factory.sessions:
            session.allow_stop.set()
        result = await asyncio.gather(run_task, return_exceptions=True)

        self.assertIsInstance(result[0], asyncio.CancelledError)
        self.assertEqual(
            [1, 1, 1, 1], [session.stop_calls for session in factory.sessions]
        )
        self.assertEqual(0, coordinator.snapshot().used)
        await coordinator.close()

    async def test_close_during_initial_creation_cancels_run_and_releases_reservations(
        self,
    ):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = BlockingFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )

        async def download(_session, _stripe):
            return None

        run_task = asyncio.create_task(pool.run([FakeStripe(0)], download))
        try:
            await factory.started.wait()
            await pool.close()
            await wait_until(
                self,
                run_task.done,
                "pool close did not cancel initial session creation",
            )
            result = await asyncio.gather(run_task, return_exceptions=True)
            self.assertIsInstance(result[0], asyncio.CancelledError)
            self.assertTrue(factory.cancelled.is_set())
            self.assertEqual(0, coordinator.snapshot().used)
        finally:
            factory.release.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

    async def test_run_rejects_pool_closed_before_start(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
        )

        async def download(_session, _stripe):
            return None

        await pool.close()
        try:
            with self.assertRaisesRegex(RuntimeError, "already closed"):
                await asyncio.wait_for(pool.run([FakeStripe(0)], download), 0.05)
        finally:
            await coordinator.close()

        self.assertEqual([], factory.sessions)

    async def test_repeated_cancellation_releases_sixty_owned_permits_once(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory(BlockingStopSession)
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=60),
        )
        pools = [
            FileMediaSessionPool(
                coordinator,
                module.FilePoolConfig(max_sessions=12),
                f"file-{index}",
                4,
            )
            for index in range(5)
        ]
        blocker = asyncio.Event()

        async def download(_session, _stripe):
            await blocker.wait()

        run_tasks = [
            asyncio.create_task(
                pool.run([FakeStripe(index) for index in range(20)], download)
            )
            for pool in pools
        ]
        try:
            await wait_until(self, lambda: len(factory.sessions) == 20, "initial 20")
            for pool in pools:
                await pool._start_workers(8, expansion=False)
                pool._target = 12
                pool._publish()
            await wait_until(self, lambda: len(factory.sessions) == 60, "all 60")

            snapshot = coordinator.snapshot()
            self.assertEqual(60, snapshot.used)
            self.assertEqual(60, snapshot.live)
            self.assertEqual(
                {12},
                {snapshot.pools[f"file-{index}"]["used"] for index in range(5)},
            )

            for task in run_tasks:
                task.cancel()
            await asyncio.gather(
                *(session.stop_started.wait() for session in factory.sessions)
            )
            for task in run_tasks:
                task.cancel()
            self.assertTrue(all(not task.done() for task in run_tasks))

            for session in factory.sessions:
                session.allow_stop.set()
            results = await asyncio.gather(*run_tasks, return_exceptions=True)

            self.assertTrue(
                all(isinstance(result, asyncio.CancelledError) for result in results)
            )
            self.assertEqual(
                [1] * 60, [session.stop_calls for session in factory.sessions]
            )
            self.assertEqual(0, coordinator.snapshot().used)
        finally:
            blocker.set()
            for session in factory.sessions:
                session.allow_stop.set()
            for task in run_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*run_tasks, return_exceptions=True)
            await coordinator.close()

    async def test_flood_wait_pauses_dc_before_retrying_unfinished_stripe(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        cooldown_tick = ControlledTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=8),
            clock=clock,
            tick=cooldown_tick,
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
            clock=clock,
            tick=RecordingTick(),
        )
        stripe = FakeStripe(0)
        calls = []

        async def download(session, current):
            calls.append((session.number, current.stripe_id))
            if len(calls) == 1:
                raise StripeAttemptError(
                    RuntimeError("FLOOD_WAIT_5"),
                    "flood_wait",
                    wait_seconds=5,
                )

        run_task = asyncio.create_task(pool.run([stripe], download))
        cooldown_started = asyncio.create_task(cooldown_tick.started.wait())
        try:
            await asyncio.wait(
                (run_task, cooldown_started),
                return_when=asyncio.FIRST_COMPLETED,
            )
            self.assertTrue(cooldown_tick.started.is_set())
            self.assertFalse(run_task.done())
            self.assertTrue(coordinator.dc_is_paused(4))
            self.assertEqual([(0, 0)], calls)
            self.assertEqual(1, len(factory.sessions))
            self.assertEqual(1, stripe.attempts)
            self.assertEqual(1, pool.snapshot().retries)
            self.assertEqual(0, pool.snapshot().active)
            self.assertEqual([stripe], list(pool._queue))

            clock.advance(5)
            cooldown_tick.release(5)
            await asyncio.wait_for(run_task, 1)
        finally:
            cooldown_started.cancel()
            await asyncio.gather(cooldown_started, return_exceptions=True)
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

        self.assertEqual([(0, 0), (0, 0)], calls)
        self.assertEqual(1, factory.sessions[0].stop_calls)

    async def test_second_transport_failure_retires_and_replaces_only_worker(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=8),
            clock=clock,
        )
        create_calls = []
        create_sessions = coordinator.create_sessions

        async def tracked_create(
            pool_id, dc_id, count, expansion=False, on_session=None
        ):
            create_calls.append((pool_id, dc_id, count, expansion))
            return await create_sessions(pool_id, dc_id, count, expansion, on_session)

        coordinator.create_sessions = tracked_create
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4, max_attempts=3),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        release_healthy = asyncio.Event()
        stripe_zero_sessions = []

        async def download(session, stripe):
            if stripe.stripe_id == 0:
                stripe_zero_sessions.append(session.number)
                if session.number == 0:
                    raise StripeAttemptError(
                        ConnectionError("worker reset"),
                        "transport",
                    )
                return
            await release_healthy.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(8)], download)
        )
        window_started = asyncio.create_task(tick.window_started.wait())
        try:
            await asyncio.wait(
                (run_task, window_started),
                return_when=asyncio.FIRST_COMPLETED,
            )
            self.assertTrue(tick.window_started.is_set())
            for _ in range(20):
                if factory.sessions and factory.sessions[0].stop_calls == 1:
                    break
                await asyncio.sleep(0)
            self.assertEqual(1, factory.sessions[0].stop_calls)
            self.assertEqual(4, len(factory.sessions))

            clock.advance(10)
            tick.release_window()
            for _ in range(20):
                if len(factory.sessions) == 5:
                    break
                await asyncio.sleep(0)
            self.assertEqual(5, len(factory.sessions))
            self.assertEqual(
                [("file-a", 4, 4, False), ("file-a", 4, 1, False)],
                create_calls,
            )
            for _ in range(20):
                if len(stripe_zero_sessions) == 3:
                    break
                await asyncio.sleep(0)
            self.assertEqual([0, 0], stripe_zero_sessions[:2])
            self.assertNotEqual(0, stripe_zero_sessions[2])

            release_healthy.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            window_started.cancel()
            await asyncio.gather(window_started, return_exceptions=True)
            release_healthy.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()
        self.assertEqual(2, pool.snapshot().retries)
        self.assertEqual(1, pool.snapshot().resets)
        self.assertEqual("unhealthy", pool.snapshot().last_scale_reason)
        self.assertEqual(
            [1, 1, 1, 1, 1], [session.stop_calls for session in factory.sessions]
        )

    async def test_unhealthy_contraction_replaces_deficit_at_new_tier(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=12, expansion_interval=10),
            clock=clock,
        )
        create_calls = []
        create_sessions = coordinator.create_sessions

        async def tracked_create(
            pool_id, dc_id, count, expansion=False, on_session=None
        ):
            create_calls.append((count, expansion))
            return await create_sessions(pool_id, dc_id, count, expansion, on_session)

        coordinator.create_sessions = tracked_create
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=8, max_attempts=3),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        begin = asyncio.Event()
        release_healthy = asyncio.Event()
        started = set()

        async def download(session, _stripe):
            started.add(session.number)
            await begin.wait()
            if session.number < 5:
                raise StripeAttemptError(ConnectionError("worker reset"), "transport")
            await release_healthy.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(20)], download)
        )
        try:
            await wait_until(self, lambda: len(pool._workers) == 4, "initial workers")
            await pool._start_workers(4, expansion=False)
            pool._target = 8
            pool._controller._target = 8
            pool._publish()
            await wait_until(self, lambda: len(started) == 8, "all workers started")
            begin.set()
            await wait_until(
                self,
                lambda: sum(session.stop_calls for session in factory.sessions[:5])
                == 5,
                "unhealthy workers stopped",
            )

            clock.advance(10)
            tick.release_window()
            await wait_until(
                self,
                lambda: len(factory.sessions) == 9,
                "contracted tier deficit was not replaced",
            )

            self.assertEqual(4, pool.snapshot().target)
            self.assertEqual(4, pool.snapshot().tier)
            self.assertEqual([(4, False), (4, False), (1, False)], create_calls)
            release_healthy.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            begin.set()
            release_healthy.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

        self.assertEqual([1] * 9, [session.stop_calls for session in factory.sessions])

    async def test_complete_windows_expand_four_to_eight_to_twelve(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=12, expansion_interval=10),
            clock=clock,
        )
        create_calls = []
        create_sessions = coordinator.create_sessions

        async def tracked_create(
            pool_id, dc_id, count, expansion=False, on_session=None
        ):
            create_calls.append((count, expansion))
            return await create_sessions(pool_id, dc_id, count, expansion, on_session)

        coordinator.create_sessions = tracked_create
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=12),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        release_work = asyncio.Event()

        async def download(_session, _stripe):
            await release_work.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(40)], download)
        )
        try:
            await wait_until(self, lambda: len(tick.window_calls) == 1, "first window")
            byte_counts = (100, 100, 110, 110, 110)
            for index, byte_count in enumerate(byte_counts):
                pool.record_committed(byte_count)
                clock.advance(10)
                tick.release_window(index)
                if index < len(byte_counts) - 1:
                    await wait_until(
                        self,
                        lambda index=index: len(tick.window_calls) > index + 1,
                        f"window {index + 2}",
                    )

            await wait_until(
                self,
                lambda: len(factory.sessions) == 12 and pool.snapshot().target == 12,
                "tier 12",
            )
            self.assertEqual([(4, False), (4, True), (4, True)], create_calls)
            self.assertEqual(12, pool.snapshot().target)
            self.assertEqual(12, pool.snapshot().tier)
            self.assertEqual(11.0, pool.snapshot().committed_bytes_per_second)
            self.assertEqual(
                530, coordinator.snapshot().pools["file-a"]["committed_bytes"]
            )

            release_work.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            release_work.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

        self.assertEqual([1] * 12, [session.stop_calls for session in factory.sessions])

    async def test_denied_growth_retries_later_with_fresh_evaluation_windows(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory()
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=12, expansion_interval=10),
            clock=clock,
        )
        create_calls = []
        deny_first_growth = True
        create_sessions = coordinator.create_sessions

        async def tracked_create(
            pool_id, dc_id, count, expansion=False, on_session=None
        ):
            nonlocal deny_first_growth
            create_calls.append((clock(), count, expansion))
            if expansion and deny_first_growth:
                deny_first_growth = False
                return []
            return await create_sessions(pool_id, dc_id, count, expansion, on_session)

        coordinator.create_sessions = tracked_create
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=12),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        release_work = asyncio.Event()

        async def download(_session, _stripe):
            await release_work.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(40)], download)
        )
        try:
            await wait_until(self, lambda: len(tick.window_calls) == 1, "first window")
            byte_counts = (100, 100, 110, 120, 120, 120)
            for index, byte_count in enumerate(byte_counts):
                pool.record_committed(byte_count)
                clock.advance(10)
                tick.release_window(index)
                if index < len(byte_counts) - 1:
                    await wait_until(
                        self,
                        lambda index=index: len(tick.window_calls) > index + 1,
                        f"window {index + 2}",
                    )
                if index == 1:
                    self.assertEqual(4, len(factory.sessions))
                    self.assertEqual("growth_denied", pool.snapshot().last_scale_reason)
                if index == 2:
                    await wait_until(self, lambda: len(factory.sessions) == 8, "tier 8")
                if index == 4:
                    self.assertEqual(8, len(factory.sessions))

            await wait_until(
                self,
                lambda: len(factory.sessions) == 12 and pool.snapshot().target == 12,
                "tier 12",
            )
            self.assertEqual(
                [
                    (0.0, 4, False),
                    (20.0, 4, True),
                    (30.0, 4, True),
                    (60.0, 4, True),
                ],
                create_calls,
            )
            release_work.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            release_work.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

    async def test_unhealthy_contraction_drains_only_excess_active_workers(self):
        module = importlib.import_module(MODULE_NAME)
        StripeAttemptError, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory(BlockingStopSession)
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=12, expansion_interval=10),
            clock=clock,
        )
        create_calls = []
        create_sessions = coordinator.create_sessions

        async def tracked_create(
            pool_id, dc_id, count, expansion=False, on_session=None
        ):
            create_calls.append((count, expansion))
            return await create_sessions(pool_id, dc_id, count, expansion, on_session)

        coordinator.create_sessions = tracked_create
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=12, max_attempts=3),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        release_work = asyncio.Event()
        calls_by_session = {}

        async def download(session, _stripe):
            calls_by_session[session.number] = (
                calls_by_session.get(session.number, 0) + 1
            )
            if session.number == 4:
                raise StripeAttemptError(ConnectionError("tier reset"), "transport")
            if calls_by_session[session.number] == 1:
                await release_work.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(index) for index in range(40)], download)
        )
        try:
            await wait_until(self, lambda: len(tick.window_calls) == 1, "first window")
            for index in range(2):
                pool.record_committed(100)
                clock.advance(10)
                tick.release_window(index)
                await wait_until(
                    self,
                    lambda index=index: len(tick.window_calls) > index + 1,
                    f"window {index + 2}",
                )

            await wait_until(self, lambda: len(factory.sessions) == 8, "tier 8")
            await factory.sessions[4].stop_started.wait()
            self.assertFalse(factory.sessions[4].stop_completed.is_set())
            self.assertEqual(1, pool.snapshot().resets)

            clock.advance(10)
            tick.release_window(2)
            await wait_until(
                self,
                lambda: pool.snapshot().last_scale_reason == "unhealthy",
                "unhealthy contraction",
            )
            contracted = pool.snapshot()
            self.assertEqual(4, contracted.target)
            self.assertEqual(4, contracted.tier)
            self.assertEqual(4, contracted.draining)
            self.assertEqual([(4, False), (4, True)], create_calls)

            release_work.set()
            for session in factory.sessions:
                session.allow_stop.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            release_work.set()
            for session in factory.sessions:
                session.allow_stop.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()

        self.assertEqual(1, calls_by_session[0])
        self.assertEqual(1, calls_by_session[1])
        self.assertEqual(1, calls_by_session[2])
        self.assertGreater(calls_by_session[3], 1)
        self.assertEqual([1] * 8, [session.stop_calls for session in factory.sessions])

    async def test_window_integrates_active_live_utilization_and_tail_draining(self):
        module = importlib.import_module(MODULE_NAME)
        _, _, FileMediaSessionPool = file_pool_types(self)
        factory = FakeFactory(BlockingStopSession)
        clock = FakeClock()
        tick = ControlWindowTick()
        coordinator = module.MediaTransferCoordinator(
            factory,
            module.CoordinatorConfig(max_sessions=4),
            clock=clock,
        )
        pool = FileMediaSessionPool(
            coordinator,
            module.FilePoolConfig(max_sessions=4),
            "file-a",
            4,
            clock=clock,
            tick=tick,
        )
        observed = []
        observe = pool._controller.observe

        def track_window(sample, now):
            observed.append((sample, now))
            return observe(sample, now)

        pool._controller.observe = track_window
        release_first = asyncio.Event()
        release_second = asyncio.Event()

        async def download(session, _stripe):
            if session.number == 0:
                await release_first.wait()
            else:
                await release_second.wait()

        run_task = asyncio.create_task(
            pool.run([FakeStripe(0), FakeStripe(1)], download)
        )
        try:
            await wait_until(self, lambda: len(tick.window_calls) == 1, "first window")
            clock.advance(5)
            release_first.set()
            await factory.sessions[0].stop_started.wait()

            self.assertEqual(1, pool.snapshot().draining)
            self.assertEqual(1, pool.snapshot().pending)

            clock.advance(5)
            tick.release_window()
            await wait_until(self, lambda: len(observed) == 1, "window observation")

            sample, observed_at = observed[0]
            self.assertEqual(10.0, observed_at)
            self.assertEqual(0.75, sample.utilization)
            self.assertEqual(1, sample.pending)

            release_second.set()
            for session in factory.sessions:
                session.allow_stop.set()
            await asyncio.wait_for(run_task, 1)
        finally:
            release_first.set()
            release_second.set()
            for session in factory.sessions:
                session.allow_stop.set()
            if not run_task.done():
                run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await coordinator.close()


if __name__ == "__main__":
    unittest.main()
