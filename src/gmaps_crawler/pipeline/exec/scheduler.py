"""
Progressive task scheduler
Start workers in small batches with delays to avoid resource spikes.
"""

from __future__ import annotations

import time
from logger import crawler_thread_logger as logger
import threading
from typing import List, Callable, Any, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue, Empty

from gmaps_crawler.config import settings
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT


# use named crawler logger


class ProgressiveTaskScheduler:
    """Progressively start tasks with configurable pacing.

    Features:
    1. Start threads in batches instead of all at once
    2. Configurable startup delay and batch size
    3. Support dynamic task submission
    4. Graceful stop via STOP_EVENT
    """

    def __init__(
        self,
        max_workers: int,
        startup_delay: Optional[float] = None,
        batch_size: Optional[int] = None,
        batch_delay: Optional[float] = None,
    ):
        self.max_workers = max_workers
        self.startup_delay = startup_delay or settings.THREAD_STARTUP_DELAY
        self.batch_size = batch_size or settings.THREAD_BATCH_SIZE
        self.batch_delay = batch_delay or settings.THREAD_BATCH_DELAY

        self._task_queue: Queue = Queue()
        self._active_futures: Dict[Future, Any] = {}
        self._executor: Optional[ThreadPoolExecutor] = None
        self._scheduler_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        # logger.info(
        #     "Initialize progressive scheduler: max_workers=%d, batch_size=%d, batch_delay=%.1fs",
        #     max_workers, self.batch_size, self.batch_delay,
        # )

    def start(self):
        """Start scheduler."""
        with self._lock:
            if self._running:
                return

            self._running = True
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                name="ProgressiveScheduler",
                daemon=True,
            )
            self._scheduler_thread.start()

    def stop(self, wait: bool = True):
        """Stop scheduler."""
        with self._lock:
            if not self._running:
                return

            self._running = False

        if self._scheduler_thread and wait:
            self._scheduler_thread.join(timeout=5.0)

        if self._executor:
            self._executor.shutdown(wait=wait)

        logger.info("Progressive scheduler stopped")

    def submit_task(self, func: Callable, *args, **kwargs) -> None:
        """Submit a single task to the queue."""
        if not self._running:
            raise RuntimeError("Scheduler not started")

        task = (func, args, kwargs)
        self._task_queue.put(task)

    def submit_tasks(self, tasks: List[tuple]) -> None:
        """Submit a batch of tasks.

        Each item is (func, args) or (func, args, kwargs).
        """
        for task in tasks:
            if len(task) == 1:
                func = task[0]
                args, kwargs = (), {}
            elif len(task) == 2:
                func, args = task
                kwargs = {}
            else:
                func, args, kwargs = task

            self.submit_task(func, *args, **kwargs)

    def get_results(self):
        """Yield results of finished tasks and remove them from active list."""
        completed_futures: List[Future] = []

        with self._lock:
            for future in list(self._active_futures.keys()):
                if future.done():
                    completed_futures.append(future)
                    del self._active_futures[future]

        for future in completed_futures:
            try:
                yield future.result()
            except Exception as e:
                logger.warning("Task failed: %s", e)
                yield None

    def wait_for_completion(self, timeout: Optional[float] = None):
        """Block until all tasks finished or timeout/STOP_EVENT triggered."""
        start_time = time.time()

        while True:
            if STOP_EVENT.is_set():
                break

            with self._lock:
                has_pending = not self._task_queue.empty()
                has_active = len(self._active_futures) > 0

            if not has_pending and not has_active:
                break

            if timeout and (time.time() - start_time) > timeout:
                logger.warning("Wait for completion timed out")
                break

            # Drain any finished results to keep active set small
            list(self.get_results())
            time.sleep(0.1)

        logger.info("All tasks completed or stopped")

    def _scheduler_loop(self):
        """Background loop to start queued tasks progressively."""
        logger.info("Scheduler main loop start")

        while self._running and not STOP_EVENT.is_set():
            try:
                with self._lock:
                    active_count = len(self._active_futures)

                if active_count < self.max_workers:
                    available_slots = self.max_workers - active_count
                    batch_size = min(self.batch_size, available_slots)

                    started_count = self._start_task_batch(batch_size)

                    if started_count > 0:
                        logger.debug("Started %d new task(s), active=%d", started_count, active_count + started_count)

                        # If a full batch was started, wait batch delay
                        if started_count == batch_size:
                            time.sleep(self.batch_delay)
                        continue

                # Cleanup completed
                self._cleanup_completed_tasks()

                time.sleep(0.1)

            except Exception as e:
                logger.error("Scheduler loop error: %s", e)
                time.sleep(1.0)

        logger.info("Scheduler main loop end")

    def _start_task_batch(self, batch_size: int) -> int:
        """Start up to batch_size tasks from the queue."""
        started = 0

        for _ in range(batch_size):
            try:
                task = self._task_queue.get_nowait()
                func, args, kwargs = task

                future = self._executor.submit(func, *args, **kwargs)

                with self._lock:
                    self._active_futures[future] = task

                started += 1

                if started < batch_size and self.startup_delay > 0:
                    time.sleep(self.startup_delay)

            except Empty:
                break
            except Exception as e:
                # Submit失败：同步降级执行以保证每个出队任务都产生一个结果
                logger.error("Failed to start task: %s", e)
                try:
                    # 同步调用任务函数，获取与正常路径一致的结果结构
                    result = func(*args, **kwargs)
                    # 包装为已完成的 Future 纳入结果收集
                    fut = Future()
                    fut.set_result(result)
                    with self._lock:
                        self._active_futures[fut] = task
                    started += 1
                except Exception as e2:
                    # 同步执行也失败，仅记录错误；为避免 writer 端协议破坏，不塞入无效结果
                    logger.error("Fallback execute task failed: %s", e2)

        return started

    def _cleanup_completed_tasks(self):
        """Evict completed futures from active set; no-op hook for extra processing."""
        completed_futures: List[Future] = []

        with self._lock:
            for future in list(self._active_futures.keys()):
                if future.done():
                    completed_futures.append(future)
                    del self._active_futures[future]

        for future in completed_futures:
            try:
                _ = future.result()
            except Exception as e:
                logger.warning("Task execution exception: %s", e)

    @property
    def active_task_count(self) -> int:
        with self._lock:
            return len(self._active_futures)

    @property
    def pending_task_count(self) -> int:
        return self._task_queue.qsize()


class ProgressiveTaskManager:
    """High-level wrapper to run tasks with the scheduler and collect results."""

    def __init__(self, scheduler: ProgressiveTaskScheduler):
        self.scheduler = scheduler
        self.results: List[Any] = []
        self.errors: List[Exception] = []
        self._result_lock = threading.Lock()

    def execute_tasks(self, tasks: List[tuple], timeout: Optional[float] = None) -> List[Any]:
        """Execute tasks and return collected results."""
        self.results.clear()
        self.errors.clear()

        self.scheduler.start()

        try:
            self.scheduler.submit_tasks(tasks)

            self._collect_results_async()
            self.scheduler.wait_for_completion(timeout)

            self._collect_remaining_results()

        finally:
            self.scheduler.stop()

        logger.info("Tasks finished: success=%d, failures=%d", len(self.results), len(self.errors))

        return self.results

    def _collect_results_async(self):
        """Background collector that appends results as they become available."""

        def collect_loop():
            while self.scheduler._running and not STOP_EVENT.is_set():
                for result in self.scheduler.get_results():
                    with self._result_lock:
                        if result is not None:
                            self.results.append(result)
                        else:
                            self.errors.append(Exception("task returned None"))
                time.sleep(0.1)

        collector_thread = threading.Thread(
            target=collect_loop,
            name="ResultCollector",
            daemon=True,
        )
        collector_thread.start()

    def _collect_remaining_results(self):
        for result in self.scheduler.get_results():
            with self._result_lock:
                if result is not None:
                    self.results.append(result)
                else:
                    self.errors.append(Exception("task returned None"))

