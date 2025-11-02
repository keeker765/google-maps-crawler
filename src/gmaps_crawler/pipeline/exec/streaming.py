from __future__ import annotations

import time
from typing import Callable, Dict, List

from gmaps_crawler.pipeline.exec.scheduler import ProgressiveTaskScheduler
from gmaps_crawler.pipeline.exec.stop import STOP_EVENT
from logger import main_thread_logger as logger


def run_streaming(scheduler: ProgressiveTaskScheduler, 
                  tasks: List[Dict], 
                  worker: Callable[[Dict], Dict], 
                  writer) -> int:
    scheduler.start()
    scheduler.submit_tasks([(worker, (info,)) for info in tasks])
    logger.info("Submit %d tasks.", len(tasks))
    thread_done = 0
    total = len(tasks)
    while not STOP_EVENT.is_set():
        made = False
        for res in scheduler.get_results():
            thread_done += 1
            # per-task completion log
            try:
                status = str(res.get("status") or "")
                payload = res.get("payload") or {}
                pid = str(payload.get("place_id") or "")
                tile_idx = getattr(getattr(writer, 'tile_ctx', None), 'index', None)
                if tile_idx is None:
                    logger.info("[task][done] %d/%d status=%s pid=%s", thread_done, total, status, pid)
                else:
                    logger.info("[task][done] tile=%s %d/%d status=%s pid=%s", tile_idx, thread_done, total, status, pid)
            except Exception as e:
                logger.debug("task-done log error: %s", e)
            writer.feed(res)
            made = True
        if scheduler.pending_task_count == 0 and scheduler.active_task_count == 0:
            if not made:
                break
        if not made:
            time.sleep(0.1)
    scheduler.stop()
    for res in scheduler.get_results():
        thread_done += 1
        try:
            status = str(res.get("status") or "")
            payload = res.get("payload") or {}
            pid = str(payload.get("place_id") or "")
            tile_idx = getattr(getattr(writer, 'tile_ctx', None), 'index', None)
            if tile_idx is None:
                logger.info("[task][done] %d/%d status=%s pid=%s", thread_done, total, status, pid)
            else:
                logger.info("[task][done] tile=%s %d/%d status=%s pid=%s", tile_idx, thread_done, total, status, pid)
        except Exception as e:
            logger.debug("task-done log error: %s", e)
        writer.feed(res)
    return thread_done

