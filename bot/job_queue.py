# bot/job_queue.py
import asyncio
import itertools
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable, Optional

JobPayload = dict[str, Any]
JobFunc = Callable[[JobPayload], Awaitable[None]]

@dataclass
class Job:
    id: int
    payload: JobPayload
    status: str = "queued"   # queued|running|done|failed|cancelled
    error: Optional[str] = None

@dataclass
class _QueueState:
    queue: "asyncio.Queue[Job]" = field(default_factory=asyncio.Queue)
    jobs: dict[int, Job] = field(default_factory=dict)
    seq: itertools.count = field(default_factory=lambda: itertools.count(1))
    workers: list[asyncio.Task] = field(default_factory=list)
    running: bool = False
    on_job: Optional[JobFunc] = None
    concurrency: int = 2

_state = _QueueState()

def configure(on_job: JobFunc, *, concurrency: int = 2) -> None:
    """Указать обработчик задач и количество воркеров (вызывается при старте бота)."""
    _state.on_job = on_job
    _state.concurrency = max(1, int(concurrency))

def submit(payload: JobPayload) -> Job:
    """Положить задачу в очередь и вернуть объект Job (с id)."""
    jid = next(_state.seq)
    job = Job(id=jid, payload=payload)
    _state.jobs[jid] = job
    _state.queue.put_nowait(job)
    return job

def get(job_id: int) -> Optional[Job]:
    return _state.jobs.get(job_id)

async def _worker(idx: int):
    while True:
        job = await _state.queue.get()
        if job is None:  # сигнал остановки
            _state.queue.task_done()
            break
        if _state.on_job is None:
            job.status = "failed"
            job.error = "No on_job handler configured"
            _state.queue.task_done()
            continue
        try:
            job.status = "running"
            await _state.on_job(job.payload)
            job.status = "done"
        except asyncio.CancelledError:
            job.status = "cancelled"
            raise
        except Exception as e:
            job.status = "failed"
            job.error = str(e)
        finally:
            _state.queue.task_done()

async def start():
    """Запустить воркеры (вызвать один раз при старте приложения)."""
    if _state.running:
        return
    _state.running = True
    _state.workers = [asyncio.create_task(_worker(i)) for i in range(_state.concurrency)]

async def stop():
    """Аккуратная остановка (например, в on_shutdown)."""
    if not _state.running:
        return
    for _ in _state.workers:
        await _state.queue.put(None)
    await _state.queue.join()
    for t in _state.workers:
        t.cancel()
    _state.workers.clear()
    _state.running = False
