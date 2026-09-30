"""Discovery of the daily lab-result files dropped by lab_results_simulator.py.

File name contract (set by the simulator):
    lab_results_day{N}_{unix_epoch}.csv

`N` restarts at 1 whenever the simulator restarts, so a file is identified by
its full name (not by N). The epoch is the moment the simulated day closed and
the lab "uploaded" its end-of-day extract; the batch layer therefore pairs a
file with the vitals of the simulated day that ended at that instant:
    [epoch - SIMULATED_DAY_SECONDS, epoch)
"""
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Set

LAB_FILE_RE = re.compile(r"^lab_results_day(\d+)_(\d+)\.csv$")


@dataclass(frozen=True)
class LabFile:
    path: Path
    day: int
    dropped_at_epoch: int

    @property
    def name(self) -> str:
        return self.path.name

    def vitals_window(self, simulated_day_seconds: int):
        end = datetime.fromtimestamp(self.dropped_at_epoch, tz=timezone.utc)
        return end - timedelta(seconds=simulated_day_seconds), end

    def as_dict(self) -> dict:
        return {"path": str(self.path), "name": self.name, "day": self.day,
                "dropped_at_epoch": self.dropped_at_epoch}


def parse_lab_file(path) -> Optional[LabFile]:
    path = Path(path)
    match = LAB_FILE_RE.match(path.name)
    if not match:
        return None
    return LabFile(path=path, day=int(match.group(1)), dropped_at_epoch=int(match.group(2)))


def list_lab_files(directory) -> List[LabFile]:
    directory = Path(directory)
    if not directory.is_dir():
        return []
    files = (parse_lab_file(p) for p in directory.iterdir() if p.is_file())
    return sorted((f for f in files if f), key=lambda f: (f.dropped_at_epoch, f.day))


def pending_lab_files(files: Iterable[LabFile], processed_names: Set[str], *,
                      grace_seconds: int, max_age_seconds: int,
                      limit: int, now: float = None) -> List[LabFile]:
    """Oldest-first files that still need a batch run.

    grace_seconds  - skip files younger than this: lets in-flight vitals land
                     in the master dataset and avoids reading half-written CSVs.
    max_age_seconds- ignore very old drops (e.g. leftovers from earlier test
                     sessions) instead of back-filling them forever.
    """
    now = time.time() if now is None else now
    pending = [
        f for f in files
        if f.name not in processed_names
        and now - f.dropped_at_epoch >= grace_seconds
        and now - f.dropped_at_epoch <= max_age_seconds
    ]
    return pending[:limit]


def ready_for_batch(pending: List[LabFile], latest_event_epoch: Optional[float], *,
                    grace_seconds: int, max_wait_seconds: int, now: float = None) -> List[LabFile]:
    """Completeness gate: the oldest-first prefix of `pending` that can run now.

    A file is ready when the master dataset already holds readings at/after
    the end of its vitals window (the speed layer has caught up, e.g. after a
    restart it is still replaying Kafka), or when it has waited max_wait_seconds
    beyond the grace period (producer down -> run anyway, flagged incomplete).
    Stops at the first not-ready file so days are always processed in order.
    """
    now = time.time() if now is None else now
    ready = []
    for f in pending:
        caught_up = latest_event_epoch is not None and latest_event_epoch >= f.dropped_at_epoch
        waited_out = now - f.dropped_at_epoch >= grace_seconds + max_wait_seconds
        if not (caught_up or waited_out):
            break
        ready.append(f)
    return ready


def fetch_latest_event_epoch(cur) -> Optional[float]:
    cur.execute("SELECT EXTRACT(EPOCH FROM max(event_time))::float AS latest FROM vitals_readings")
    row = cur.fetchone()
    return row["latest"] if isinstance(row, dict) else row[0]


def fetch_processed_file_names(cur) -> Set[str]:
    cur.execute("SELECT DISTINCT source_file FROM batch_runs WHERE status = 'success'")
    return {row["source_file"] if isinstance(row, dict) else row[0] for row in cur.fetchall()}
