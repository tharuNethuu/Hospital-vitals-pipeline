from datetime import datetime, timezone

from processing.lab_files import list_lab_files, parse_lab_file, pending_lab_files


def test_parse_lab_file_name():
    f = parse_lab_file("lab_results_drops/lab_results_day3_1789136839.csv")
    assert (f.day, f.dropped_at_epoch, f.name) == (3, 1789136839, "lab_results_day3_1789136839.csv")
    assert parse_lab_file("notes.txt") is None
    assert parse_lab_file("lab_results_dayX_1.csv") is None


def test_vitals_window_is_the_simulated_day_that_just_closed():
    start, end = parse_lab_file("lab_results_day1_1000.csv").vitals_window(120)
    assert end == datetime.fromtimestamp(1000, tz=timezone.utc)
    assert (end - start).total_seconds() == 120


def test_list_lab_files_sorted_and_filtered(tmp_path):
    for name in ["lab_results_day2_200.csv", "lab_results_day1_100.csv", "other.csv"]:
        (tmp_path / name).write_text("x")
    assert [f.name for f in list_lab_files(tmp_path)] == [
        "lab_results_day1_100.csv", "lab_results_day2_200.csv"]
    assert list_lab_files(tmp_path / "missing") == []


def test_pending_respects_processed_grace_age_and_limit():
    files = [parse_lab_file(f"lab_results_day{i}_{t}.csv")
             for i, t in enumerate([100, 900, 950, 990, 998], start=1)]
    pending = pending_lab_files(files, {"lab_results_day2_900.csv"}, grace_seconds=5,
                                max_age_seconds=500, limit=10, now=1000)
    # day1 too old, day2 processed, day5 still inside the grace period
    assert [f.day for f in pending] == [3, 4]
    assert len(pending_lab_files(files, set(), grace_seconds=0, max_age_seconds=10_000,
                                 limit=2, now=1000)) == 2


def test_restarted_simulator_day_numbers_do_not_collide():
    files = [parse_lab_file("lab_results_day1_100.csv"), parse_lab_file("lab_results_day1_500.csv")]
    pending = pending_lab_files(files, {"lab_results_day1_100.csv"}, grace_seconds=0,
                                max_age_seconds=10_000, limit=10, now=600)
    assert [f.name for f in pending] == ["lab_results_day1_500.csv"]


def test_completeness_gate_waits_for_master_dataset():
    from processing.lab_files import ready_for_batch

    files = [parse_lab_file("lab_results_day1_1000.csv"), parse_lab_file("lab_results_day2_1120.csv")]
    kw = dict(grace_seconds=10, max_wait_seconds=120)
    # stream only reached t=1050: day1 ready, day2 not (and nothing jumps the queue)
    assert [f.day for f in ready_for_batch(files, 1050, now=1140, **kw)] == [1]
    # stream fully caught up
    assert [f.day for f in ready_for_batch(files, 1125, now=1140, **kw)] == [1, 2]
    # no data at all: wait, then stop waiting after grace + max_wait
    assert ready_for_batch(files, None, now=1100, **kw) == []
    assert [f.day for f in ready_for_batch(files, None, now=1131, **kw)] == [1]
