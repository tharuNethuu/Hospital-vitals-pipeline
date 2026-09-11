import os
import csv
import random
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","component":"lab_results_simulator","message":"%(message)s"}',
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)
logger = logging.getLogger("lab_results_simulator")

# Same fixed patient pool as the vitals stream — must match for the join to work
PATIENT_IDS = [f"P{100 + i}" for i in range(15)]  # P100 ... P114

TEST_TYPES = {
    "Hemoglobin": {"unit": "g/dL", "normal_range": (12.0, 17.0)},
    "WBC_Count": {"unit": "x10^9/L", "normal_range": (4.0, 11.0)},
    "Creatinine": {"unit": "mg/dL", "normal_range": (0.6, 1.3)},
    "Glucose": {"unit": "mg/dL", "normal_range": (70, 140)},
    "CRP": {"unit": "mg/L", "normal_range": (0, 10)},  # C-reactive protein, inflammation marker
}

OUTPUT_DIR = "lab_results_drops"
SIMULATED_DAY_SECONDS = 120  # 1 simulated "day" = 2 real minutes


def generate_result(test_type: str) -> float:
    """Generate a result value, occasionally out-of-range to simulate real abnormal labs."""
    low, high = TEST_TYPES[test_type]["normal_range"]
    if random.random() < 0.15:  # 15% chance of an abnormal result
        return round(random.choice([
            low - abs(low) * random.uniform(0.2, 0.6),
            high + abs(high) * random.uniform(0.2, 0.6)
        ]), 2)
    return round(random.uniform(low, high), 2)


def generate_daily_batch(day_number: int) -> str:
    """Generate one day's lab results file for a random subset of patients."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    tested_patients = random.sample(PATIENT_IDS, k=random.randint(5, 10))
    filename = f"{OUTPUT_DIR}/lab_results_day{day_number}_{int(time.time())}.csv"

    abnormal_count = 0
    row_count = 0

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["patient_id", "test_type", "result_value", "reference_range", "collected_at"])

        for patient_id in tested_patients:
            num_tests = random.randint(1, 3)
            chosen_tests = random.sample(list(TEST_TYPES.keys()), k=num_tests)

            for test_type in chosen_tests:
                low, high = TEST_TYPES[test_type]["normal_range"]
                result = generate_result(test_type)
                ref_range = f"{low}-{high} {TEST_TYPES[test_type]['unit']}"
                collected_at = datetime.now(timezone.utc).isoformat()

                writer.writerow([patient_id, test_type, result, ref_range, collected_at])
                row_count += 1

                if result < low or result > high:
                    abnormal_count += 1
                    logger.warning(
                        f"Abnormal lab result: patient={patient_id} test={test_type} "
                        f"result={result} reference_range={ref_range}"
                    )

    logger.info(
        f"Lab results file written: {filename} | patients={len(tested_patients)} "
        f"rows={row_count} abnormal={abnormal_count}"
    )
    return filename


def main():
    day_number = 1
    logger.info(f"Starting daily lab results simulator (1 simulated day = {SIMULATED_DAY_SECONDS}s)")

    try:
        while True:
            filename = generate_daily_batch(day_number)
            logger.info(f"[Day {day_number}] Lab results file dropped: {filename}")
            day_number += 1
            time.sleep(SIMULATED_DAY_SECONDS)

    except KeyboardInterrupt:
        logger.info("Stopping lab results simulator...")


if __name__ == "__main__":
    main()