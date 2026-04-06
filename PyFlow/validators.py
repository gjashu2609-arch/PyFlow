
import re
import pandas as pd
import logging
from exceptions_custom import ValidationError

logger = logging.getLogger(__name__)


def validate_email(email: str) -> bool:
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, str(email)))


def validate_phone(phone: str) -> bool:
    cleaned = re.sub(r"[\s\-\(\)\+]", "", str(phone))
    return cleaned.isdigit() and len(cleaned) in [10, 11]


def validate_date_range(
    df: pd.DataFrame,
    date_col: str,
    min_date: str = "2024-01-01",
    max_date: str = "2024-12-31"
) -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    valid = df[
        (df[date_col] >= min_date) &
        (df[date_col] <= max_date)
    ]

    removed = len(df) - len(valid)
    if removed > 0:
        logger.warning(f"{removed} rows outside date range removed")

    return valid


def validate_dataframe(df: pd.DataFrame) -> dict[str, any]:
   
    results = {
        "total_rows":     len(df),
        "issues":         [],
        "passed":         True
    }

    required = ["fare_amount", "trip_distance"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        results["issues"].append(f"Missing columns: {missing_cols}")
        results["passed"] = False

    for col in required:
        if col in df.columns:
            null_pct = df[col].isna().mean() * 100
            if null_pct > 20:
                results["issues"].append(
                    f"Column '{col}' has {null_pct:.1f}% nulls — too many"
                )
                results["passed"] = False

    if "email" in df.columns:
        invalid_emails = df[~df["email"].apply(validate_email)]
        if len(invalid_emails) > 0:
            results["issues"].append(
                f"{len(invalid_emails)} invalid email formats found"
            )

    if "fare_amount" in df.columns:
        bad_fares = df[(df["fare_amount"] < 0) | (df["fare_amount"] > 1000)]
        if len(bad_fares) > 0:
            results["issues"].append(
                f"{len(bad_fares)} fares outside valid range ($0-$1000)"
            )

    logger.info(f"Validation: {'PASSED' if results['passed'] else 'FAILED'}")
    for issue in results["issues"]:
        logger.warning(f"  Issue: {issue}")

    return results