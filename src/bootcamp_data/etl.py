from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from pathlib import Path

import pandas as pd

# Day 1 I/O
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet

# Day 2 quality + cleaning
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)

# Day 2/3 transforms
from bootcamp_data.transforms import (
    enforce_schema,
    normalize_text,
    apply_mapping,
    add_missing_flags,
    parse_datetime,
    add_time_parts,
    winsorize,
    add_outlier_flag,
)

# Day 3 joins
from bootcamp_data.joins import safe_left_join

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path

    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract: read raw inputs."""
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def transform(orders_raw: pd.DataFrame, users_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Transform: compose Day2 + Day3 helpers into final analytics table.
    Returns: (orders_clean, users_clean, analytics_table, stats_dict)
    """
    # ---- Fail-fast checks (inputs) ----
    require_columns(
        orders_raw,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status"],
    )
    require_columns(users_raw, ["user_id", "country", "signup_date"])
    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users_raw, "users_raw")

    # users must be unique for many_to_one join
    assert_unique_key(users_raw, "user_id")

    # ---- Clean users (keep it simple) ----
    users = users_raw.copy()
    # (اختياري) تطبيع بسيط للنصوص لو عندك أعمدة نصية تحتاج
    if "country" in users.columns:
        users["country"] = normalize_text(users["country"])

    # ---- Clean + enrich orders ----
    status_map = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
        "returned": "refund",
    }

    orders = (
        orders_raw.copy()
        .pipe(enforce_schema)
        .assign(
            status_clean=lambda d: apply_mapping(
                normalize_text(d["status"]),
                status_map,
            )
        )
        .pipe(add_missing_flags, cols=["amount", "quantity", "created_at", "status"])
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    # outliers + winsor for charts (keep raw amount for totals too)
    # winsorize should return a Series/ndarray; if your winsorize returns Series, this works:
    if "amount" in orders.columns:
        orders = orders.assign(amount_winsor=winsorize(orders["amount"]))
        orders = add_outlier_flag(orders, "amount")

    # ---- Join orders -> users (safe) ----
    analytics = safe_left_join(
        orders,
        users,
        on="user_id",
        validate="many_to_one",
        # suffixes optional depending on your implementation
        # suffixes=("", "_user"),
        check_row_count=True,
    )

    # orders_clean = orders-only view (drop user-side columns that got joined)
    user_side_cols = [c for c in users.columns if c != "user_id"]
    orders_clean = analytics.drop(columns=[c for c in user_side_cols if c in analytics.columns], errors="ignore")

    # ---- stats for _run_meta.json ----
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = None
    if "country" in analytics.columns:
        country_match_rate = float(1.0 - analytics["country"].isna().mean())

    stats = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users_raw": int(len(users_raw)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at_after_parse": missing_created_at,
        "country_match_rate": country_match_rate,
    }

    return orders_clean, users, analytics, stats


def load_outputs(*, orders_clean: pd.DataFrame, users: pd.DataFrame, analytics: pd.DataFrame, cfg: ETLConfig) -> None:
    """Load: write processed artifacts (idempotent overwrite)."""
    cfg.out_orders_clean.parent.mkdir(parents=True, exist_ok=True)

    write_parquet(orders_clean, cfg.out_orders_clean)
    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)


def write_run_meta(cfg: ETLConfig, *, stats: dict) -> None:
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)

    meta = {
        **stats,
        "inputs": {
            "orders_raw": str(cfg.raw_orders),
            "users_raw": str(cfg.raw_users),
        },
        "outputs": {
            "orders_clean": str(cfg.out_orders_clean),
            "users": str(cfg.out_users),
            "analytics_table": str(cfg.out_analytics),
            "run_meta": str(cfg.run_meta),
        },
        "config": {
            "root": str(cfg.root),
        },
    }

    cfg.run_meta.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs...")
    orders_raw, users_raw = load_inputs(cfg)
    log.info("orders_raw=%s users_raw=%s", len(orders_raw), len(users_raw))

    log.info("Transforming...")
    orders_clean, users, analytics, stats = transform(orders_raw, users_raw)
    log.info("analytics=%s", len(analytics))

    log.info("Writing outputs...")
    load_outputs(orders_clean=orders_clean, users=users, analytics=analytics, cfg=cfg)

    log.info("Writing run metadata...")
    write_run_meta(cfg, stats=stats)

    log.info("Done. Outputs in: %s", cfg.out_analytics.parent)
