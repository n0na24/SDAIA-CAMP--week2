from __future__ import annotations
import pandas as pd


def safe_left_join(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str | list[str],
    validate: str = "many_to_one",
    suffixes: tuple[str, str] = ("", "_r"),
    check_row_count: bool = True,
) -> pd.DataFrame:
    """
    Safe left join with:
    - enforced join cardinality via `validate`
    - optional protection against join explosion
    - column name conflict handling via `suffixes`
    """

    before = len(left)

    out = left.merge(
        right,
        how="left",
        on=on,
        validate=validate,
        suffixes=suffixes,
    )

    if check_row_count:
        after = len(out)
        assert before == after, (
            f"Join explosion detected: {before} -> {after}"
        )

    return out
