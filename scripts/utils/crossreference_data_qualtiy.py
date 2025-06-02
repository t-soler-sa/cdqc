import logging
from datetime import datetime
from pathlib import Path
import pandas as pd


def analyze_cross_reference(
    df: pd.DataFrame,
    main_id: str,
    secondary_id: str,
    out_dir: str | Path,
    logger: logging.Logger | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Check a one-to-one mapping between *main_id* and *secondary_id*.

    Parameters
    ----------
    df : pd.DataFrame
        Full cross-reference table.
    main_id : str
        Column with the primary identifier (e.g. issuer_id / aladdin_id).
    secondary_id : str
        Column with the secondary identifier (e.g. permid).
    out_dir : str | pathlib.Path
        Directory where result CSVs are written.
    logger : logging.Logger | None, default None
        If provided, basic statistics are emitted with logger.info().

    Returns
    -------
    dict[str, pd.DataFrame]
        Keys:  duplicate_full_rows,
               multiple_main_per_secondary,
               multiple_secondary_per_main,
               empty_permid_duplicate_main
    """
    out_dir = Path(out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d")

    # ------------------------------------------------------------------
    # Split the data
    # ------------------------------------------------------------------
    has_secondary = df[df[secondary_id].notna()].copy()
    no_secondary = df[df[secondary_id].isna()].copy()

    results: dict[str, pd.DataFrame] = {}

    # 1. Exact duplicate rows where secondary_id is present -------------
    dup_rows = has_secondary[has_secondary.duplicated(keep=False)]
    dup_rows.to_csv(out_dir / f"cross_brs_duplicate_full_rows_{stamp}.csv", index=False)
    results["duplicate_full_rows"] = dup_rows
    if logger:
        logger.info(
            "Exact duplicate rows (with %s present): %d", secondary_id, len(dup_rows)
        )

    # 2. Same secondary_id linked to multiple main_id values ------------
    multi_main_per_secondary = (
        has_secondary.groupby(secondary_id)
        .filter(lambda g: g[main_id].nunique() > 1)
        .sort_values([secondary_id, main_id])
    )
    multi_main_per_secondary.to_csv(
        out_dir / f"cross_brs_multiple_{main_id}_per_{secondary_id}_{stamp}.csv",
        index=False,
    )
    results["multiple_main_per_secondary"] = multi_main_per_secondary
    if logger:
        logger.info(
            "Secondary IDs with >1 distinct %s: %d (rows: %d)",
            main_id,
            multi_main_per_secondary[secondary_id].nunique(),
            len(multi_main_per_secondary),
        )

    # 3. Same main_id linked to multiple secondary_id values ------------
    multi_secondary_per_main = (
        has_secondary.groupby(main_id)
        .filter(lambda g: g[secondary_id].nunique() > 1)
        .sort_values([main_id, secondary_id])
    )
    multi_secondary_per_main.to_csv(
        out_dir / f"cross_brs_multiple_{secondary_id}_per_{main_id}_{stamp}.csv",
        index=False,
    )
    results["multiple_secondary_per_main"] = multi_secondary_per_main
    if logger:
        logger.info(
            "Primary IDs with >1 distinct %s: %d (rows: %d)",
            secondary_id,
            multi_secondary_per_main[main_id].nunique(),
            len(multi_secondary_per_main),
        )

    # 4. Duplicate main_id where secondary_id is missing ----------------
    dup_main_in_empty = no_secondary[
        no_secondary.duplicated(subset=[main_id], keep=False)
    ]
    dup_main_in_empty.to_csv(
        out_dir / f"empty_permid_duplicate_{main_id}_{stamp}.csv", index=False
    )
    results["empty_permid_duplicate_main"] = dup_main_in_empty
    if logger:
        logger.info(
            "Rows without %s but duplicate %s: %d",
            secondary_id,
            main_id,
            len(dup_main_in_empty),
        )

    return results
