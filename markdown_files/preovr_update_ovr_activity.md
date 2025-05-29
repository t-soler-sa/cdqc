# Overview Pre-Override Update Override Activity

The script **`update_ovr_db_active_col.py`** refreshes an *overrides* table that is used to overwrite (“override”) values coming from a Clarity ESG-data feed.  
The high-level flow is:

1. **Load all sources**  
   * `df_clarity` — latest issuer-level feed (CSV)  
   * `overrides` — current override database (Excel)  
   * `crossreference` — PermID ⇄ Aladdin-ID mapping (CSV)

2. **Data quality checks**  
   * Detect overrides that have two different `ovr_value`s for the same key (`aladdin_id`, `ovr_target`).  
   * Clean the cross-reference (drop `NaN` PermIDs, remove duplicates).

3. **Enrich Clarity with Aladdin IDs**  
   * Join `df_clarity` ↔ `crossreference` on `permid`.  
   * Remove rows that still have empty or duplicate `aladdin_id`s.

4. **Filter to the issuers that actually have overrides.**

5. **Update the override table**  
   * **`df_value` column** — for every (`aladdin_id`,`ovr_target`) pair, pull the latest value from the Clarity feed.  
   * **`ovr_active` flag** — set to **`False`** whenever the override value **equals** the data-feed value, meaning the override is no longer needed.

6. **Produce three Excel files**  
   * fresh override DB (`overrides_db_beta.xlsx`)  
   * time-stamped backup of the original (`.../backup/YYYYMM_override_db.xlsx`)  
   * list of newly de-activated overrides.

With the log we can see exactly **what** each dataset looks like at every stage; below are the first 10 rows captured in the run.

---

### 1. Input datasets

#### 1.1 `df_clarity`
str_001_s | str_002_ec | str_003_ec | str_004_asec | str_005_ec | cs_001_sec | cs_002_ec | str_006_sec | str_007_sect | art_8_basicos | permid | str_003b_ec
:--|:--|:--|:--|:--|:--|:--|:--|:--|:--|:--|:--
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5036159086|OK
OK|OK|OK|EXCLUDED|OK|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|4297073705|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5038058818|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296543204|OK
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|8589934312|EXCLUDED
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|5000016911|EXCLUDED
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296080185|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5000045326|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296726047|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4295868248|OK


#### 1.2 `overrides`
clarityid | permid | aladdin_id | issuer_name | ovr_target | df_value | ovr_value | ovr_active
:--|:--|:--|:--|:--|:--|:--|:--
18621|4295894740|375|BP PLC|str_002_ec|OK|EXCLUDED|True
 |4295895363|2800|SANTANDER UK PLC|str_005_ec| |OK|True
 |4296457498|2801|SANTANDER FINANCIAL SERVICES PLC|str_005_ec| |OK|True
27220|4295903265|2824|Abbott Laboratories|str_001_s|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_003_ec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_004_asec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_006_sec|FLAG|OK|True
16460|8589934205|7699|Banco Santander SA|str_001_s|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_002_ec|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_003_ec|EXCLUDED|OK|True


#### 1.3 `crossreference_raw`
aladdin_id | issuer_name | permid | msci | sust
:--|:--|:--|:--|:--
H56976|AUXIFIP SA|5001248970|IID000000002682941|
H57042|AVESTA TECHNOLOGIES LLC|4295900331| | 
H57890|INFANT BACTERIAL THERAPEUTICS AB|5040202605|IID000000002761045|2004150866
H57901|MB SECURITIES JSC|4298118784|IID000000002761038|
H57917|BARCODE 121 HOLDING AS|5050698850| |
H57921|FIRST FEDERAL OF OLATHE BANCORP INC|4295905243| |
H57922|ABQ FINANCE LTD|5050698905|IID000000002760999|1336472509
H57935|FIRST NATIONAL BANCORP INC (MICHIGAN)|4295909893| |
H57936|CVC CORDATUS LOAN FUND VI DAC|5050698612| |
H57958|OHB EIENDOMSHOLDING AS|5050698854| |


#### 1.4 `crossreference_cleaned`  
(same 10 rows as above — duplicates & NaNs already removed)

---

### 2. Intermediate dataset

#### 2.1 `df_clarity_with_aladdin_id`  
(the feed after joining with the cleaned cross-reference)

str_001_s | str_002_ec | str_003_ec | str_004_asec | str_005_ec | cs_001_sec | cs_002_ec | str_006_sec | str_007_sect | art_8_basicos | permid | str_003b_ec | aladdin_id
:--|:--|:--|:--|:--|:--|:--|:--|:--|:--|:--|:--|:--
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5036159086|OK|F90449
OK|OK|OK|EXCLUDED|OK|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|4297073705|OK|D37193
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5038058818|OK|G13037
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296543204|OK|B75918
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|8589934312|EXCLUDED|128005
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|5000016911|EXCLUDED|63230E
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296080185|OK|R72792
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5000045326|OK|B30407
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296726047|OK|C74027
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4295868248|OK|R57937


*(The script then drops 18 247 blank `aladdin_id`s and the same number of duplicates, leaving 576 unique, matched issuers.)*

---

### 3. Output datasets

#### 3.1 `overrides_updated`
clarityid | permid | aladdin_id | issuer_name | ovr_target | df_value | ovr_value | ovr_active
:--|:--|:--|:--|:--|:--|:--|:--
18621|4295894740|375|BP PLC|str_002_ec|OK|EXCLUDED|True
 |4295895363|2800|SANTANDER UK PLC|str_005_ec| |OK|True
 |4296457498|2801|SANTANDER FINANCIAL SERVICES PLC|str_005_ec| |OK|True
27220|4295903265|2824|Abbott Laboratories|str_001_s|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_003_ec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_004_asec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_006_sec|FLAG|OK|True
16460|8589934205|7699|Banco Santander SA|str_001_s|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_002_ec|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_003_ec|EXCLUDED|OK|True


#### 3.2 `deactivated_overrides`
clarityid | permid | aladdin_id | issuer_name | ovr_target | df_value | ovr_value | ovr_active
:--|:--|:--|:--|:--|:--|:--|:--
27243|4295909075|48825|Atlantic Richfield Co|cs_002_ec|OK|OK|False
171097|4296546906|123123|Bayer Corp|cs_002_ec|OK|OK|False
 |4296546906|123123|BAYER CORP|str_002_ec|EXCLUDED|EXCLUDED|False
41101|4295903627|126650|CVS Health Corp|art_8_basicos|OK|OK|False
41101|4295903627|126650|CVS Health Corp|cs_001_sec|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_001_s|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_004_asec|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_006_sec|OK|OK|False
180923|4296717558|319252|US Bank NA|str_001_s|OK|OK|False
180923|4296717558|319252|US Bank NA|str_002_ec|OK|OK|False


*(Total rows de-activated in this run: **573**.)*

---

## What the script is **really** doing (step-by-step)

| Step | Transformation | Key columns | Purpose |
|------|----------------|------------|---------|
| 1 | **Load** three files (Clarity feed, override DB, cross-reference) | — | Put everything into Pandas DataFrames. |
| 2 | **Find malformed overrides** with two different `ovr_value`s for the same (`aladdin_id`,`ovr_target`) | override DB | Data-quality warning (none found in this run). |
| 3 | **Clean cross-reference** (`permid` must exist, keep first occurrence) | permid | Prepare for join. |
| 4 | **Add `aladdin_id` to Clarity feed** via `permid` join | permid, aladdin_id | Align the feed with override keys. |
| 5 | **Drop rows** if `aladdin_id` is null or duplicated | aladdin_id | Keep one-to-one mapping. |
| 6 | **Filter Clarity** to only issuers that actually appear in overrides | aladdin_id | Performance + relevance. |
| 7 | **Update `df_value`** in overrides:  
&emsp;*a)* melt Clarity feed to long format (`aladdin_id`,`ovr_target`,`clarity_value`)  
&emsp;*b)* left-join to overrides  
&emsp;*c)* if a Clarity value exists, overwrite `df_value` | aladdin_id, ovr_target | Carry the latest feed value into the DB. |
| 8 | **Deactivate overrides** (`ovr_active = False`) **when** `ovr_value == df_value` | aladdin_id, ovr_target | An override is pointless if the live feed already matches it. |
| 9 | **Save** three Excel files | — | Persist changes and backups. |

---

## How to reproduce the same logic in **PySpark**

```python
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.getOrCreate()

# 1) read data ---------------------------------------------------------------
df_clarity     = spark.read.options(header=True).csv(".../20250601_df_issuer_level_without_ovr.csv")
df_overrides   = spark.read.options(header=True).format("com.crealytics.spark.excel") \
                           .load(".../overrides_db.xlsx")
df_crossref    = spark.read.options(header=True).csv(".../Aladdin_Clarity_Issuers_20250601.csv")

# 2) data-quality: conflicting overrides -------------------------------------
w_key  = Window.partitionBy("aladdin_id", "ovr_target")
duplicates = (df_overrides
              .groupBy("aladdin_id","ovr_target")
              .agg(F.countDistinct("ovr_value").alias("n"))
              .filter("n > 1"))

# 3) clean cross-reference ---------------------------------------------------
df_crossref = (df_crossref
               .filter(F.col("permid").isNotNull())
               .dropDuplicates(["permid"]))

# 4) enrich clarity with Aladdin-ID ------------------------------------------
df_clarity = (df_clarity
              .join(df_crossref.select("permid","aladdin_id"), "permid", "left"))

# 5) remove null / duplicate Aladdin-IDs -------------------------------------
df_clarity = (df_clarity
              .filter(F.col("aladdin_id").isNotNull())
              .dropDuplicates(["aladdin_id"]))

# 6) keep only issuers present in overrides ----------------------------------
ids = [row["aladdin_id"] for row in df_overrides.select("aladdin_id").distinct().collect()]
df_clarity = df_clarity.filter(F.col("aladdin_id").isin(ids))

# 7) update df_value ---------------------------------------------------------
long_feed = (df_clarity
             .select("aladdin_id", *clarity_test_cols)  # same 11 test columns
             .selectExpr("aladdin_id",
                         "stack(11," +
                         ", ".join(f"'{c}', {c}" for c in clarity_test_cols) +
                         ") as (ovr_target, clarity_value)"))

df_overrides = (df_overrides
                .join(long_feed, ["aladdin_id","ovr_target"], "left")
                .withColumn("df_value",
                            F.coalesce("clarity_value","df_value"))
                .drop("clarity_value"))

# 8) deactivate matching overrides ------------------------------------------
df_overrides = df_overrides.withColumn(
    "ovr_active",
    F.when(F.col("ovr_value") == F.col("df_value"), F.lit(False))
     .otherwise(F.col("ovr_active"))
)

# 9) write results -----------------------------------------------------------
(df_overrides
 .write.mode("overwrite")
 .format("com.crealytics.spark.excel")
 .option("header", True)
 .save(".../overrides_db_beta.xlsx"))
```

Replace the three `read` / `write` paths with your own storage locations (or switch to `parquet`/`csv` writers if Excel isn’t required).

---

### Key take-aways

* **Deactivate overrides automatically** whenever they no longer differ from the authoritative data feed.  
* **Track provenance** — keep both the latest override DB and a time-stamped backup.  
* **Use a one-to-one key** (`aladdin_id`, `ovr_target`) to avoid ambiguity and ensure deterministic updates.  
* **The workflow is join-filter-melt-update**; that pattern translates cleanly from Pandas to PySpark.


## 6. Python Code

```python

# update_ovr_db_active_col.py

"""
Script to update the OVR database with active columns.
Check latest datafeed and update the OVR database with active columns.
If datafeed value and the override value are the same active column is FALSE.
"""
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd
from pandas.api.types import is_scalar

from scripts.utils.config import get_config
from scripts.utils.dataloaders import (
    load_clarity_data,
    load_overrides,
    load_crossreference,
)
from scripts.utils.clarity_data_quality_control_functions import log_df_head_compact

# config script
config = get_config("update-ovr-db-active-col", interactive=False, gen_output_dir=False)
logger = config["logger"]
DATE = config["DATE"]
paths = config["paths"]
df_path = paths["CURRENT_DF_WOUTOVR_PATH"]
overrides_path = paths["OVR_PATH"]
crossreference_path = paths["CROSSREFERENCE_PATH"]

clarity_test_col = [
    "permid",
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_007_sect",
    "art_8_basicos",
    "cs_001_sec",
    "cs_002_ec",
]


target_cols_overrides = [
    "permid",
    "aladdin_id",
    "clarityid",
    "issuer_name",
    "ovr_target",
    "ovr_value",
    "ovr_active",
    "df_value",
]


# Define Regular functions
def update_df_value_column(
    overrides: pd.DataFrame, df_clarity_filtered: pd.DataFrame
) -> pd.DataFrame:
    """
    Updates the 'df_value' column in the overrides DataFrame using values
    from the df_clarity_filtered DataFrame based on matching 'aladdin_id' and 'ovr_target'.

    Parameters:
    - overrides: DataFrame containing override entries with 'aladdin_id' and 'ovr_target' columns.
    - df_clarity_filtered: Filtered DataFrame from Clarity data with columns including 'aladdin_id'.

    Returns:
    - A new DataFrame with the updated 'df_value' column.
    """
    # Melt df_clarity_filtered to long format for easier matching
    clarity_melted = df_clarity_filtered.melt(
        id_vars="aladdin_id", var_name="ovr_target", value_name="clarity_value"
    )

    # Merge to bring in the matching clarity value
    overrides_updated = overrides.merge(
        clarity_melted, how="left", on=["aladdin_id", "ovr_target"]
    )

    # Update the df_value column only where clarity_value is present
    overrides["df_value"] = overrides_updated["clarity_value"].combine_first(
        overrides["df_value"]
    )

    return overrides


def update_override_active(
    overrides: pd.DataFrame,
    df_clarity_filtered: pd.DataFrame,
) -> pd.DataFrame:

    clarity_melted = df_clarity_filtered.melt(
        id_vars="aladdin_id", var_name="ovr_target", value_name="clarity_value"
    ).dropna(subset=["clarity_value"])

    duplicates = clarity_melted.duplicated(["aladdin_id", "ovr_target"])
    if duplicates.any():
        dup_keys = clarity_melted.loc[duplicates, ["aladdin_id", "ovr_target"]]
        raise ValueError(
            f"[DQ] {len(dup_keys)} duplicate keys in clarity feed:\n"
            f"{dup_keys.head().to_string(index=False)}"
        )

    overrides_merged = overrides.merge(
        clarity_melted, on=["aladdin_id", "ovr_target"], how="left"
    )

    condition = overrides_merged["ovr_value"] == overrides_merged["df_value"]

    try:
        overrides.loc[condition.values, "ovr_active"] = False
    except Exception as e:
        logger.error("Error updating overrides: %s", e)
        raise

    return overrides


def find_conflicting_columns(
    df: pd.DataFrame,
    id_col: str = "aladdin_id",
    conflict_col_a: str = "ovr_target",
    conflict_col_b: str = "ovr_value",
) -> pd.DataFrame:
    grouping_cols = [id_col, conflict_col_a]
    target_cols = grouping_cols + [conflict_col_b]

    # Step 1: Count unique conflict_col_b values per group
    grouped_df = df.groupby(grouping_cols)[conflict_col_b].nunique()

    # Step 2: Filter to groups with more than one unique conflict_col_b
    conflicting_keys = grouped_df[grouped_df > 1].index

    # Step 3: Use a mask to filter original DataFrame
    mask = df.set_index(grouping_cols).index.isin(conflicting_keys)
    return df[mask].sort_values(by=grouping_cols)[target_cols].copy()


def main():
    # load data

    df_clarity = load_clarity_data(df_path, target_cols=clarity_test_col)
    log_df_head_compact(df_clarity, df_name="df_clarity")
    overrides = load_overrides(
        overrides_path, target_cols=target_cols_overrides, drop_active=False
    )
    log_df_head_compact(overrides, df_name="overrides")
    troubles_overrides = find_conflicting_columns(overrides)
    log_df_head_compact(troubles_overrides, df_name="troubles_overrides")

    logger.info(
        f"\ntroubles_overrides first 10 rows is {troubles_overrides.head(10)}\n"
    )

    logger.info(f"There are {len(troubles_overrides)} conflicting rows in overrides\n")

    # save back columns for backup
    overrides_copy = overrides.copy()
    crossreference = load_crossreference(crossreference_path)
    log_df_head_compact(crossreference, df_name="crossreference_raw")
    logger.info("Removing duplicates and NaN values from crossreference")
    crossreference = crossreference.dropna(subset=["permid"]).drop_duplicates(
        subset=["permid"]
    )
    log_df_head_compact(crossreference, df_name="crossreference_cleaned")
    # set permid in crossreference as str
    crossreference["permid"] = crossreference["permid"].apply(
        lambda x: str(x) if pd.notna(x) else x
    )

    # add aladdin_id to df_clarity from crossreference
    df_clarity = df_clarity.merge(
        crossreference[["permid", "aladdin_id"]], on="permid", how="left"
    )
    log_df_head_compact(df_clarity, df_name="df_clarity_with_aladdin_id")
    # set permid in df_clarity & overrides as str
    logger.info("Setting permid in df_clarity & overrides as str - to avoid issues")
    df_clarity["aladdin_id"] = df_clarity["aladdin_id"].apply(
        lambda x: str(x) if pd.notna(x) else x
    )
    overrides["aladdin_id"] = overrides["aladdin_id"].astype(str)

    empty_aladdin_rows = df_clarity["aladdin_id"].isna().sum()
    duplicated_aladdin_rows = df_clarity["aladdin_id"].duplicated().sum()
    logger.info(
        f"""\nRows with empty aladdin_id on df_clarity: {empty_aladdin_rows}.
        \nRows with duplicate {duplicated_aladdin_rows}."""
    )
    if empty_aladdin_rows > 0:
        logger.info(f"We will drop Rows with empty aladdin_id on df_clarity.")
        # drop rows with empty aladdin_id on df_clarity
        df_clarity = df_clarity.dropna(subset=["aladdin_id"])

    if duplicated_aladdin_rows > 0:
        logger.info(f"We will drop Rows with duplicate aladdin_id on df_clarity.")
        # drop rows with duplicate aladdin_id on df_clarity
        df_clarity = df_clarity.drop_duplicates(subset=["aladdin_id"])

    # filter out only the columns we need with using the relevant permids
    df_clarity_filtered = df_clarity[
        df_clarity["aladdin_id"].isin(overrides["aladdin_id"])
    ].copy()

    if df_clarity_filtered.shape[0] < 1:
        logger.warning("No matches between df_clarity and overrrides!")
        sys.exit()

    logger.info(f"Size df_clarity_filterd is {df_clarity_filtered.shape[0]}")

    # define output paths
    base_path = Path(
        r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides"
    )
    current_date = datetime.now().strftime("%Y%m%d")
    output_file = base_path / "overrides_db_beta.xlsx"
    backup_file = base_path / "overrides_db_backup" / f"{DATE}_override_db.xlsx"
    deactivated_overrides_file = (
        base_path / f"{current_date}_{DATE}_deactivated_overrides.xlsx"
    )

    # update active column df_value of overrides with data from df_clarity
    logger.info("Updating overrides df_value column")
    overrides = update_df_value_column(overrides, df_clarity_filtered)

    # update active status of overrides
    logger.info("updating overrides active status")
    overrides = update_override_active(overrides, df_clarity_filtered)
    log_df_head_compact(overrides, df_name="overrides_updated")

    # RETURN DF OF OVERRIDES THAT HAS BEEN DEACTIVATED
    deactivated_overrides = overrides[overrides["ovr_active"] == False]
    log_df_head_compact(deactivated_overrides, df_name="deactivated_overrides")
    # log length of deactivated overrides
    logger.info(f"Number of deactivated overrides: {len(deactivated_overrides)}")

    # save the updated overrides to the output file
    logger.info(
        f"Saving updated overrides to {output_file}\nand backup to {backup_file}"
    )
    overrides.to_excel(output_file, index=False)
    overrides_copy.to_excel(backup_file, index=False)
    deactivated_overrides.to_excel(deactivated_overrides_file, index=False)


if __name__ == "__main__":
    main()
    logger.info("Script completed successfully.")

```

## 7. Example of Log
Here is the log from the output:

"""
2025-05-28 14:05:42,966 - scripts.utils.get_date - INFO - [get_date.py:56 in validate_date()] - Date format is valid. Date set to 202506.
2025-05-28 14:05:42,967 - scripts.utils.get_date - INFO - [get_date.py:33 in get_date()] - Date provided with --date flag: 202506
2025-05-28 14:05:42,967 - scripts.utils.dataloaders - INFO - [dataloaders.py:134 in load_clarity_data()] - Loading Clarity data from: C:\Users\n740789\Documents\Projects_local\datasets\datafeeds\datafeeds_without_ovr\2025\20250601_df_issuer_level_without_ovr.csv
2025-05-28 14:05:43,661 - scripts.utils.dataloaders - INFO - [dataloaders.py:157 in load_clarity_data()] - Successfully loaded Clarity data from: C:\Users\n740789\Documents\Projects_local\datasets\datafeeds\datafeeds_without_ovr\2025\20250601_df_issuer_level_without_ovr.csv
2025-05-28 14:05:43,896 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df df_clarity

**str_001_s**|**str_002_ec**|**str_003_ec**|**str_004_asec**|**str_005_ec**|**cs_001_sec**|**cs_002_ec**|**str_006_sec**|**str_007_sect**|**art_8_basicos**|**permid**|**str_003b_ec**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5036159086|OK
OK|OK|OK|EXCLUDED|OK|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|4297073705|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5038058818|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296543204|OK
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|8589934312|EXCLUDED
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|5000016911|EXCLUDED
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296080185|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5000045326|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296726047|OK
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4295868248|OK


2025-05-28 14:05:43,897 - scripts.utils.dataloaders - INFO - [dataloaders.py:229 in load_overrides()] - Loading overrides from: C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db.xlsx
2025-05-28 14:05:44,645 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df overrides

**clarityid**|**permid**|**aladdin_id**|**issuer_name**|**ovr_target**|**df_value**|**ovr_value**|**ovr_active**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
18621|4295894740|375|BP PLC|str_002_ec|OK|EXCLUDED|True
|4295895363|2800|SANTANDER UK PLC|str_005_ec||OK|True
|4296457498|2801|SANTANDER FINANCIAL SERVICES PLC|str_005_ec||OK|True
27220|4295903265|2824|Abbott Laboratories|str_001_s|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_003_ec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_004_asec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_006_sec|FLAG|OK|True
16460|8589934205|7699|Banco Santander SA|str_001_s|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_002_ec|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_003_ec|EXCLUDED|OK|True


2025-05-28 14:05:44,661 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df troubles_overrides

**aladdin_id**|**ovr_target**|**ovr_value**
:-----:|:-----:|:-----:


2025-05-28 14:05:44,665 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:157 in main()] - 
troubles_overrides first 10 rows is Empty DataFrame
Columns: [aladdin_id, ovr_target, ovr_value]
Index: []

2025-05-28 14:05:44,666 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:161 in main()] - There are 0 conflicting rows in overrides

2025-05-28 14:05:44,667 - scripts.utils.dataloaders - INFO - [dataloaders.py:197 in load_crossreference()] - Loading crossreference data from: C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\aladdin_data\crossreference\Aladdin_Clarity_Issuers_20250601.csv
2025-05-28 14:05:44,881 - scripts.utils.dataloaders - INFO - [dataloaders.py:203 in load_crossreference()] - Cleaning columns and renaming crossreference data
2025-05-28 14:05:44,883 - scripts.utils.dataloaders - INFO - [dataloaders.py:210 in load_crossreference()] - Successfully loaded crossreference from: C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\aladdin_data\crossreference\Aladdin_Clarity_Issuers_20250601.csv
2025-05-28 14:05:44,886 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df crossreference_raw

**aladdin_id**|**issuer_name**|**permid**|**msci**|**sust**
:-----:|:-----:|:-----:|:-----:|:-----:
H56976|AUXIFIP SA|5001248970|IID000000002682941|
H57042|AVESTA TECHNOLOGIES LLC|4295900331||
H57890|INFANT BACTERIAL THERAPEUTICS AB|5040202605|IID000000002761045|2004150866
H57901|MB SECURITIES JSC|4298118784|IID000000002761038|
H57917|BARCODE 121 HOLDING AS|5050698850||
H57921|FIRST FEDERAL OF OLATHE BANCORP INC|4295905243||
H57922|ABQ FINANCE LTD|5050698905|IID000000002760999|1336472509
H57935|FIRST NATIONAL BANCORP INC (MICHIGAN)|4295909893||
H57936|CVC CORDATUS LOAN FUND VI DAC|5050698612||
H57958|OHB EIENDOMSHOLDING AS|5050698854||


2025-05-28 14:05:44,889 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:167 in main()] - Removing duplicates and NaN values from crossreference
2025-05-28 14:05:44,930 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df crossreference_cleaned

**aladdin_id**|**issuer_name**|**permid**|**msci**|**sust**
:-----:|:-----:|:-----:|:-----:|:-----:
H56976|AUXIFIP SA|5001248970|IID000000002682941|
H57042|AVESTA TECHNOLOGIES LLC|4295900331||
H57890|INFANT BACTERIAL THERAPEUTICS AB|5040202605|IID000000002761045|2004150866
H57901|MB SECURITIES JSC|4298118784|IID000000002761038|
H57917|BARCODE 121 HOLDING AS|5050698850||
H57921|FIRST FEDERAL OF OLATHE BANCORP INC|4295905243||
H57922|ABQ FINANCE LTD|5050698905|IID000000002760999|1336472509
H57935|FIRST NATIONAL BANCORP INC (MICHIGAN)|4295909893||
H57936|CVC CORDATUS LOAN FUND VI DAC|5050698612||
H57958|OHB EIENDOMSHOLDING AS|5050698854||


2025-05-28 14:05:45,054 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df df_clarity_with_aladdin_id

**str_001_s**|**str_002_ec**|**str_003_ec**|**str_004_asec**|**str_005_ec**|**cs_001_sec**|**cs_002_ec**|**str_006_sec**|**str_007_sect**|**art_8_basicos**|**permid**|**str_003b_ec**|**aladdin_id**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5036159086|OK|F90449
OK|OK|OK|EXCLUDED|OK|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|4297073705|OK|D37193
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5038058818|OK|G13037
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296543204|OK|B75918
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|8589934312|EXCLUDED|128005
EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|EXCLUDED|OK|EXCLUDED|EXCLUDED|OK|OK|5000016911|EXCLUDED|63230E
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296080185|OK|R72792
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5000045326|OK|B30407
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4296726047|OK|C74027
OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|4295868248|OK|R57937


2025-05-28 14:05:45,058 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:183 in main()] - Setting permid in df_clarity & overrides as str - to avoid issues
2025-05-28 14:05:45,090 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:191 in main()] - 
Rows with empty aladdin_id on df_clarity: 18247.
        
Rows with duplicate 18247.
2025-05-28 14:05:45,091 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:196 in main()] - We will drop Rows with empty aladdin_id on df_clarity.
2025-05-28 14:05:45,107 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:201 in main()] - We will drop Rows with duplicate aladdin_id on df_clarity.
2025-05-28 14:05:45,128 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:214 in main()] - Size df_clarity_filterd is 576
2025-05-28 14:05:45,129 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:228 in main()] - Updating overrides df_value column
2025-05-28 14:05:45,139 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:232 in main()] - updating overrides active status
2025-05-28 14:05:45,152 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df overrides_updated

**clarityid**|**permid**|**aladdin_id**|**issuer_name**|**ovr_target**|**df_value**|**ovr_value**|**ovr_active**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
18621|4295894740|375|BP PLC|str_002_ec|OK|EXCLUDED|True
|4295895363|2800|SANTANDER UK PLC|str_005_ec||OK|True
|4296457498|2801|SANTANDER FINANCIAL SERVICES PLC|str_005_ec||OK|True
27220|4295903265|2824|Abbott Laboratories|str_001_s|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_003_ec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_004_asec|FLAG|OK|True
27220|4295903265|2824|Abbott Laboratories|str_006_sec|FLAG|OK|True
16460|8589934205|7699|Banco Santander SA|str_001_s|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_002_ec|EXCLUDED|OK|True
16460|8589934205|7699|Banco Santander SA|str_003_ec|EXCLUDED|OK|True


2025-05-28 14:05:45,162 - update-ovr-db-active-col - INFO - [clarity_data_quality_control_functions.py:1002 in log_df_head_compact()] - 


current look of df deactivated_overrides

**clarityid**|**permid**|**aladdin_id**|**issuer_name**|**ovr_target**|**df_value**|**ovr_value**|**ovr_active**
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:
27243|4295909075|48825|Atlantic Richfield Co|cs_002_ec|OK|OK|False
171097|4296546906|123123|Bayer Corp|cs_002_ec|OK|OK|False
|4296546906|123123|BAYER CORP|str_002_ec|EXCLUDED|EXCLUDED|False
41101|4295903627|126650|CVS Health Corp|art_8_basicos|OK|OK|False
41101|4295903627|126650|CVS Health Corp|cs_001_sec|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_001_s|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_004_asec|OK|OK|False
41101|4295903627|126650|CVS Health Corp|str_006_sec|OK|OK|False
180923|4296717558|319252|US Bank NA|str_001_s|OK|OK|False
180923|4296717558|319252|US Bank NA|str_002_ec|OK|OK|False


2025-05-28 14:05:45,167 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:240 in main()] - Number of deactivated overrides: 573
2025-05-28 14:05:45,167 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:243 in main()] - Saving updated overrides to C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db_beta.xlsx
and backup to C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\sri_data\overrides\overrides_db_backup\202506_override_db.xlsx
2025-05-28 14:05:45,793 - update-ovr-db-active-col - INFO - [update_ovr_db_active_col.py:253 in <module>()] - Script completed successfully.

"""