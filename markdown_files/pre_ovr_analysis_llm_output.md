# Pre-OVR Analysis 

---

## 1.  Raw inputs

### 1.1 BRS portfolios `brs_carteras`
|issuer_name|aladdin_id|security_description|portfolio_full_name|portfolio_id|str_001_s|str_002_ec|str_003b_ec|str_003_ec|str_004_asec|str_004_asec_sust._bonds|str_005_ec|str_006_sec|str_007_sect|str_sfdr8_aec|scs_001_sec|scs_002_ec|scs_003_sec|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|SNT-WORLD|*nan*|||*nan*|||||||||||||
| |*nan*|10Y RTP 2.590 000 09-JUN-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.255 000 16-AUG-2039 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.800 000 12-JUL-2034 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.808 000 31-JUL-2034 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.840 000 30-MAY-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.850 000 29-MAY-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.904 500 28-MAY-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.960 000 06-JUN-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||
| |*nan*|10Y RTP 3.964 000 09-JUN-2025 10|SANTANDER GO DYNAMIC BOND|LXMS0640|||||||||||||

### 1.2 BRS benchmarks `brs_benchmarks`
|issuer_name|aladdin_id|portfolio_full_name|benchmark_id|benchmark_market_value_(m)|str_001_s|str_002_ec|str_003_ec|str_003b_ec|str_004_asec|str_005_ec|str_006_sec|str_007_sect|str_sfdr8_aec|scs_001_sec|scs_002_ec|scs_003_sec|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|SNT-WORLD|*nan*||*nan*|2.7246585e+11|||||||||||||
|BRAZIL FEDERATIVE REPUBLIC OF (GOVT)|R61159|100 % IRF-M 1|AB2IRFM1S|4 248 571 227.25|||||||||||||
|…|…|…|…|…|||||||||||||

### 1.3 Aladdin–Clarity cross-reference `crossreference`
|aladdin_id|issuer_name|permid|msci|sust|
|:---|:---|:---|:---|:---|
|H56976|AUXIFIP SA|5001248970|IID000000002682941| |
|H57042|AVESTA TECHNOLOGIES LLC|4295900331|| |
|…|…|…|…|…|

### 1.4 Previous Clarity file (2025-05, *with* overrides)
|isin|issuer_name|str_001_s|str_002_ec|str_003_ec|str_004_asec|str_005_ec|cs_001_sec|cs_002_ec|str_006_sec|str_007_sect|art_8_basicos|permid|str_003b_ec|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|CH1148212231|Credit Suisse Ag London Branch|FLAG|FLAG|OK|FLAG|FLAG|OK|OK|OK|FLAG|OK|4296785143|OK|
|…|…|…|…|…|…|…|…|…|…|…|…|…|…|

### 1.5 Current Clarity file (2025-06, *without* overrides)
|isin|issuer_name|str_001_s|str_002_ec|str_003_ec|str_004_asec|str_005_ec|cs_001_sec|cs_002_ec|str_006_sec|str_007_sect|art_8_basicos|permid|str_003b_ec|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|XS1842065010|SG Issuer SA|OK|OK|OK|OK|OK|OK|OK|OK|OK|OK|5036159086|OK|
|…|…|…|…|…|…|…|…|…|…|…|…|…|…|

### 1.6 Override database `overrides`
|clarityid|permid|aladdin_id|ovr_target|ovr_value|
|:---|:---|:---|:---|:---|
|18621|4295894740|375|str_002_ec|EXCLUDED|
| |4295895363|2800|str_005_ec|OK|
|…|…|…|…|…|

---

## 2.  Preparation steps  

1. **Column normalisation** – rename columns, force upper-case values in strategy flags.  
2. **Issuer-level views** – explode security rows to issuer view for BRS data (`*_issuerlevel`).  
3. **Key joins**  
   * Add `aladdin_id` to Clarity via `crossreference`.  
   * Split Clarity into *previous* and *current* vintages.  
4. **`prepare_dataframes()`** –  
   * Sets a common index (`permid` or `aladdin_id`).  
   * Returns “old”, “new”, plus *new_issuers* and *out_issuers* helper sets.

### 2.1 Prepared Clarity (old) `prep_old_clarity_df`
*(first 10 rows identical to the raw example but now with `aladdin_id`, index =`permid` and renamed columns – omitted for brevity)*  

### 2.2 Prepared Clarity (new) `prep_new_clarity_df`
*(same structure, 73 456 rows)*  

### 2.3 Prepared BRS portfolio issuer view `prep_brs_df_ptf`
|issuer_name|security_description|portfolio_full_name|portfolio_id|str_001_s|str_002_ec|str_003b_ec|str_003_ec|str_004_asec|…|
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
|2I RETE GAS SPA|2I RETE GAS SPA|02.018.59973438020.0|PFC00659|OK|OK|OK|OK|EXCLUDED|…|
|3I GROUP PLC|3I GROUP PLC|SMIO Max 30 % Income Eq.|OMM3V018|OK|OK|OK|OK|OK|…|
|…|…|…|…|…|…|…|…|…|…|

*(equivalent `prep_brs_df_bmk` for benchmarks not repeated here)*  

---

## 3.  Delta generation  

The helper `generate_delta()` compares any two aligned data-sets column-by-column:  

```text
new_flag ≠ old_flag
    → mark PERMID / aladdin_id as changed
segregate:
    – new_exclusion (flag turned to EXCLUDED)
    – new_inclusion (flag turned to OK or FLAG)
append list of changed column names
```

Three delta families are produced:  

|Delta|Old vs New|Index|Rows (excl)|Rows (incl)|
|:---|:---|:---|---:|---:|
|`delta_clarity`|Clarity 2025-05 ↔ 2025-06|permid|1 519|1 219|
|`delta_brs_ptf`|BRS portfolio ↔ Clarity-new|aladdin_id| 256| 525|
|`delta_brs_bmks`|Benchmark ↔ Clarity-new|aladdin_id| 757|1 665|

### 3.1 Example – `delta_ex_clarity` (first 10 rows)
|permid|issuer_name|…|exclusion_list|
|:---|:---|:---|:---|
|4296785143|Credit Suisse Ag London Branch|…|['str_001_s', 'str_004_asec', …]|
|…|…|…|…|

### 3.2 Example – `delta_ex_ptf`
|aladdin_id|isin|issuer_name|…|exclusion_list|
|:---|:---|:---|:---|:---|
|F70532|XS1979288328|ABB Finance BV|…|['str_001_s', 'str_004_asec', …]|
|…|…|…|…|…|

*(analogous tables exist for the other four deltas)*  

---

## 4.  Enrichment, filtering & cleaning  

For each delta dataframe the pipeline:

1. **Add override info** – `ovr_list` (dict of manual flags).  
2. **Attach portfolio / benchmark strings** (`affected_*_str`) from prepared dicts.  
3. **Filter empty lists** (`filter_empty_lists`).  
4. **Remove unmatched rows** (`filter_rows_with_common_elements`).  
5. **Clean list columns** (different helpers for inclusion/exclusion).  
6. **Drop rows whose cleaned exclusion list became empty.**  
7. **Standardise column order.**  

### Output after cleaning  

Six *final* sheets are saved:

|Excel sheet|DataFrame|Rows|
|:---|:---|---:|
|`excl_carteras`|`portfolio_deltas_exclusion_df`|**1 253 → 33***|
|`excl_benchmarks`|`benchmark_deltas_exclusion_df`|33|
|`excl_clarity`|`clarity_deltas_exclusion_df`|1 519|
|`incl_clarity`|`clarity_deltas_inclusion_df`|1 219|
|`incl_carteras`|`portfolio_deltas_inclusion_df`|52|
|`incl_benchmarks`|`benchmark_deltas_inclusion_df`|?? (log truncated)|

\*rows drop sharply because only issuers whose exclusion list overlaps at least one invested portfolio survive the filters.  

#### 4.1 `portfolio_deltas_exclusion_df` (top 10)
|aladdin_id|permid|issuer_name|exclusion_list|ovr_list|affected_portfolio_str|affected_benchmark_str|
|:---|:---|:---|:---|:---|:---|:---|
|R48907|4295903388|American Tower Corp|['str_004_asec','str_007_sect']| |[(FIG00677,str_007_sect)…]|[(ML1-3EULC,[…]), …]|
|059456|4295889577|Banco Bilbao Vizcaya Argentaria SA|['str_001_s', 'str_002_ec', 'str_003b_ec']| |[(CPE00035,str_003b_ec)…]|[(ML1-3EULC,[…]), …]|
|…|…|…|…|…|…|…|

#### 4.2 `portfolio_deltas_inclusion_df` (top 10)
|aladdin_id|permid|issuer_name|inclusion_list|ovr_list|affected_portfolio_str|affected_benchmark_str|
|:---|:---|:---|:---|:---|:---|:---|
|R44750|5000005309|A2A SpA|['str_007_sect']| |[(FIG00677,str_007_sect)…]|[(ML1-3EULC,[…]), …]|
|E97445|5000757618|ABN Amro Bank NV|['str_007_sect']|{'str_004_asec':'OK'}|[(FIG00677,str_007_sect)…]|[(ML1-3EULC,[…]), …]|
|…|…|…|…|…|…|…|

*(analogous views for clarity/benchmark deltas not repeated)*  

---

## 5.  Strategy-level view (`--simple` flag)

`process_data_by_strategy()` pivots every delta to one sheet **per ESG strategy column** (e.g. `str_004_asec`).  
For each strategy it reports *new*, *old*, *BRS* and *override* status plus the list of portfolios/benchmarks invested.

Example – **str_004_asec exclusions (portfolio view)**  

|aladdin_id|permid|issuer_name|str_004_asec_new|str_004_asec_old|str_004_asec_brs|str_004_asec_ovr|affected_portfolio_str|
|:---|:---|:---|:---|:---|:---|:---|:---|
|R48907|4295903388|American Tower Corp|EXCLUDED|OK|OK| |[(FIG00677,str_007_sect)…]|
|J71326|5066585518|Logicor Financing SARL|EXCLUDED|EXCLUDED|OK| |[(FIG00677,str_007_sect)…]|
|…|…|…|…|…|…|…|…|

---

## 6.  Saving  

* All six cleaned deltas exported to **`YYYYMM_pre_ovr_analysis.xlsx`**.  
* Each strategy sheet (when `--simple`) exported individually.  

---

## 7.  Re-implementing in PySpark (outline)

```python
# 0. Spark session
spark = (
    SparkSession.builder.appName("pre_ovr_analysis")
    .config("spark.sql.caseSensitive", "false")
    .getOrCreate()
)

# 1. Load raw CSV / XLSX to Spark DataFrames
brs_portf    = spark.read.format("com.crealytics.spark.excel")…load(path_portf)
brs_bmk      = spark.read.format("com.crealytics.spark.excel")…load(path_bmk)
crossref     = spark.read.csv(path_crossref, header=True)
clarity_prev = spark.read.csv(path_prev,  header=True)
clarity_curr = spark.read.csv(path_curr,  header=True)
overrides    = spark.read.format("com.crealytics.spark.excel")…load(path_ovr)

# 2. Helpers --------------------------------------------------------------
upper = F.udf(lambda s: F.lit(None) if s is None else s.upper().strip())
strategy_cols = ["str_001_s","str_002_ec",…]   # 11 + 2 SCS

def normalise(df):
    for c in strategy_cols:
        df = df.withColumn(c, upper(F.col(c)))
    return df

# 3. Prepare issuer-level views ------------------------------------------
issuer_view = (
    brs_portf
    .groupBy("aladdin_id")
    .agg(F.first("issuer_name").alias("issuer_name"), *[
        F.max(F.col(c)).alias(c) for c in strategy_cols     # pick any non-null
    ])
)

# 4. Join Aladdin id to Clarity ------------------------------------------
clarity_prev = (
    clarity_prev.join(crossref.select("permid","aladdin_id"), "permid", "left")
)
clarity_curr = (
    clarity_curr.join(crossref.select("permid","aladdin_id"), "permid", "left")
)

# 5. Delta generator ------------------------------------------------------
def delta(old_df, new_df, key):
    join_expr = old_df[key] == new_df[key]
    joined = old_df.alias("o").join(new_df.alias("n"), join_expr, "inner")
    diff = None
    for c in strategy_cols:
        cond = F.col(f"n.{c}") != F.col(f"o.{c}")
        sel  = joined.where(cond).select(
            F.col(f"n.{key}").alias(key),
            F.col(f"n.{c}").alias(f"{c}_new"),
            F.col(f"o.{c}").alias(f"{c}_old")
        )
        diff = sel if diff is None else diff.unionByName(sel)
    return diff

delta_clarity   = delta(clarity_prev, clarity_curr, "permid")
delta_portfolio = delta(issuer_view, clarity_curr, "aladdin_id")
# …

# 6. Tag inclusion / exclusion -------------------------------------------
def with_flag(df, flag_col):
    incr  = df.where(F.array_contains("exclusion_list", "EXCLUDED"))
    decr  = df.where(F.array_contains("inclusion_list", "OK") |
                     F.array_contains("inclusion_list", "FLAG"))
    return incr, decr

# 7. Attach override & portfolio strings ----------------------------------
# broadcast small dicts and use UDF map look-ups

# 8. Filtering & cleaning identical to pandas version ---------------------
# leverage higher-order functions on arrays in Spark SQL

# 9. Save to Excel (or Parquet) -------------------------------------------
delta_clarity.write.format("excel").option("dataAddress","excl_clarity").save(...)
# -------------------------------------------------------------------------
```

Key points:

* **`normalise()`** replicates upper-casing/strip.  
* **`delta()`** mirrors `generate_delta()` but keeps only changed rows.  
* Portfolio / benchmark enrichment can be done by broadcasting small lookup DataFrames.  
* List-column utilities (`filter_empty_lists`, cleaning functions) map naturally to Spark SQL `transform`, `filter`, `size`, `arrays_overlap`.  

With these steps the same six output sheets – plus optional strategy pivots – can be regenerated entirely in PySpark.
