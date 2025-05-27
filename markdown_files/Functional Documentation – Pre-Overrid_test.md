# Functional Documentation – Pre-Override Analysis & Override DB Maintenance

## Table of Contents
- [1 Input Data](#1-input-data)
  - [1.1 Datasets](#11-datasets)
  - [1.2 Business Description](#12-business-description)
  - [1.3 Dataset Structures](#13-dataset-structures)
- [2 Processes](#2-processes)
  - [2.1 Pre-Override Analysis (`pre-ovr-analysis.py`)](#21-pre-override-analysis-pre-ovr-analysispy)
    - [2.1.1 High-level Summary](#211-high-level-summary)
    - [2.1.2 Step-by-Step Logic](#212-step-by-step-logic)
    - [2.1.3 Key Helper Functions](#213-key-helper-functions)
  - [2.2 Override DB Active-Flag Updater (`update_ovr_db_active_col.py`)](#22-override-db-active-flag-updater-update_ovr_db_active_colpy)
    - [2.2.1 High-level Summary](#221-high-level-summary)
    - [2.2.2 Step-by-Step Logic](#222-step-by-step-logic)
- [3 Output Data](#3-output-data)
  - [3.1 Excel Workbooks](#31-excel-workbooks)
  - [3.2 Business Description of Outputs](#32-business-description-of-outputs)
  - [3.3 Output Structures](#33-output-structures)

---

## 1 Input Data<a name="1-input-data"></a>

### 1.1 Datasets<a name="11-datasets"></a>

| Acronym / Name | Origin | Typical Format | Script(s) Using It |
| -------------- | ------ | -------------- | ------------------ |
| **Clarity Feed** | Third-party ESG data provider | `.xlsx` / Parquet with one row per *ISIN* and ESG signal columns | Both |
| **BRS/Aladdin Portfolios** | BlackRock Aladdin (internal) | `.xlsx` sheet *portfolio_carteras* | Pre-OVR |
| **BRS/Aladdin Benchmarks** | BlackRock Aladdin (internal) | `.xlsx` sheet *portfolio_benchmarks* | Pre-OVR |
| **Cross-reference** | Internal mapping | `.xlsx` with `permid` → `aladdin_id` | Both |
| **Overrides DB** | ESG team database | `.xlsx` with override rules | Both |
| **Committee File** | SRI committee | `.xlsx` list of strategies & owners | Pre-OVR |

Optional:

| Dataset | Purpose |
| ------- | ------- |
| **Previous Clarity Snapshot** | Delta vs current feed |
| **Zombie Analysis Inputs** | Full holdings vs Clarity coverage |

### 1.2 Business Description<a name="12-business-description"></a>

* **Clarity feed** – authoritative universe of issuer-level ESG signals (e.g. `str_001_s`, `str_006_sec`, `art_8_basicos`).  
* **Portfolios / Benchmarks** – positions held in client portfolios and their strategic reference indices. Used to scope material ESG changes.  
* **Overrides** – manual adjustments that supersede Clarity values. Each row: one issuer, one target field (`ovr_target`), the manual value (`ovr_value`), and a Boolean flag (`ovr_active`).  
* **Cross-reference** – normalises identifiers (PERMID ↔ Aladdin ID) so all sources align.  

### 1.3 Dataset Structures<a name="13-dataset-structures"></a>

Common identifier columns:

| Column | Type | Notes |
| ------ | ---- | ----- |
| `permid` | string | Primary key for Clarity |
| `aladdin_id` | string | Primary key for BRS |
| `isin` | string | Only used during ingestion |
| `issuer_name` | string | Display only |

ESG signal columns are **categorical strings**: `"OK"`, `"FLAG"`, `"EXCLUDED"` (or blank).

Overrides add:

| Column | Type | Meaning |
| ------ | ---- | ------- |
| `ovr_target` | categorical | Name of Clarity column overridden |
| `ovr_value` | same type as target | Value that should replace feed |
| `ovr_active` | bool | `True` if still valid |
| `df_value` | same type as target | Latest value in Clarity (for comparison) |

---

## 2 Processes<a name="2-processes"></a>

### 2.1 Pre-Override Analysis (`pre-ovr-analysis.py`)<a name="21-pre-override-analysis-pre-ovr-analysispy"></a>

#### 2.1.1 High-level Summary<a name="211-high-level-summary"></a>
The script compares the **current** Clarity delivery with:
1. the **previous** Clarity snapshot,
2. holdings in BRS portfolios,
3. holdings in BRS benchmarks, and
4. the **Overrides DB**

to surface material ESG changes **before** override logic is executed.  
Results are written to a dated Excel workbook and, optionally, a set of simplified strategy-level files and a *Zombie* report (holdings in BRS but missing in Clarity).

#### 2.1.2 Step-by-Step Logic<a name="212-step-by-step-logic"></a>

1. **Configuration & Argument Parsing**  
   * Reads a central config (`get_config`) and CLI flags `--simple`, `--zombie`, and `--date`.  
2. **Data Loading**  
   * Loads Clarity (`load_clarity_data`), BRS holdings (`load_aladdin_data`), overrides (`load_overrides`), etc.  
3. **Data Preparation**  
   * Normalises string case, trims whitespace, renames legacy column names, merges `aladdin_id` from the cross-reference.  
   * Converts portfolios & benchmarks to *issuer level* (`get_issuer_level_df`).  
4. **Delta Generation** (`generate_delta`)  
   * Three delta groups are built:  
     * **`delta_clarity`** – old vs new Clarity (by `permid`)  
     * **`delta_brs_ptf`** – BRS portfolios vs new Clarity (by `aladdin_id`)  
     * **`delta_brs_bmks`** – BRS benchmarks vs new Clarity (by `aladdin_id`)  
   * Each group splits into **inclusions** (`OK`, `FLAG`) and **exclusions** (`EXCLUDED`).  
5. **Enrichment with Overrides & Coverage Context**  
   * Builds a fast-lookup dictionary `ovr_dict` per `aladdin_id`.  
   * Adds *affected portfolio* / *benchmark* strings from mapping dictionaries.  
6. **Filtering & Cleaning**  
   * Drops rows without affected portfolios/benchmarks, removes empty exclusion lists, synchronises list-type columns, and re-orders columns (`reorder_columns`).  
7. **Strategy-Level Aggregation** (`process_data_by_strategy`) – *optional (`--simple`)*  
   * Re-pivots deltas so each ESG strategy (e.g. `str_004_asec`) gets its own sheet summarising affected portfolios, overrides, etc.  
8. **Zombie Analysis** – *optional (`--zombie`)*  
   * Calls `zombie_killer` to list issuers present in BRS but absent from Clarity.  
9. **Output**  
   * Writes one master workbook `YYYYMMDD_pre_ovr_analysis.xlsx` plus, if requested, one workbook per strategy and zombie output.  

#### 2.1.3 Key Helper Functions<a name="213-key-helper-functions"></a>

| Function | Role |
| -------- | ---- |
| `prepare_dataframes` | Aligns two dataframes, flags new / missing issuers |
| `generate_delta` | Column-wise comparison; emits inclusion/exclusion deltas |
| `create_override_dict` | `{aladdin_id: [ovr_targets...]}` mapping |
| `add_portfolio_benchmark_info_to_df` | Injects list columns `affected_portfolio_str` / `affected_benchmark_str` |
| `filter_empty_lists` / `filter_rows_with_common_elements` | Removes irrelevant rows |
| `clean_inclusion_list`, `clean_exclusion_list_with_ovr`, `clean_empty_exclusion_rows` | Tidies list columns & resolves override conflicts |
| `reorder_columns` | Puts identifiers first, ESG columns next |
| `process_data_by_strategy` | Builds strategy-specific summaries |

*(These live in **`clarity_data_quality_control_functions.py`** and are reused across tooling.)*

---

### 2.2 Override DB Active-Flag Updater (`update_ovr_db_active_col.py`)<a name="22-override-db-active-flag-updater-update_ovr_db_active_colpy"></a>

#### 2.2.1 High-level Summary<a name="221-high-level-summary"></a>
Keeps the Overrides DB truthful by de-activating records whose manual value **matches** the latest Clarity value. This prevents redundant overrides and maintains auditability.

#### 2.2.2 Step-by-Step Logic<a name="222-step-by-step-logic"></a>

1. **Load Inputs** – latest Clarity snapshot, Overrides DB (all rows), cross-reference.  
2. **Cross-Reference Join** – ensures both dataframes share `aladdin_id`.  
3. **Conflict Check** – `find_conflicting_columns` flags duplicate `(aladdin_id, ovr_target)` pairs with different `ovr_value`.  
4. **Populate `df_value`** – `update_df_value_column` melts Clarity wide→long and fills Overrides.`df_value`.  
5. **Set `ovr_active`** – `update_override_active` sets `False` where `ovr_value == df_value`.  
6. **Outputs**  
   * **`overrides_db_beta.xlsx`** – updated master file.  
   * **`overrides_db_backup/YYYYMM_override_db.xlsx`** – immutable backup.  
   * **`YYYYMMDD_YYYYMM_deactivated_overrides.xlsx`** – rows just de-activated.  

---

## 3 Output Data<a name="3-output-data"></a>

### 3.1 Excel Workbooks<a name="31-excel-workbooks"></a>

| Workbook | Sheets | Produced By |
| -------- | ------ | ----------- |
| `YYYYMMDD_pre_ovr_analysis.xlsx` | *delta_ex_* & *delta_in_* sheets for Clarity, Portfolios, Benchmarks | Pre-OVR |
| `YYYYMMDD_strategy_level_preovr_analysis_*.xlsx` | One sheet per investment strategy | Pre-OVR (`--simple`) |
| `YYYYMMDD_zombie_analysis.xlsx` | Holdings in BRS but not in Clarity | Pre-OVR (`--zombie`) |
| `overrides_db_beta.xlsx` | Complete Overrides DB (updated) | OVR Updater |
| `deactivated_overrides.xlsx` | Newly inactivated overrides | OVR Updater |

### 3.2 Business Description of Outputs<a name="32-business-description-of-outputs"></a>

* **Exclusion delta** – issuers whose status changed **to or from** `EXCLUDED`.  
* **Inclusion delta** – issuers now `OK`/`FLAG` vs before.  
* **Zombie analysis** – data-gaps requiring supplier escalation.  
* **Strategy sheets** – stakeholder-friendly view, mapping ESG policies to impacted portfolios/benchmarks.  
* **Overrides DB** – authoritative manual adjustments; `ovr_active = False` means automatic Clarity value is valid.

### 3.3 Output Structures<a name="33-output-structures"></a>

Minimal column set for all delta sheets (order after `reorder_columns`):

| Identifier Block | ESG Signal Block | Enrichment Block |
| ---------------- | ---------------- | ---------------- |
| `aladdin_id` • `permid` • `issuer_name` | `str_001_s` … `str_007_sect`, `str_sfdr8_aec`, `scs_001_sec`, `scs_002_ec` | `inclusion_list` / `exclusion_list` • `ovr_list` • `affected_portfolio_str` • `affected_benchmark_str` |

---
