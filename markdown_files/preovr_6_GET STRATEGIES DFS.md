# 6. GET STRATEGIES DFS
## Overview
The **`process_data_by_strategy`** function converts a “delta” table (a set of security-level changes) into a **suite of per-strategy DataFrames** that are:

1. **Scoped** to only the rows relevant to each strategy (via an *exclusion / inclusion* list in the source).
2. **Enriched** with reference data drawn from:
   * “old” Clarity data (`df1_lookup_source`)
   * “new” Clarity data (`df2_lookup_source`)
   * BRS issuer-level data (`brs_lookup_source`)
   * bespoke *override* rules (`overrides_df`)
3. **Tidied** by moving a “what-portfolio/benchmark is affected” column to the far right.
4. **Cleaned** via a call to an external helper, `remove_matching_rows`, that strips duplicate or unwanted lines.

The result is returned as a **dict** `{strategy_name: processed_dataframe}` ready for downstream analytics.

---

## Key Inputs

| Argument | Purpose |
|----------|---------|
| `input_delta_df` | The delta table you want to explode by strategy. |
| `strategies_list` | List of column “roots” (e.g. `["stratA", "stratB"]`). The function will look for columns `stratA_new`, `stratA_old`, … and so on. |
| `input_df_exclusion_col` | Name of the column that stores a *list* of strategies associated with each row (e.g. `"exclusion_list"` or `"inclusion_list"`). |
| `df1_lookup_source` / `df2_lookup_source` | Old vs. new Clarity snapshots keyed by `permid`. |
| `brs_lookup_source` | BRS issuer data keyed by `aladdin_id`. |
| `overrides_df` | Table of manual overrides: one permid/strategy row per patch. |
| `affected_portfolio_col_name` | Column whose position is forced to the end (defaults to `"affected_portfolio_str"`). |
| `logger` | Optional `logging.Logger`; if `None`, the caller is expected to set this up. |

---

## Step-by-Step Logic

### **Part 1 – Seed an empty DataFrame per strategy**

For every `strategy_name_key` in `strategies_list`:

1. **Column template.** Build a canonical ordered list:

   ```
   [aladdin_id, permid, issuer_name,
    {strategy}_new, {strategy}_old, {strategy}_brs, {strategy}_ovr,
    affected_portfolio_col_name]
   ```

2. **Row filter.** Iterate through `input_delta_df`; for any line whose
   `input_df_exclusion_col` *contains* the strategy name, pull just those template
   columns into a dict and append to `rows`.

3. **Initialise.** Always create a DataFrame—even if `rows` is empty—so every
   strategy key exists in `str_dfs_dict`.

---

### **Part 2 – Build trimmed lookup tables**

* Extract the unique `permid` and `aladdin_id` values present in the delta.
* For each of the three lookup sources, keep **only** the matching rows and
  ensure they are indexed by the appropriate key column.
* **`prefix_func`** renames any strategy-root column shared with the delta
  by appending a suffix (`_old`, `_new`, `_brs`) so later merge logic is trivial.

---

### **Part 3 – Enrich each strategy DataFrame**

For each non-empty temp DataFrame in `str_dfs_dict`:

1. Pre-create nullable columns `{strategy}_new/old/brs/ovr`.
2. Loop over its rows:
   * Fill **`_old`** from `permid_to_df1_lookup`.
   * Fill **`_new`** from `permid_to_df2_lookup`.
   * Fill **`_brs`** from `aladdin_to_brs_lookup`.
   * Fill **`_ovr`** from the first matching row in `overrides_df`
     (same `permid` *and* same strategy).
3. Re-order columns so *affected_portfolio/benchmark* is last.

---

### **Part 4 – Final clean-up**

* Emit copious logging for troubleshooting.
* Run **`remove_matching_rows`** (implementation not shown) which presumably
  deletes rows where “old”, “new”, and “brs” values are identical, or applies
  some bespoke deduping rule.
* Store the cleaned DataFrame back into `str_dfs_dict`.

---

### **Part 5 – Return**

`str_dfs_dict` is returned to the caller.

---

## How the Function Is Consumed (excerpt below the definition)

The calling code prepares **six configurations** that differ by:

* **Delta slice** (`delta_ex_ptf`, `delta_in_bmk`, …)
* **Which column holds the strategy list** (`"exclusion_list"` vs `"inclusion_list"`)
* **Whether we care about portfolios or benchmarks** (changes `affected_portfolio_col_name`)
* **Which pre-processed BRS table to hand in**

It then loops over the configs:

```python
results_str_level_dfs[var_name] = process_data_by_strategy(
    input_delta_df=config["input_df"],
    strategies_list=delta_test_cols,
    input_df_exclusion_col=config["exclusion_col"],
    df1_lookup_source=prep_old_clarity_df,
    df2_lookup_source=prep_new_clarity_df,
    brs_lookup_source=config["brs_source"],
    overrides_df=overrides,
    affected_portfolio_col_name=config["affected_col"],
    logger=logger,
)
```

Each entry in `results_str_level_dfs` is thus **a dictionary of strategy-level
DataFrames** for one specific analytic slice, letting the caller compare
“old vs new vs BRS vs override” at an issuer level without wrestling with joins.

---

## Takeaways

* **Separation of concerns:** Raw delta → per-strategy slice → enrich → clean.
* **Suffix-based column naming** avoids dynamic joins; every lookup table
  already matches the target column names.
* **Override precedence:** Manual overrides trump all automated lookups because
  `_ovr` is always present and can be prioritised later.
* **Extensive logging** makes the pipeline transparent but can be silenced by
  downgrading the logger’s level.

All told, the snippet is a reusable “strategy skeleton” that turns a single
issuer-level delta file into multiple, self-contained, analysis-ready tables.