# 5. GET STRATEGIES DFS

This snippet is part of a **data-cleaning and filtering pipeline** that takes several *delta* DataFrames (differences between two datasets) and prepares them for further analysis or export. Each step is numbered **5.1 – 5.5** in the comments and log messages.


# Data journey—from raw → clean

Below is a **conceptual before-and-after** of what happens to every
*delta* DataFrame as it moves through the pipeline, ignoring all logging
and orchestration details.

---

## 1 · Starting point

Each delta arrives as a Pandas **DataFrame** that typically contains:

* A **“main list” column** (`config["main_parameter"]`)  
  – e.g. `affected_portfolio_str` or `affected_benchmark_str`.

* One or more **helper list columns**  
  * `exclusion_list` – strategies that must be excluded  
  * `inclusion_list` – strategies that must be kept  
  * Both may be *empty*, *nested*, or contain items later overridden.

* **`ovr_list`** (dict per row) – optional overrides; keys are strategy
  codes, values are status flags like `"OK"`, `"FLAG"`, `"EXCLUDED"`.

* For portfolio deltas, `affected_portfolio_str` is an **even-length
  list** that actually stores consecutive `(id, strategy)` entries.

The raw data therefore may suffer from:

* Empty lists that add no information.  
* Nested lists that complicate comparisons.  
* Conflicting overrides (`ovr_list`) that contradict inclusion/exclusion
  choices.  
* Entire rows whose list columns share **no overlap** with the main list,
  making them irrelevant noise.

---

## 2 · Transformations (what really changes)

| Step | What it does to the **data** | Why it matters |
|------|------------------------------|----------------|
| **Filter empty main lists** | Drop any row whose main list column is empty. | Nothing to compare → no analytical value. |
| **Keep only rows with overlap** | For each row, check if the helper list (`exclusion_list` or `inclusion_list`) shares **at least one element** with the main list; if not, drop it. Nested lists in `affected_benchmark_str` are flattened first. | Removes rows unrelated to the affected portfolio or benchmark. |
| **Apply overrides to `exclusion_list`** | Inside each row, delete codes whose `ovr_list` flag is `"OK"` or `"FLAG"`. | Those codes are no longer truly excluded. |
| **Apply overrides to `inclusion_list`** | Remove items whose `ovr_list` flag is `"EXCLUDED"`; if a row’s inclusion list becomes empty, drop the row. | Ensures inclusion really means “not excluded.” |
| **Synchronise portfolio + exclusion (portfolio deltas only)** | In `affected_portfolio_str`, pair consecutive elements into `(id, strategy)` tuples; throw away tuples whose strategy is **not** in `exclusion_list`; then trim `exclusion_list` to the surviving strategies. | Guarantees that the exclusion list matches what is actually present in the portfolio field. |
| **Drop rows with empty `exclusion_list`** | If, after all cleaning, an exclusion list is empty, remove the row entirely. | A row that excludes nothing is irrelevant for an exclusion delta. |
| **Re-order columns** | Arrange columns into a predefined schema. | Uniform structure for downstream joins / exports. |

---

## 3 · End state

After these passes every DataFrame now satisfies:

* **No empty list columns**: `main_parameter`, `exclusion_list`, and
  `inclusion_list` (when present) all contain at least one element.

* **Overrides honoured**: Any item contradicted by `ovr_list`
  (`"OK"/"FLAG"` for exclusions, `"EXCLUDED"` for inclusions) has been
  removed.

* **Row relevance enforced**:  
  – Each exclusion-row’s `exclusion_list` intersects the main list.  
  – Each inclusion-row’s `inclusion_list` intersects the main list.

* **Portfolio/exclusion consistency** (portfolio deltas only):
  * `affected_portfolio_str` is a list of `(id, strategy)` tuples,
    filtered so that every tuple’s `strategy` is present in
    `exclusion_list`.  
  * `exclusion_list` now holds **exactly** those strategies.

* **Uniform column order** ready for export or further analytics.

In short, the pipeline turns loosely structured,
potentially contradictory list data into a **tight, consistent, and
fully aligned dataset**—only the rows and codes that genuinely matter
for the delta remain. 

The code can be divided into two layers:

1. **Reusable helper functions** (`filter_empty_lists`,  
   `filter_rows_with_common_elements`, `clean_exclusion_list_with_ovr`,
   `clean_inclusion_list`, `clean_portfolio_and_exclusion_list`,
   `clean_empty_exclusion_rows`).

2. **Driver loop** that iterates over a configuration list
   (`prep_config`) and applies those helpers to every DataFrame stored in
   `config["dfs_dict"]`, writing progress to a `logger`.

---

## 1. Helper functions

| Step | Function | Purpose | Key Details |
|------|----------|---------|-------------|
| **5.1** | `filter_empty_lists(df, column)` | Keep only rows whose `column` **is a non-empty list**. | Uses `apply` + `lambda` to test *listness* and length. |
| **5.2.1.a / b** | `filter_rows_with_common_elements(df, col1, col2)` | Retain rows where the lists in `col1` and `col2` **share at least one common element**. | *Special case*: If `col2 == "affected_benchmark_str"`, first “flatten” any nested lists. Builds a Boolean mask with set intersection. |
| **5.2.2.a** | `clean_exclusion_list_with_ovr(df, exclusion_list_col="exclusion_list")` | Remove from each `exclusion_list` any code whose override status in `ovr_list` is `"OK"` or `"FLAG"`. | Treats missing or malformed `ovr_list` values safely. |
| **5.2.2.b** | `clean_inclusion_list(df)` | • For every row, drop items from `inclusion_list` whose override in `ovr_list` is `"EXCLUDED"`.<br>• Drop entire rows whose `inclusion_list` ends up empty. | Handles NumPy arrays, non-list values, malformed `ovr_list`. |
| **5.3** | `clean_portfolio_and_exclusion_list(row, …)` | **Row-wise**: 1️⃣ pair consecutive items in `affected_portfolio_str` into `(id, strategy)` tuples, 2️⃣ keep only those tuples whose strategy appears in `exclusion_list`, 3️⃣ shrink `exclusion_list` to that same set of strategies. | Ensures portfolio / exclusion lists stay consistent. |
| **5.4** | `clean_empty_exclusion_rows(df, target_col="exclusion_list")` | Drop rows whose `exclusion_list` is empty after previous cleaning. | Simple filter with `apply`. |

---

### 2. Sequence applied to each DataFrame

1. **5.1** `filter_empty_lists` – remove rows whose main list column is empty.

2. **5.2**  
   *If the DataFrame is named* **`"exclusion_df"`**  
   &nbsp;  a. **5.2.1.a** `filter_rows_with_common_elements` on `exclusion_list` vs main column  
   &nbsp;  b. **5.2.2.a** `clean_exclusion_list_with_ovr`  
   *Else* (all other delta tables)  
   &nbsp;  a. **5.2.1.b** `filter_rows_with_common_elements` on `inclusion_list` vs main column  
   &nbsp;  b. **5.2.2.b** `clean_inclusion_list`

3. **5.3** *Only for the configuration named* **`"portfolio_deltas"`**  
   Apply `clean_portfolio_and_exclusion_list` row-wise to `"exclusion_df"`.

4. **5.4** `clean_empty_exclusion_rows` – drop rows whose `exclusion_list`
   became empty (only on `"exclusion_df"`).

5. **5.5** `reorder_columns` – reorder columns into a final schema
   (`id_name_issuers_cols` + `delta_test_cols`).

After all configs are processed, the cleaned DataFrames are saved into
`final_dfs_dict` under keys like  
`"{prep_config_name}_{df_name}"`.

---

## 3. Logging & Transparency

Extensive `logger.info` calls precede and follow each operation,
recording:

* DataFrame name (e.g. *`exclusion_df`*).
* Step description (e.g. *“Cleaning inclusion list”*).
* Row counts *before* and *after* every transformation.

This makes the pipeline auditable and easier to debug.

---

## 4. Why these steps matter

* **Data integrity** – All list columns are guaranteed to contain valid,
  non-empty lists, free from conflicting overrides.
* **Consistency** – Portfolio/exclusion pairs are synchronised, so later
  logic does not hit mismatches.
* **Focus** – Only rows that genuinely relate to at least one affected
  portfolio/benchmark survive, reducing noise for downstream analytics.


## Relevant Code Below

```python

...

# 5.1. filtering not empty lists
def filter_empty_lists(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Returns a DataFrame filtered so that rows where the specified column contains
    an empty list are removed. Keeps rows where the column has a list with at least one element.

    Parameters:
    - df (pd.DataFrame): The input DataFrame
    - column (str): The name of the column to check

    Returns:
    - pd.DataFrame: Filtered DataFrame
    """
    return df[df[column].apply(lambda x: isinstance(x, list) and len(x) > 0)]

# 5.2.1.a filter rows with common elements in the exclusion list & clean exclusion list
def filter_rows_with_common_elements(df, col1, col2):
    """
    Return rows of df where the lists in col1 and col2 have at least one common element.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        col1 (str): The name of the first column containing lists.
        col2 (str): The name of the second column containing lists.

    Returns:
        pd.DataFrame: A DataFrame filtered to include only rows where col1 and col2 have a common element.
    """

    # if col2 = "affected_benchmark_str" flatten list first
    def flatten_mixed_list(val):
        if isinstance(val, list):
            flat = []
            for item in val:
                if isinstance(item, list):
                    flat.extend(item)
                else:
                    flat.append(item)
            return flat
        return val

    if col2 == "affected_benchmark_str":
        df.loc[:, col2] = df[col2].apply(flatten_mixed_list)

    logger.info(f"Filtering rows with common elements in columns: {col1} and {col2}")
    mask = df.apply(lambda row: bool(set(row[col1]) & set(row[col2])), axis=1)
    return df[mask].copy()


# 5.2.2.a Clean exclusion data
def clean_exclusion_list_with_ovr(df, exclusion_list_col: str = "exclusion_list"):
    # Define helper function with safety check
    def filter_exclusions(row):
        ovr_dict = row["ovr_list"] if isinstance(row["ovr_list"], dict) else {}
        return [
            code
            for code in row[exclusion_list_col]
            if ovr_dict.get(code) not in {"OK", "FLAG"}
        ]

    # Apply filtering safely
    df[exclusion_list_col] = df.apply(filter_exclusions, axis=1)

    return df

# 5.2.2.b Clean inclusion data
def clean_inclusion_list(df):
    """
    Processes each row of df:
    1. For each element in 'inclusion_list', if the element is a key in 'ovr_list' and
       its value is 'EXCLUDED', remove the element.
    2. Rows where 'inclusion_list' is empty (or becomes empty after filtering) are dropped.
    """

    def process_row(row):
        inc_list = row.get("inclusion_list", [])
        ovr_list = row.get("ovr_list", {})

        # If inc_list is a NumPy array, convert it to a list.
        if isinstance(inc_list, np.ndarray):
            inc_list = inc_list.tolist()

        # If inc_list is not a list (or array), then treat it as empty.
        if not isinstance(inc_list, list):
            return []

        # For ovr_list: if it's not a dict, then treat it as an empty dict.
        if not isinstance(ovr_list, dict):
            ovr_list = {}

        # Filter out items that are in ovr_list and marked as 'EXCLUDED'
        return [
            item
            for item in inc_list
            if item not in ovr_list or ovr_list[item] != "EXCLUDED"
        ]

    # Apply the row-wise processing.
    df.loc[:, "inclusion_list"] = df.apply(process_row, axis=1)

    # Drop rows where 'inclusion_list' is empty.
    df = df[df["inclusion_list"].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    return df

# 5.3 apply clean portfolio and exclusion list
def clean_portfolio_and_exclusion_list(
    row,
    affected_col_name="affected_portfolio_str",
    exclusion_list_name="exclusion_list",
):
    """
    First pairs elements in 'affected_portfolio_str' and filters them based
    on 'exclusion_list'. Then cleans 'exclusion_list' based on the
    filtered results.
    """

    def pair_elements(input_list):
        """
        Pairs consecutive elements in a list into tuples.
        Parameters: -> input_list : list
        -----------
        Returns: -> list_of_tuples : list. A list where consecutive elements are paired as tuples.
        --------
        Raises TypeError: If the input is not a list and ValueError: If the list does not have an even number of elements.
        """
        if not isinstance(input_list, list):
            raise TypeError("Expected a list as input.")
        if len(input_list) % 2 != 0:
            raise ValueError("The list must have an even number of elements.")

        return [
            (input_list[i], input_list[i + 1]) for i in range(0, len(input_list), 2)
        ]

    raw_list = row[affected_col_name]
    exclusion_list = row[exclusion_list_name]

    # Pair elements first
    paired = pair_elements(raw_list)

    # Filter tuples based on exclusion list
    cleaned_paired = [tup for tup in paired if tup[1] in exclusion_list]

    # Update affected_portfolio_str
    row[affected_col_name] = cleaned_paired

    # Extract strategies from the cleaned paired tuples
    affected_strategies = {strategy for _, strategy in cleaned_paired}

    # Update exclusion_list
    row[exclusion_list_name] = [
        strategy for strategy in exclusion_list if strategy in affected_strategies
    ]

    return row


# 5.4. clean empty exclusion lists
def clean_empty_exclusion_rows(df, target_col: str = "exclusion_list"):

    # Drop rows where exclusion_list is empty after cleaning
    return df[df[target_col].apply(lambda x: isinstance(x, list) and len(x) > 0)]

...

# 5.    FILTERING & CLEANING DELTAS
    logger.info("\n\n\n5. FILTERING & CLEANING DELTAS\n\n\n")

    for config in prep_config:
        # 5. Filtering BRS Delta's based on affected portfolio & benchmark
        logger.info(
            "\n5. Filtering & Cleaning BRS Delta's based on affected portfolio & benchmark\n"
        )
        if config["brs_data"]:

            for df_name, df in config["dfs_dict"].items():
                logger.info(
                    f"\n5.1 Filtering Empty {config["main_parameter"]} for {config["prep_config_name"]}'s {df_name}.\n"
                )
                # 5.1. filtering not empty lists
                logger.info(
                    f"""
                    {df_name} had {df.shape[0]} before applying filter_empty_lists() func to empty {config["main_parameter"]}.
                    """
                )
                df = filter_empty_lists(df, config["main_parameter"])
                config["dfs_dict"][df_name] = df
                logger.info(
                    f"""
                    {df_name} has {df.shape[0]} after applying filter_empty_lists() func to empty {config["main_parameter"]}.
                    """
                )

        # 5.2.a filter rows with common elements
        if config["brs_data"]:
            for df_name, df in config["dfs_dict"].items():
                if df_name == "exclusion_df":
                    # 5.2.1.a filter rows with common elements in the exclusion list & clean exclusion list
                    logger.info(
                        f"""
                        \n5.2.1.a Filtering rows with common elements using filter_rows_with_common_elements() func for {config["prep_config_name"]}'s {df_name}.
                        {df_name} had {df.shape[0]} ROWS before applying filter_rows_with_common_elements() func to empty {config["main_parameter"]}.
                        """
                    )
                    df = filter_rows_with_common_elements(
                        df, "exclusion_list", config["main_parameter"]
                    )
                    logger.info(
                        f"""
                        {df_name} has {df.shape[0]} ROWS after applying filter_rows_with_common_elements() func to empty {config["main_parameter"]}.
                        """
                    )
                    # 5.2.2.a Clean exclusion data
                    logger.info(
                        f"""
                        \n5.2.2.a Cleaning exclusion list for overrides OK. 
                        {df_name} had {df.shape[0]} rows BEFORE applying clean_exclusion_list_with_ovr() func.
                        """
                    )
                    df = clean_exclusion_list_with_ovr(df)
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows AFTER applying clean_exclusion_list_with_ovr() func."
                    )

                else:
                    # 5.2.b filter rows with common elements in the inclusion list & clean inclusion list
                    # 5.2.1.b filter rows with common elements in the exclusion list & clean exclusion list
                    logger.info(
                        f"""
                        \n5.2.1.b Filter rows with common elements in the inclusion list & clean inclusion list. 
                        {df_name} had {config["dfs_dict"][df_name].shape[0]} rows BEFORE applying filter_rows_with_common_elements() func.
                        """
                    )

                    df = filter_rows_with_common_elements(
                        df, "inclusion_list", config["main_parameter"]
                    )
                    # 5.2.2.b Clean inclusion data
                    logger.info(
                        f"""
                        \n5.2.2.b Cleaning inclusion list. 
                        {df_name} had {config["dfs_dict"][df_name].shape[0]} rows BEFORE applying clean_inclusion_list() func.
                        """
                    )
                    df = clean_inclusion_list(df)
                    config["dfs_dict"][df_name] = df
                    logger.info(
                        f"{df_name} has {config["dfs_dict"][df_name].shape[0]} rows AFTER applying clean_inclusion_list() func.\n"
                    )

        # 5.3 apply clean portfolio and exclusion list
        if config["prep_config_name"] == "portfolio_deltas":
            logger.info(
                "\n5.3 Cleaning portfolio and exclusion lists for BRS Portfolio Delta"
            )
            prep_config_name = config["prep_config_name"]
            for df_name, df in config["dfs_dict"].items():
                if df_name == "exclusion_df":

                    # cleant portfolio and exclusion list
                    logger.info(
                        f"""
                        {prep_config_name}'s {df_name} had {df.shape[0]} rows BEFORE applying clean_portfolio_and_exclusion_list() func.
                        """
                    )
                    df = df.apply(clean_portfolio_and_exclusion_list, axis=1)

                    logger.info(
                        f"""
                        {prep_config_name}'s {df_name} had {df.shape[0]} rows AFTER applying clean_portfolio_and_exclusion_list() func.
                        """
                    )
                    config["dfs_dict"][df_name] = df

        # 5.4. clean empty exclusion lists
        for df_name, df in config["dfs_dict"].items():
            prep_config_name = config["prep_config_name"]
            # remove rows with empyt exclusion lists
            if df_name == "exclusion_df":
                logger.info(
                    f"""
                            \n5.4. Cleaning empty exclusion lists for {prep_config_name}'s {df_name}.
                            {prep_config_name}'s {df_name} had {df.shape[0]} rows BEFORE applying clean_empty_exclusion_rows() func.
                            """
                )
                df = clean_empty_exclusion_rows(df)
                config["dfs_dict"][df_name] = df
                logger.info(
                    f"""
                            {prep_config_name}'s {df_name} had {df.shape[0]} rows AFTER applying clean_empty_exclusion_rows() func.
                            """
                )

        # 5.5. reoder columns for all the deltas
        logger.info("\n6. Reordering columns for all the deltas")
        for df_name, df in config["dfs_dict"].items():
            logger.info(
                f"Reordering columns for {config["prep_config_name"]}'s {df_name}"
            )
            df = reorder_columns(df, id_name_issuers_cols, delta_test_cols)
            config["dfs_dict"][df_name] = df

    # persist cleaned DataFrames
    for config in prep_config:
        for df_name, df in config["dfs_dict"].items():
            final_key = f"{config['prep_config_name']}_{df_name}"
            final_dfs_dict[final_key] = df

```
