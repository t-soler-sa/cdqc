# clarity_data_quality_control_functions.py

"""

Module to define functions & constans to carry Clarity.ai's data quality control & analysis

"""

import logging
from pathlib import Path
from typing import List, Tuple, Union
from itertools import chain
from collections import defaultdict
import re

import numpy as np
import pandas as pd

# Module-level logger
logger = logging.getLogger(__name__)

# DEFINE CONSTANTS

delta_test_cols = [
    "str_001_s",
    "str_002_ec",
    "str_003_ec",
    "str_003b_ec",
    "str_004_asec",
    "str_005_ec",
    "str_006_sec",
    "str_007_sect",
    "str_sfdr8_aec",
    "scs_001_sec",
    "scs_002_ec",
]

brs_test_cols = ["aladdin_id"] + delta_test_cols


# DEFINE FUNCTIONS
def prepare_dataframes(
    base_df: pd.DataFrame, new_df: pd.DataFrame, target_index: str = "permid"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prepare DataFrames by setting the index and filtering for common indexes.
    Logs info about common, new, and missing indexes.
    """
    # Set index to 'permid' if it exists, otherwise assume it's already the index.
    logger.info(f"Setting index to {target_index}.")
    if target_index in base_df.columns:
        base_df = base_df.set_index(target_index)
    else:
        logger.warning("df1 does not contain a 'permid' column. Using current index.")

    if target_index in new_df.columns:
        new_df = new_df.set_index(target_index)
    else:
        logger.warning("df2 does not contain a 'permid' column. Using current index.")

    common_indexes = base_df.index.intersection(new_df.index)
    new_indexes = new_df.index.difference(base_df.index)
    missing_indexes = base_df.index.difference(new_df.index)

    logger.info(f"Number of common indexes: {len(common_indexes)}")

    return (
        base_df.loc[common_indexes],
        new_df.loc[common_indexes],
        new_df.loc[new_indexes],
        base_df.loc[missing_indexes],
    )


def finalize_delta(
    delta: pd.DataFrame,
    test_col: List[str] = delta_test_cols,
    target_index: str = "permid",
) -> pd.DataFrame:
    """Finalize the delta DataFrame by removing unchanged rows and resetting the index."""
    delta = delta.dropna(subset=test_col, how="all")
    # Make a copy to avoid SettingWithCopyWarning
    delta = delta.copy()
    delta.reset_index(inplace=True)
    delta.loc[:, target_index] = delta[target_index].astype(str)
    logger.info(f"Final delta shape: {delta.shape}")
    return delta


def filter_and_drop(
    df: pd.DataFrame, filter_col: str, drop_cols: List[str]
) -> pd.DataFrame:
    """
    Filters a DataFrame based on a boolean column and drops specified columns.

    Parameters:
    - df (pd.DataFrame): The DataFrame to filter and modify.
    - filter_col (str): The name of the boolean column to filter on.
    - drop_cols (List[str]): A list of column names to drop.
    - logger: Logger object for logging messages.

    Returns:
    - pd.DataFrame: A filtered and reduced copy of the original DataFrame.

    Raises:
    - KeyError: If specified columns are not found in the DataFrame.
    - Exception: For any other unexpected errors during execution.
    """
    try:
        logger.info(f"Filtering DataFrame using column: '{filter_col}'")

        # We drop first unnecesary columns
        missing_cols = [col for col in drop_cols if col not in df.columns]
        if missing_cols:
            logger.info(f"columns {missing_cols} not in df")
            drop_cols = [col for col in drop_cols if col in df.columns]
            logger.info(f"Dropping instead columns: {drop_cols}")
        else:
            drop_cols = drop_cols
            logger.info(f"Dropping columns: {drop_cols}")

        logger.info(f"Dropping: {drop_cols}")
        filtered_df = df.drop(columns=drop_cols).copy()

        # We then filter only the relevant rows based on the filtering column
        if filter_col not in df.columns:
            raise KeyError(f"Filter column '{filter_col}' not found in DataFrame.")
        else:
            logger.info(f"Filtering DataFrame where '{filter_col}' is True")
            filtered_df = filtered_df[filtered_df[filter_col]].copy()
            logger.info(
                f"Filtered {len(filtered_df)} rows where '{filter_col}' is True"
            )
            logger.info(f"Let's drop filter_col column: {filter_col}")
            filtered_df.drop(columns=[filter_col], inplace=True)
        return filtered_df

    except Exception as e:
        e.add_note("Error in filter_and_drop")
        logger.error(f"{e}")
        raise


def generate_delta(
    df1: pd.DataFrame,  # old_df that you get from othe function prepare_dataframes
    df2: pd.DataFrame,  # new_df that you get from othe function prepare_dataframes
    test_col: List[str] = delta_test_cols,
    condition_list: List[str] = [],  # either ["EXCLUDED"] or ["OK", "FLAG"]
    delta_analysis_str: str = "",  # either "exclusion" or "inclusion"
    get_inc_excl: bool = True,  # if False, skip step 2 and return after comparison
    delta_name_str: str = "delta",  # name of the delta DataFrame
    target_index: str = "permid",  # index to be used for the DataFrame
    filter_col: str = "",
    drop_cols: List[str] = [],  # columns to be dropped after filtering
) -> pd.DataFrame:
    """
    Generate a delta DataFrame highlighting differences between two input DataFrames,
    with an optional analysis to detect value transitions based on specified conditions.

    This function performs the following:
    1. Compares specified columns between `df1` and `df2` and marks changes in a copy of `df2` by replacing
       unchanged values with NaN.
    2. Optionally, identifies changes where values in `df1` did not meet a specified condition and
       changed in `df2` to meet the condition, marking these rows and logging the count of such transitions.
    3. Appends a column listing all columns where such condition-based transitions occurred.

    Parameters:
        df1 (pd.DataFrame): The original DataFrame used as a baseline for comparison. These dataframes are the output of the function prepare_dataframes()
        df2 (pd.DataFrame): The new DataFrame to compare against the baseline. These dataframes are the output of the function prepare_dataframes()
        test_col (List[str]): List of column names to be tested for changes and condition transitions.
        suffix_level (str): A suffix appended to the generated list column name for disambiguation (e.g., "_brs").
        condition_list (List[str]): List of string values representing the target conditions
            (e.g., ["EXCLUDED"]) used to detect value transitions.
        delta_analysis_str (str): String label used to name the analysis, such as "exclusion" or "inclusion".
        get_inc_excl (bool): Flag indicating whether to perform condition-based transition analysis.
            If False, only the delta comparison is returned.

    Returns:
        pd.DataFrame: A modified copy of `df2` where unchanged values are replaced with NaN in specified columns,
        and additional columns indicate new condition-based changes and lists of columns where such changes occurred.

    Raises:
        ValueError: If `get_inc_excl` is True but `condition_list` is empty.
    """

    # Step 1: Compare DataFrames and create a delta DataFrame
    delta = df2.copy()
    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Comparing column: {col}")
            # Create a mask for differences between the two DataFrames
            diff_mask = df1[col] != df2[col]
            # Update the delta DataFrame with the differences
            delta.loc[~diff_mask, col] = np.nan

    # Return early if inclusion/exclusion analysis is not required
    if not get_inc_excl:
        return finalize_delta(delta, test_col, target_index)

    # Step 2: Check for condition-based changes (e.g., new exclusions/inclusions)
    logger.info(f"Generating {delta_analysis_str} for {delta_name_str} analysis.")
    if not condition_list and get_inc_excl:
        logger.warning(
            "Condition list is empty. No exclusions will be checked. Provide a list with at least an item like 'EXCLUSION'."
        )
        raise ValueError("Condition list is empty.")

    delta_column_name = f"new_{delta_analysis_str}"
    logger.info(f"Checking for new {delta_analysis_str}")
    delta[delta_column_name] = False

    # Normalize condition list once
    normalized_conditions = [cond.strip().upper() for cond in condition_list]

    for col in test_col:
        if col in df1.columns and col in df2.columns:
            logger.info(f"Checking for new {delta_analysis_str} in column: {col}")
            mask = (
                ~df1[col]
                .astype(str)
                .str.strip()
                .str.upper()
                .isin(normalized_conditions)
            ) & (
                df2[col].astype(str).str.strip().str.upper().isin(normalized_conditions)
            )
            delta.loc[mask, delta_column_name] = True
            logger.info(f"Number of new {delta_analysis_str}s in {col}: {mask.sum()}")

    # Auxiliary function to get the list of changed columns
    def get_delta_list(row: pd.Series) -> List[str]:
        """Get list of columns that changed to value in the condition_list."""
        return [
            col
            for col in test_col
            if col in df1.columns
            and col in df2.columns
            and str(df2.loc[row.name, col]).strip().upper() in normalized_conditions
            and str(df1.loc[row.name, col]).strip().upper() not in normalized_conditions
        ]

    delta[f"{delta_analysis_str}_list"] = delta.apply(
        lambda row: get_delta_list(row), axis=1
    )

    final_df = finalize_delta(delta, test_col, target_index)

    # Step 3: Filter and drop specified columns
    logger.info(f"Filtering DataFrame using column: '{filter_col}'")
    filtered_delta = filter_and_drop(final_df, filter_col, drop_cols)
    return filtered_delta


def create_override_dict(
    df: pd.DataFrame = None,
    id_col: str = "aladdin_id",
    str_col: str = "ovr_target",
    ovr_col: str = "ovr_value",
):
    """
    Converts the overrides DataFrame to a dictionary.
    Args:
        df (pd.DataFrame): DataFrame containing the overrides.
        id_col (str): Column name for the identifier.
        str_col (str): Column name for the strategy.
        ovr_col (str): Column name for the override value.
    Returns:
        dict: Dictionary of overrides.
    """
    # 1. Groupd the df by issuer_id
    grouped = df.groupby(id_col)

    # 2. Initialise the dictionary
    ovr_dict = {}

    # 3. Iterate over each group (issuer id and its corresponding rows)
    for id, group_data in grouped:
        # 3.1. for each issuer id create a dict pairing the strategy and the override value
        ovr_result = dict(zip(group_data[str_col], group_data[ovr_col]))
        # 3.2. add the dict to the main dict
        ovr_dict[id] = ovr_result

    return ovr_dict


def add_portfolio_benchmark_info_to_df(
    portfolio_dict, delta_df, column_name="affected_portfolio_str"
):

    # Initialize a defaultdict to accumulate (portfolio_id, strategy_name) pairs
    aladdin_to_info = defaultdict(list)

    for portfolio_id, data in portfolio_dict.items():
        strategy = data.get("strategy_name")
        for a_id in data.get("aladdin_id", []):
            aladdin_to_info[a_id].append((portfolio_id, strategy))

    # Map each aladdin_id in delta_df to a list of accumulated portfolio info
    delta_df[column_name] = delta_df["aladdin_id"].apply(
        lambda x: list(chain.from_iterable(aladdin_to_info.get(x, [])))
    )

    return delta_df


def get_issuer_level_df(df: pd.DataFrame, idx_name: str) -> pd.DataFrame:
    """
    Removes duplicates based on idx_name, and drops rows where idx_name column contains
    NaN, None, or strings like "nan", "NaN", "none", or empty strings.

    Args:
        df (p   d.DataFrame): Input dataframe.
        idx_name (str): Column name used for duplicate removal and NaN filtering.

    Returns:
        pd.DataFrame: Cleaned dataframe.
    """
    # Drop duplicates
    df_cleaned = df.drop_duplicates(subset=[idx_name])

    # Drop rows where idx_name is NaN/None or has invalid strings
    valid_rows = df_cleaned[idx_name].notnull() & (
        ~df_cleaned[idx_name]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(["nan", "none", ""])
    )

    return df_cleaned[valid_rows]


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


def reorder_columns(df: pd.DataFrame, keep_first: list[str], exclude: list[str] = None):
    if exclude is None:
        exclude = set()
    return df[
        keep_first
        + [col for col in df.columns if col not in keep_first and col not in exclude]
    ]


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
    row["affected_portfolio_str"] = cleaned_paired

    # Extract strategies from the cleaned paired tuples
    affected_strategies = {strategy for _, strategy in cleaned_paired}

    # Update exclusion_list
    row["exclusion_list"] = [
        strategy for strategy in exclusion_list if strategy in affected_strategies
    ]

    return row


def clean_empty_exclusion_rows(df, target_col: str = "exclusion_list"):

    # Drop rows where exclusion_list is empty after cleaning
    return df[df[target_col].apply(lambda x: isinstance(x, list) and len(x) > 0)]


# DEFINE FUNCTIONS TO CREATE STRATEGY-LEVEL DATAFRAMES & CLEAN THEM
# This function is used to remove rows from the Strategy Level DataFrame where the values in three specific columns match.
def remove_matching_rows(
    df, logger: logging.Logger = None
) -> Tuple[pd.DataFrame, bool]:
    # Identify columns with specific suffixes
    cols_new = [col for col in df.columns if col.endswith("_new")]
    cols_old = [col for col in df.columns if col.endswith("_old")]
    cols_brs = [col for col in df.columns if col.endswith("_brs")]
    cols_ovr = [col for col in df.columns if col.endswith("_ovr")]

    # Assuming there's only one set of each column type based on your example
    if len(cols_old) != 1 or len(cols_brs) != 1 or len(cols_ovr) != 1:
        logger.warning(
            "Expected exactly one column each for '_new '_old', '_brs', '_ovr'"
        )
        return df, False  # Return the original DataFrame if the assumption is violated

    col_old, col_brs, col_ovr = cols_old[0], cols_brs[0], cols_ovr[0]

    # Filter rows where all three column values match
    df_filtered = df[
        ~(df[col_old] == df[col_brs]) | ~(df[col_old] == df[col_ovr])
    ].copy()

    return df_filtered, True


def process_data_by_strategy(
    input_delta_df: pd.DataFrame,
    strategies_list: list,
    input_df_exclusion_col: str,
    # --- Dataframes with data for old, new, and overrides values ---
    df1_lookup_source: pd.DataFrame,
    df2_lookup_source: pd.DataFrame,
    brs_lookup_source: pd.DataFrame,
    overrides_df: pd.DataFrame,
    affected_portfolio_col_name: str = "affected_portfolio_str",  # change to "affected_benchmark_str" if needed
    # --- Key column names in the strategy-specific DataFrames being built ---
    strategy_df_permid_col: str = "permid",  # Must be in initial_cols_to_extract
    strategy_df_aladdin_id_col: str = "aladdin_id",  # Must be in initial_cols_to_extract
    # --- Key column names in the source lookup DataFrames ---
    df1_source_key_col: str = "permid",
    df2_source_key_col: str = "permid",
    brs_source_key_col: str = "aladdin_id",
    # --- Column names for override logic ---
    overrides_permid_col: str = "permid",
    overrides_target_col: str = "ovr_target",
    overrides_value_col: str = "ovr_value",
    logger: logging.Logger = None,
) -> dict:
    """
    Processes an input DataFrame to create separate, enriched DataFrames for each specified strategy.

    Args:
        input_delta_df: The main Delta DataFrame to process (e.g., delta_brs for exclusions on portfolio,
                                 or dlt_inc_brs, and dlt_inc_benchmarks inclusions).
        strategies_list: A list of strategy names to iterate over (e.g., delta_test_cols).
        input_df_exclusion_col: Column name in input_delta_df that contains lists/iterables
                                 of strategies for exclusion or inclusion criteria (e.g. "exclusion_list" or "inclusion_list").
        df1_lookup_source: DataFrame with previous Clarity Data (e.g., df_1).
        df2_lookup_source: DataFrame with latest Clarity Data (e.g., df_1).
        brs_lookup_source: DataFrame for the BRS lookup (e.g., brs_carteras_issuerlevel).
        overrides_df: DataFrame for override lookups.
        affected_portfolio_col_name: Name of the column to move to the end of each
                                     strategy DataFrame.
        strategy_df_permid_col: Column name in strategy DataFrames used as key for
                                df1_lookup_source and overrides_df.
        strategy_df_aladdin_id_col: Column name in strategy DataFrames used as key for
                                    brs_lookup_source.
        df1_source_key_col: Key column name in df1_lookup_source to index on.
        brs_source_key_col: Key column name in brs_lookup_source to index on.
        overrides_permid_col: PermID column name in overrides_df.
        overrides_target_col: Target strategy column name in overrides_df.
        overrides_value_col: Value column name in overrides_df for the override.

    Returns:
        A dictionary where keys are strategy names and values are the processed
        DataFrames for each strategy.
    """

    cols_to_extract = [
        "aladdin_id",
        "permid",
        "issuer_name",
        affected_portfolio_col_name,
    ]

    logger.info(
        "Starting to process to generate exclusion & inclusion analysis at the strategies."
    )
    str_dfs_dict = {}

    # Part 1: Iterate over strategies to build initial DataFrames from input_df
    logger.info(
        "Starting to process to generate exclusion & inclusion analysis at the strategies."
    )
    for strategy_name_key in strategies_list:
        rows = []
        target_col_new = f"{strategy_name_key}_new"
        target_col_old = f"{strategy_name_key}_old"
        target_col_brs = f"{strategy_name_key}_brs"
        target_col_ovr = f"{strategy_name_key}_ovr"

        # Explicitly define standard columns for each strategy
        required_cols = [
            "aladdin_id",
            "permid",
            "issuer_name",
            target_col_new,
            target_col_old,
            target_col_brs,
            target_col_ovr,
            affected_portfolio_col_name,
        ]

        for _, row in input_delta_df.iterrows():
            # Check if the exclusion column exists and its value is iterable and contains the strategy
            if (
                input_df_exclusion_col in row
                and hasattr(row[input_df_exclusion_col], "__contains__")
                and strategy_name_key in row[input_df_exclusion_col]
            ):
                extracted_row_data = {col: row.get(col, pd.NA) for col in required_cols}
                rows.append(extracted_row_data)

        # Always initialise Dataframe with required columns
        str_dfs_dict[strategy_name_key] = pd.DataFrame(rows, columns=required_cols)
        logger.info(
            f"Head of DataFrame for strategy {strategy_name_key}: \n{str_dfs_dict[strategy_name_key].head()}\n\n"
        )

    # Part 2: Prepare lookup DataFrames - create indexed copies to avoid modifying originals
    # get list uniques permid & aladdin_ids from input_delta_df
    try:
        target_permid_list = input_delta_df["permid"].dropna().unique().tolist()
        target_aladdin_id_list = input_delta_df["aladdin_id"].dropna().unique().tolist()
    except KeyError as e:
        logger.error(f"Missing expected column in input_delta_df: {e}")
        raise

    # We will loop through the lookup DataFrames to create new DataFrames using only the target permids & issuer_ids
    lookup_configs = [
        {
            "source": df1_lookup_source,
            "key_col": df1_source_key_col,
            "target_list": target_permid_list,
            "output_name": "permid_to_df1_lookup",
        },
        {
            "source": df2_lookup_source,
            "key_col": df2_source_key_col,
            "target_list": target_permid_list,
            "output_name": "permid_to_df2_lookup",
        },
        {
            "source": brs_lookup_source,
            "key_col": brs_source_key_col,
            "target_list": target_aladdin_id_list,
            "output_name": "aladdin_to_brs_lookup",
        },
    ]

    # Dictionary to collect output DataFrames
    lookup_results = {}

    for config in lookup_configs:
        source = config["source"]
        key_col = config["key_col"]
        target_list = config["target_list"]
        output_name = config["output_name"]

        try:
            # Validate key_col
            if key_col not in source.columns and source.index.name != key_col:
                error_message = (
                    f"'{key_col}' is neither a column nor the index in DataFrame for {output_name}. "
                    f"Available columns: {list(source.columns)}, Index name: {source.index.name}"
                )
                logger.error(error_message)
                raise KeyError(error_message)

            if source.index.name != key_col:
                logger.info(
                    f"Setting index for {output_name} using key_col '{key_col}'."
                )
                df = (
                    source[source[key_col].isin(target_list)]
                    .copy()
                    .set_index(key_col, drop=False)
                )
            else:
                df = source.loc[source.index.isin(target_list)].copy()

            lookup_results[output_name] = df
            logger.info(
                f"{output_name} has {lookup_results[output_name].shape[0]} rows"
            )

        except KeyError as e:
            logger.error(f"Key error for {output_name}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {output_name}: {e}", exc_info=True)

    # def  prefix func
    def prefix_func(
        df: pd.DataFrame, suffix: str, target_columns: List[str] = strategies_list
    ) -> pd.DataFrame:
        common_cols = set(df.columns).intersection(target_columns)
        new_column_names = [
            f"{col}_{suffix}" if col in common_cols else col for col in df.columns
        ]
        return df.rename(columns=dict(zip(df.columns, new_column_names)))

    # Unpack variables
    permid_to_df1_lookup = prefix_func(
        lookup_results["permid_to_df1_lookup"], suffix="old"
    )
    permid_to_df2_lookup = prefix_func(
        lookup_results["permid_to_df2_lookup"], suffix="new"
    )
    aladdin_to_brs_lookup = prefix_func(
        lookup_results["aladdin_to_brs_lookup"], suffix="brs"
    )
    # delete lookup_results to free up memory
    del lookup_results

    # Part 3: Iterate through the newly created strategy DataFrames to add/enrich columns
    logger.info("Adding new columns to strategy DataFrames.")
    for strategy_name, temp_df in str_dfs_dict.items():
        if temp_df.empty:
            logger.info(f"Df for {strategy_name} is empty")
            continue  # Skip processing for strategies with no initial data

        # Define derived column names based on the strategy_name (as seen in snippet)
        # These columns will be added to the temp_df for the current strategy
        target_col_new = f"{strategy_name}_new"
        target_col_old = f"{strategy_name}_old"
        target_col_brs = f"{strategy_name}_brs"
        target_col_ovr = f"{strategy_name}_ovr"

        # Initialize these new columns in the strategy-specific DataFrame
        # Using pd.NA for better compatibility with pandas dtypes, allows nullable integers etc.
        temp_df[target_col_new] = pd.NA
        temp_df[target_col_old] = pd.NA
        temp_df[target_col_brs] = pd.NA
        temp_df[target_col_ovr] = pd.NA

        # Ensure columns are of object type if they might hold mixed types or pd.NA
        # This is important if the source data for these columns is not uniformly numeric.
        temp_df = temp_df.astype(
            {target_col_old: object, target_col_brs: object, target_col_ovr: object}
        )

        for i, df_row in temp_df.iterrows():
            permid_val = df_row.get(strategy_df_permid_col)
            aladdin_id_val = df_row.get(strategy_df_aladdin_id_col)

            # Lookup from df1_lookup_source
            if pd.notna(permid_val) and permid_val in permid_to_df1_lookup.index:
                # The source column name in df1_lookup_source is assumed to match target_col_old
                if target_col_old in permid_to_df1_lookup.columns:
                    temp_df.at[i, target_col_old] = permid_to_df1_lookup.at[
                        permid_val, target_col_old
                    ]

            # Lookup from df2_lookup_source
            if pd.notna(permid_val) and permid_val in permid_to_df2_lookup.index:
                # The source column name in df2_lookup_source is assumed to match target_col_new
                if target_col_new in permid_to_df2_lookup.columns:
                    temp_df.at[i, target_col_new] = permid_to_df2_lookup.at[
                        permid_val, target_col_new
                    ]

            # Lookup from brs_lookup_source
            if (
                pd.notna(aladdin_id_val)
                and aladdin_id_val in aladdin_to_brs_lookup.index
            ):
                # The source column name in brs_lookup_source is assumed to match target_col_brs
                if target_col_brs in aladdin_to_brs_lookup.columns:
                    temp_df.at[i, target_col_brs] = aladdin_to_brs_lookup.at[
                        aladdin_id_val, target_col_brs
                    ]

            # Lookup from overrides_df
            if pd.notna(permid_val):
                # Filter overrides_df for matching permid and strategy_name
                match_override = overrides_df[
                    (overrides_df[overrides_permid_col] == permid_val)
                    & (overrides_df[overrides_target_col] == strategy_name)
                ]
                if not match_override.empty:
                    # Assign the first matched override value
                    temp_df.at[i, target_col_ovr] = match_override[
                        overrides_value_col
                    ].values[0]

        # Part 4: Move the 'affected_portfolio_col_name' to the end of the DataFrame
        logger.info(
            f"Moving {affected_portfolio_col_name} column to the end of the DataFrame."
        )
        if affected_portfolio_col_name in temp_df.columns:
            cols = [
                col for col in temp_df.columns if col != affected_portfolio_col_name
            ] + [affected_portfolio_col_name]
            temp_df = temp_df[cols]

        str_dfs_dict[strategy_name] = (
            temp_df  # Update dictionary with the processed DataFrame
        )
        logger.info(
            f"Df {strategy_name} has {str_dfs_dict[strategy_name].shape[0]} rows"
        )
    logger.info(
        "Finished processing to generate exclusion & inclusion analysis at the strategies."
    )

    # Part 5: Clean up the DataFrames in str_dfs_dict
    # remove matchin rows from strategies dataframes
    logger.info("\n\n==============DEBUGGING CHECK ===============\n\n")
    for df_name, df in str_dfs_dict.items():
        logger.info(f"{df_name}'s COLUMNS: \n{df.columns.tolist()}")
        logger.info(f"{df_name} has {df.shape[0]} rows")

    logger.info("Removing matching rows from strategy DataFrames.")
    for df_name, df in str_dfs_dict.items():
        filtered_df, was_filtered = remove_matching_rows(df, logger=logger)
        str_dfs_dict[df_name] = filtered_df
        logger.info(
            f"{df_name} has {str_dfs_dict[df_name].shape[0]} rows after being filtered"
        )
        if not was_filtered:
            logger.warning(
                f"For DataFrame '{df_name}', remove_matching_rows returned the original DataFrame (no rows filtered)."
            )

    return str_dfs_dict


def get_strategy_file(
    date_prefix: str = "default", directory: Union[str, Path] = Path.cwd()
) -> Tuple[Path, str]:
    """
    Return the most recent *strategy benchmark* file that matches *date_prefix*.

    The directory is expected to contain files named

        ``YYYYMMDD_strategies_snt world_portf_bmks.xlsx``

    Parameters
    ----------
    date_prefix : str, optional
        * ``"default"`` – return the latest date available.
        * ``YYYY``      – a four-digit year.
        * ``YYYYMM``    – a year+month.
        * ``YYYYMMDD``  – a full date.
        Anything else raises ``ValueError``.
    directory : str | pathlib.Path, optional
        Location that holds the Excel files (defaults to the current working
        directory). It may be a ``Path`` or any value accepted by
        ``pathlib.Path``.

    Returns
    -------
    path : pathlib.Path
        Absolute path of the chosen file.
    date_str : str
        The 8-digit date (``YYYYMMDD``) extracted from the filename.

    Raises
    ------
    FileNotFoundError
        No file in *directory* satisfies the criteria.
    ValueError
        *date_prefix* is neither ``"default"`` nor a 4/6/8-digit string.

    """

    # Input sanitisation
    directory = Path(directory).expanduser().resolve()  # normalise path
    date_prefix = date_prefix.strip().lower()  # remove blanks, case insensitive

    if date_prefix != "default" and not re.fullmatch(
        r"\d{4}(\d{2}(\d{2})?)?", date_prefix
    ):
        raise ValueError(
            "date_prefix must be 'default' or a 4, 6, or 8 digit string "
            "(YYYY | YYYYMM | YYYYMMDD)"
        )

    # Enumerate files and keep those that satisfy the pattern
    pattern = re.compile(
        r"(?P<date>\d{8})_strategies_snt world_portf_bmks\.xlsx$", re.IGNORECASE
    )

    candidates: List[Tuple[str, Path]] = []
    for file in directory.iterdir():
        if not file.is_file():  # ignore sub-directories
            continue
        match = pattern.match(file.name)
        if match:
            date_str = match.group("date")
            if date_prefix == "default" or date_str.startswith(date_prefix):
                candidates.append((date_str, file))

    # Pick the newest file (lexicographic order works because the format is YYYYMMDD)
    if not candidates:
        raise FileNotFoundError(
            f"No files found in '{directory}' matching prefix '{date_prefix}'."
        )

    date_str, path = max(candidates, key=lambda t: t[0])
    return path, date_str
