import numpy as np
import pandas as pd


def process_dataframe(df):
    # Convert esg_score to numeric, replacing any non-numeric values with NaN
    df["esg_score"] = pd.to_numeric(df["esg_score"], errors="coerce")

    # Function to check if all values are the same (ignoring NaN)
    def all_same(s):
        return s.dropna().nunique() <= 1

    # Function to get the most common value (ignoring NaN)
    def most_common(s):
        return s.mode().iloc[0] if not s.empty else np.nan

    # Replace '-' with issuer_name in parent_company column
    df["parent_company"] = np.where(
        df["parent_company"] == "-", df["issuer_name"], df["parent_company"]
    )

    # Group by 'parent_company' and check if all 'esg_score' values are the same
    grouped = df.groupby("parent_company")
    all_same_mask = grouped["esg_score"].transform(all_same)

    # Filter the dataframe to get only the groups with different values
    diff_groups = df[~all_same_mask].copy()

    if diff_groups.empty:
        print("All groups have consistent 'esg_score' values.")
        return pd.DataFrame()

    # Calculate statistics for each group
    group_stats = grouped.agg(
        {
            "esg_score": ["count", "min", "max", "mean", "median", most_common],
            "permid": "first",  # To get a sample permid for the group
        }
    )
    group_stats.columns = [
        "count",
        "min_score",
        "max_score",
        "mean_score",
        "median_score",
        "most_common_score",
        "sample_permid",
    ]
    group_stats = group_stats.reset_index()

    # Merge the group stats back to the original dataframe
    result = pd.merge(df, group_stats, on="parent_company", suffixes=("", "_group"))

    # Sort the result by parent_company and esg_score
    result = result.sort_values(["parent_company", "esg_score"])

    return result


def main():
    df = pd.read_csv(
        r"C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ovr\202410_df_issuer_level_with_ovr.csv",
        usecols=["permid", "parent_company", "esg_score", "issuer_name", "isin"],
        dtype={
            "permid": str,
            "parent_company": str,
            "issuer_name": str,
            "esg_score": str,
        },
    )

    result = process_dataframe(df)

    if not result.empty:
        # Filter to show only groups with discrepancies
        discrepancy_groups = result[result["count"] > 1]

        # Group the results by parent_company
        grouped_results = discrepancy_groups.groupby("parent_company")

        for name, group in grouped_results:
            if (
                group["esg_score"].nunique() > 1
            ):  # Only print groups with actual discrepancies
                print(f"\nParent Company: {name}")
                print(f"Group Size: {group['count'].iloc[0]}")
                print(
                    f"ESG Score Range: {group['min_score'].iloc[0]} - {group['max_score'].iloc[0]}"
                )
                print(f"Mean ESG Score: {group['mean_score'].iloc[0]:.2f}")
                print(f"Median ESG Score: {group['median_score'].iloc[0]:.2f}")
                print(f"Most Common ESG Score: {group['most_common_score'].iloc[0]}")
                print("\nDetailed Group Information:")
                print(
                    group[["issuer_name", "permid", "esg_score", "isin"]].to_string(
                        index=False
                    )
                )
                print("\n" + "=" * 50)
    else:
        print("No discrepancies found in the data.")


if __name__ == "__main__":
    main()
