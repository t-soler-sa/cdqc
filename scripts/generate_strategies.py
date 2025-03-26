import json
import pandas as pd
from typing import Dict, Any, Union, List


class ESGStrategyProcessor:
    def __init__(self, strategy_file: str, metrics_mapping_file: str):
        """
        Initialize the ESG Strategy Processor with strategy and metrics mapping files.

        Args:
            strategy_file (str): Path to the JSON file containing strategy definitions
            metrics_mapping_file (str): Path to the CSV file containing metrics SK mapping
        """
        self.strategies = self._load_strategy_file(strategy_file)
        self.metrics_mapping = self._load_metrics_mapping(metrics_mapping_file)

    def _load_strategy_file(self, file_path: str) -> Dict:
        """Load and validate the strategy JSON file."""
        with open(file_path, "r") as f:
            strategies = json.load(f)
        return strategies["strategies"]

    def _load_metrics_mapping(self, file_path: str) -> Dict:
        """Load metrics mapping from CSV file."""
        mapping_df = pd.read_csv(file_path)
        return dict(zip(mapping_df["metric_sk"], mapping_df["metric_name"]))

    def _filter_metrics(
        self, df: pd.DataFrame, metrics_affecting: List[int]
    ) -> pd.DataFrame:
        """Filter dataframe to include only relevant metrics for the strategy."""
        metric_names = [
            self.metrics_mapping[sk]
            for sk in metrics_affecting
            if sk in self.metrics_mapping
        ]
        return df[["company_id"] + metric_names]

    def _evaluate_condition(self, value: Any, rule: Dict) -> bool:
        """Evaluate if a value meets a condition based on the specified rules."""
        if pd.isna(value):
            return False

        metric_type = rule["type"]
        condition = rule["condition"]
        threshold = rule["threshold"]

        if metric_type == "Boolean":
            value = str(value).upper() == "TRUE"
            threshold = str(threshold).upper() == "TRUE"
            return value == threshold
        elif metric_type == "Dynamic Rule; Any":
            # Handle dynamic rules for controversy metrics
            try:
                return float(value) >= float(threshold)
            except (ValueError, TypeError):
                return False

        return False

    def _evaluate_outcome(
        self, row: pd.Series, outcome_rules: Dict, metric_names: Dict[int, str]
    ) -> str:
        """Evaluate outcome (excluded or flag) rules for a row."""
        for category, rules in outcome_rules.items():
            for rule_name, rule_details in rules.items():
                if rule_name == "severe_controversy":
                    # Handle special case for controversy metrics
                    if self._evaluate_condition(
                        row[rule_details["filter_value"]], rule_details
                    ):
                        return rule_details["result"]
                else:
                    # Handle regular metrics
                    metric_sk = rule_details["metric_sk"]
                    if metric_sk in metric_names:
                        metric_name = metric_names[metric_sk]
                        if self._evaluate_condition(row[metric_name], rule_details):
                            return rule_details["result"]
        return None

    def process_data(self, input_file: str) -> pd.DataFrame:
        """
        Process the input CSV file and generate strategy outputs.

        Args:
            input_file (str): Path to the input CSV file

        Returns:
            pd.DataFrame: DataFrame with strategy results
        """
        # Read input data
        df = pd.read_csv(input_file)

        # Initialize results DataFrame
        results = pd.DataFrame(index=df.index)
        results["company_id"] = df["company_id"]

        # Process each strategy
        for strategy_name, strategy_rules in self.strategies.items():
            # Filter metrics for this strategy
            metrics_affecting = strategy_rules["metrics_affecting"]
            strategy_df = self._filter_metrics(df, metrics_affecting)

            # Initialize strategy column with 'OK'
            results[strategy_name] = "OK"

            # Process exclusions first
            if "excluded" in strategy_rules["outcome"]:
                mask = strategy_df.apply(
                    lambda row: self._evaluate_outcome(
                        row,
                        {"excluded": strategy_rules["outcome"]["excluded"]},
                        self.metrics_mapping,
                    )
                    == "EXCLUDED",
                    axis=1,
                )
                results.loc[mask, strategy_name] = "EXCLUDED"

            # Process flags for non-excluded rows
            if "flag" in strategy_rules["outcome"]:
                mask = (results[strategy_name] != "EXCLUDED") & strategy_df.apply(
                    lambda row: self._evaluate_outcome(
                        row,
                        {"flag": strategy_rules["outcome"]["flag"]},
                        self.metrics_mapping,
                    )
                    == "FLAG",
                    axis=1,
                )
                results.loc[mask, strategy_name] = "FLAG"

        return results


def main():
    # Define file paths
    strategy_file = "path/to/strategies.json"
    metrics_mapping_file = "path/to/metrics_mapping.csv"
    input_file = "path/to/input_data.csv"
    output_file = "path/to/output_results.csv"

    # Initialize and run processor
    processor = ESGStrategyProcessor(strategy_file, metrics_mapping_file)
    results = processor.process_data(input_file)

    # Save results
    results.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
