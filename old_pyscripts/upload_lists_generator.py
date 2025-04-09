import os
from datetime import datetime

import pandas as pd


def process_csv_files(
    input_file,
    output_dir=r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\override_upload_list",
):

    # get date string yyyymmdd
    date = datetime.now()
    date_str = date.strftime("%y%m%d")

    # Read the input Excel file
    df = pd.read_excel(input_file)

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Set constant comment
    COMMENT = "manual override upload"

    # Get all columns except the first one (Aladdin_Issuer)
    columns_to_process = df.columns[1:]

    # Process each column
    for column in columns_to_process:
        # Get unique values in the column
        unique_values = df[column].unique()

        # For each unique value, create a separate CSV file
        for value in unique_values:
            # Filter rows for current value
            filtered_df = df[df[column] == value]

            # Create output dataframe with required format
            output_df = pd.DataFrame(
                {
                    "ID Type": ["Issuer"] * len(filtered_df),
                    "ID": filtered_df["Aladdin_Issuer"],
                    "Start Date": [""] * len(filtered_df),
                    "Comment": [COMMENT] * len(filtered_df),
                }
            )

            # Create filename
            filename = f"{date_str}_{column}_{value}.csv"
            filepath = os.path.join(output_dir, filename)

            # Save to CSV
            output_df.to_csv(filepath, index=False)
            print(f"Created file: {filepath}")


# Example usage
if __name__ == "__main__":
    input_file = r"C:\Users\n740789\Documents\clarity_data_quality_controls\excel_books\input_strategies.xlsx"
    process_csv_files(input_file)
    print("Processing complete!")
