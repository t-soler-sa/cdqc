import pandas as pd
import logging
import os
from pathlib import Path

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reorder_columns(df):
    cols = df.columns.tolist()
    start_cols = [col for col in cols if not (col.startswith('str_') or col.startswith('art_') or col.startswith('sustainability_'))]
    end_cols = [col for col in cols if col.startswith('str_') or col.startswith('art_') or col.startswith('sustainability_')]
    end_cols.sort()
    new_order = start_cols + end_cols
    return df[new_order]

def analysis(input_file: str, output_file: str, datafeed_col: list):
    logging.info(f'Generating Impact Analysis for {input_file}')

    # READ DATASETS
    # LOAD DATASETS & MODIFY COLUMN NAMES
    logging.info('Loading crossreference')
    cross = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_20241001.csv', dtype={'CLARITY_AI': str})
    cross.rename(columns={'Aladdin_Issuer':'issuer_id', 'CLARITY_AI':'permid'}, inplace=True)
    logging.info('Crossreference loaded')

    # read datafeed
    logging.info('Loading datafeed')
    df = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ovr\202410_df_issuer_level_with_ovr.csv', usecols=datafeed_col, dtype=str)
    logging.info('Datafeed loaded')

    # read aladdin workbench excel file
    logging.info('Loading Aladdin Workbench file')
    portfolio = pd.read_excel(input_file, sheet_name='Portfolio', skiprows=3)
    benchmark = pd.read_excel(input_file, sheet_name='Benchmark', skiprows=3)
    portfolio.rename(columns={'Issuer ID':'issuer_id'}, inplace=True)
    benchmark.rename(columns={'Issuer ID':'issuer_id'}, inplace=True)
    logging.info('Aladdin Workbench file loaded')

    # PROCESS DATASETS
    logging.info('adding permid to portfolios and benchmarks')
    portfolio = pd.merge(portfolio, cross[['issuer_id', 'permid']], how='left', on='issuer_id')
    benchmark = pd.merge(benchmark, cross[['issuer_id', 'permid']], how='left', on='issuer_id')
    logging.info('permid added successfully')

    logging.info(f'adding datafeed columns to portfolio and benchmark')
    portfolio = pd.merge(portfolio, df, how='left', on='permid', suffixes=("_current","_new"))
    benchmark = pd.merge(benchmark, df, how='left', on='permid', suffixes=("_current","_new"))
    logging.info('datafeed columns added')

    logging.info('reordering columns')
    portfolio = reorder_columns(portfolio)
    benchmark = reorder_columns(benchmark)
    logging.info('columns sorted')

    # SAVE DATASETS TO EXCEL FILE
    logging.info('Saving dataframe to Excel')
    with pd.ExcelWriter(output_file) as writer:
        portfolio.to_excel(writer, sheet_name='portfolio', index=False)
        benchmark.to_excel(writer, sheet_name='benchmark', index=False)
    logging.info(f'Impact Analysis: Results saved to excel on {output_file}')

def process_directory(input_dir: str, output_dir: str, datafeed_col: list):
    for file in os.listdir(input_dir):
        if file.endswith('.xlsx'):
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(output_dir, file.replace('.xlsx', '_analysis.xlsx'))
            analysis(input_file, output_file, datafeed_col)

def main():
    setup_logging()
    base_dir = r'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\1024'
    input_base = os.path.join(base_dir, 'aladdin_input')
    output_base = os.path.join(base_dir, 'analysis_output')

    # Ensure output directories exist
    for dir_name in ['art8_analysis', 'esg_analysis', 'sustainable_analysis']:
        os.makedirs(os.path.join(output_base, dir_name), exist_ok=True)

    # Process Art 8 Basico
    process_directory(
        os.path.join(input_base, 'art_8_basico'),
        os.path.join(output_base, 'art8_analysis'),
        ['permid', 'art_8_basicos']
    )

    # Process ESG
    process_directory(
        os.path.join(input_base, 'ESG'),
        os.path.join(output_base, 'esg_analysis'),
        ['permid', 'sustainability_rating', 'str_001_s']
    )

    # Process Sustainable
    process_directory(
        os.path.join(input_base, 'Sustainable'),
        os.path.join(output_base, 'sustainable_analysis'),
        ['permid', 'sustainability_rating', 'str_004_asec']
    )

    logging.info('Script completed')

if __name__ == "__main__":
    main()