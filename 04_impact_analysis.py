import pandas as pd
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reorder_columns(df):
    cols = df.columns.tolist()
    start_cols = [col for col in cols if not (col.startswith('str_') or col.startswith('art_') or col.startswith('sustainability_'))]
    end_cols = [col for col in cols if col.startswith('str_') or col.startswith('art_') or col.startswith('sustainability_')]
    end_cols.sort()
    new_order = start_cols + end_cols
    return df[new_order]

def analysis(datafeed_col:list=['permid','art_8_basicos'], workbench_file:str='Art8Basico_Scores_check', outf_name:str='art8'): 
    #READ DATASETS 
    # LOAD DATASETS & MODIFY COLUMN NAMES
    logging.info(f'Generating {workbench_file} Impact Analisys')
    logging.info('Loading crossreference')
    cross = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_20241001.csv', dtype={'CLARITY_AI': str})
    cross.rename(columns={'Aladdin_Issuer':'issuer_id', 'CLARITY_AI':'permid'}, inplace=True) # rename 'Aladdin_Issuer' to 'issuer_id' and 'CLARITY_AI' to permid
    logging.info('Crossreference loaded')
    
    # read datafeed
    logging.info('Loading datafeed')
    df = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow\202410_df_issuer_level_with_ovr.csv', usecols = datafeed_col, dtype=str)
    logging.info('Datafeed loaded')
    
    # read aladdin workbench excel file
    logging.info('Loading Aladdin Workbench file')
    portfolio = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\{workbench_file}.xlsx', sheet_name='Portfolio', skiprows=3) 
    benchmark = pd.read_excel(rf'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\{workbench_file}.xlsx', sheet_name='Benchmark', skiprows=3)
    portfolio.rename(columns={'Issuer ID':'issuer_id'}, inplace=True) # rename 'Issuer ID' to 'issuer_id'
    benchmark.rename(columns={'Issuer ID':'issuer_id'}, inplace=True) # rename 'Issuer ID' to 'issuer_id'
    logging.info('Aladdin Workbench file loaded')

    #PROCESS DATASETS
    # add permid column from crossreference to portfolio and benchmark dataframes
    logging.info('adding permid to portfolios and benchmarks')
    portfolio = pd.merge(portfolio, cross[['issuer_id', 'permid']], how='left', on='issuer_id')
    benchmark = pd.merge(benchmark, cross[['issuer_id', 'permid']], how='left', on='issuer_id')
    logging.info('permid added successfully')
    
    # merge aladdin with df on 'permid'
    logging.info(f'adding datafeed column {datafeed_col} to portfolio and benchmark')
    portfolio = pd.merge(portfolio, df, how='left', on='permid', suffixes=("_current","_new"))
    benchmark = pd.merge(benchmark, df, how='left', on='permid', suffixes=("_current","_new"))
    logging.info(f'{datafeed_col} added')

    # order columns, columns starting with "str" or "art" should go at the end
    logging.info('reordering columns')
    portfolio = reorder_columns(portfolio)
    benchmark = reorder_columns(benchmark)
    logging.info('columns sorted')
 

    #SAVE DATASETS TO EXCEL FILE
    # save to excel portfolio and benchmark in different sheets named 'portfolio' and 'benchmark'
    logging.info('Saving dataframe to Excel')
    excel_path = rf'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\{outf_name}_impact_analysis.xlsx'
    with pd.ExcelWriter(excel_path) as writer:
        portfolio.to_excel(writer, sheet_name='portfolio', index=False)
        benchmark.to_excel(writer, sheet_name='benchmark', index=False)
    logging.info(f'{workbench_file} Impact Analisys: Results saved to excel on {excel_path}')

def main():
    setup_logging()
    analysis() # remember to include the 'permid' column name inside the parameter 'datafeed_col', which has to be a list with column names
    analysis(datafeed_col=['permid','sustainability_rating', 'str_001_s'], workbench_file='ESGFunds_Scores_check', outf_name='esg')
    analysis(datafeed_col=['permid','sustainability_rating', 'str_004_asec'], workbench_file='SustFunds_Scores_check', outf_name='sustainable')
    logging.info('Script completed')

if __name__ == "__main__":
    main()