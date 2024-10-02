import pandas as pd
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def analysis(datafeed_col='art_8_basicos', workbench_file='Art8Basico_Scores_check', outf_name='art8'): 
    #READ DATASETS 
    # LOAD DATASETS & MODIFY COLUMN NAMES
    logging.info('Loading crossreference')
    cross = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_20241001.csv', dtype={'CLARITY_AI': str})
    cross.rename(columns={'Aladdin_Issuer':'issuer_id', 'CLARITY_AI':'permid'}, inplace=True) # rename 'Aladdin_Issuer' to 'issuer_id' and 'CLARITY_AI' to permid
    logging.info('Crossreference loaded')
    
    # read datafeed
    logging.info('Loading datafeed')
    df = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow\202410_df_issuer_level_with_ovr.csv', usecols=['permid', datafeed_col], dtype=str)
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
    portfolio = pd.merge(portfolio, df, how='left', on='permid')
    benchmark = pd.merge(benchmark, df, how='left', on='permid')
    logging.info(f'{datafeed_col} added')

    #SAVE DATASETS TO EXCEL FILE
    # save to excel portfolio and benchmark in different sheets named 'portfolio' and 'benchmark'
    logging.info('Saving dataframe to Excel')
    excel_path = rf'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\{outf_name}_impact_analysis.xlsx'
    with pd.ExcelWriter(excel_path) as writer:
        portfolio.to_excel(writer, sheet_name='portfolio', index=False)
        benchmark.to_excel(writer, sheet_name='benchmark', index=False)
    logging.info(f'Results saved to excel on {excel_path}')

def main():
    setup_logging()
    analysis() #missing FOR ART8_BASICS / ESGs / 'SOSTENIBLES' - WE NEED TO ADD BENCH MARKS TOO
    logging.info('Script completed')

if __name__ == "__main__":
    main()