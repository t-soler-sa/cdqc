import pandas as pd
import logging


def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    setup_logging()

    #READ DATASETS FOR ART8_BASICS / ESGs / 'SOSTENIBLES' - WE NEED TO ADD BENCH MARKS TOO
    # read crossreference
    logging.info('Loading crossreference')
    cross = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\crossreference\Aladdin_Clarity_Issuers_20241001.csv', dtype={'CLARITY_AI': str})
    # rename 'Aladdin_Issuer' to 'issuer_id'
    cross.rename(columns={'Aladdin_Issuer':'aladdin_issuer'}, inplace=True)
    logging.info('Crossreference loaded')
    # read datafeed
    logging.info('Loading datafeed')
    df = pd.read_csv(r'C:\Users\n740789\Documents\Projects_local\DataSets\DATAFEED\datafeeds_with_ow\202410_df_issuer_level_with_ovr.csv', usecols=['permId','art_8_basicos'])
    logging.info('Datafeed loaded')
   # read aladdin workbench excel file
    logging.info('Loading Aladdin Workbench file')
    portfolio = pd.read_excel(r'C:\Users\n740789\Documents\Projects_local\DataSets\Aladdin_workbench\Aladdin_workbench_20241001.xlsx', sheet_name='Portfolio') #WE WILL NEED OT REPEAT FOR BENCHMARK
    #rename 'Issuer Id'
    portfolio.rename(columns={'Issuer Id':'issuer_id'}, inplace=True)
    logging.info('Aladdin Workbench file loaded')
    
    #PROCESS DATASETS
    # merge aladdin with crossreference, use only crossreference column "clarity_id" and "aladdin_issuer"
    logging.info('adding permId to aladdin workbench')
    #rename 'Issuer ID' to aladdin_id'
    portfolio = pd.merge(portfolio, cross[['issuer_id',"clarity_id"]], how='left', on='aladdin_issuer')
    #rename column "clarity_id" to "permId"
    portfolio.rename(columns={'clarity_id':'permId'}, inplace=True)
    logging.info('permId added succesfully')
    
    # merge aladdin with df on 'permId'
    logging.info('Merging datafeed with aladdin workbench')
    portfolio = pd.merge(portfolio, df, how='left', on='permId')
    logging.info('Datafeed merged with aladdin workbench')

    # save to excel
    logging.info('Saving dataframe to Excel')
    portfolio.to_excel(r'C:\Users\n740789\Documents\Projects_local\DataSets\impact_analysis\impact_analysis.xlsx', index=False, sheet_name='portfolio')
    logging.info('Dataframe saved to Excel')
    
    logging.info('Script completed')