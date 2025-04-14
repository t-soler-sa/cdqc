# clarity_data_quality_controls
Data quality controls and protocols - scripts and tools to explore and keep checks on data delivered by Clarity


## DOC WHAT FILES YOU NEED TO UPLOAD WHERE:

- override_db in sri_data/overrides (download from sharepoint: Fondos éticos / 03. DATASETS / 04_Datos Clarity / 00_Clarity_Data_Quality_Controls / override)
- strategy_snt world in aladdin_data/bmk_portf_str (download from aladdin)
- portfolio_list in sri_data/portfolios_committees (download from sharepoint)
- Aladdin_Clarity_Issuers_{DATE}01.csv in aladdin_data\\crossreference\\

In the future, add inheritance - also datafeed

## PIPELINES/ FLOWS

In this repo you can run 2 flows/pipelines:
    1. The first one, to get the pre-override analysis
    2. The second one, once the override db has been updated, your can run the remeining process that would apply the override and generate multiple outpus to share with SAM BAU Infinity and the Asset Managers.