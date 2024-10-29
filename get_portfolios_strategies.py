from datetime import datetime

import pandas as pd

date = datetime.now()
date_str = date.strftime("%y%m%d")

inventario = pd.read_excel(
    r"C:\Users\n740789\Documents\Projects_local\DataSets\Abril_2024_Inventario Reglas Compliance productos ISR_copy20240628.xlsx",
    skiprows=1,
    sheet_name="Productos SRI",
)

portfolio_id_strat = inventario.iloc[:, [10, 1, 2, 7]].copy()

portfolio_id_strat.columns = ["strategy", "portfolio_id", "portfolio_name", "sfdr_art"]

portfolio_id_strat.to_excel(
    rf"C:\Users\n740789\Documents\Projects_local\DataSets\product_sri_strategy\{date_str}_strategies_portfolios_ids.xlsx",
    index=None,
)
