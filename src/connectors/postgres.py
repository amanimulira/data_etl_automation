from sqlalchemy import create_engine
import pandas as pd

def read_sales(conn_str, start_date=None):
    engine = create_engine(conn_str)
    query = """
    SELECT order_id, customer_id, sale_date, amount, campaign_id
    FROM sales_transactions
    WHERE sale_date >= :start_date
    """
    params = {'start_date': start_date}
    return pd.read_sql(query, engine, params=params)