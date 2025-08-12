import pandas as pd
from ..transforms.clean_sales import clean_sales_df


def transform_sales(raw_sales_df, customers_df, marketing_df):
    sales = clean_sales_df(raw_sales_df)
    # join to customers
    merged = sales.merge(customers_df, on='customer_id', how='left')
    merged = merged.merge(marketing_df, on='campaign_id', how='left')
    # create derived cols
    merged['sale_month'] = merged['sale_date'].dt.to_period('M')
    return merged