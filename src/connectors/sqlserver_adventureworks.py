"""Connector utilities and example optimized queries for AdventureWorks on SQL Server.

Usage:
    from src.connectors.sqlserver_adventureworks import get_sales_transactions, get_customers
    engine = create_engine(connection_string)
    df = get_sales_transactions(engine, start_date='2024-01-01')
"""
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def _build_conn_string():
    user = os.getenv('SQLSERVER_USER', 'sa')
    pwd = os.getenv('SQLSERVER_PASSWORD', 'YourStrong!Passw0rd')
    host = os.getenv('SQLSERVER_HOST', 'localhost')
    port = os.getenv('SQLSERVER_PORT', '1433')
    db = os.getenv('SQLSERVER_DB', 'AdventureWorks')
    # Using pyodbc connection style
    return f"mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=ODBC+Driver+17+for+SQL+Server"

def get_engine_from_env():
    conn = _build_conn_string()
    return create_engine(conn, fast_executemany=True)

def get_sales_transactions(engine=None, start_date=None, end_date=None):
    """Pull optimized sales order header data with filters applied in SQL (avoid SELECT *).
    Joins to customer/territory are kept minimal here; enrich in Python or in a single SQL join if preferred.
    """
    if engine is None:
        engine = get_engine_from_env()
    params = {}
    where_clause = []
    if start_date:
        where_clause.append("soh.OrderDate >= :start_date")
        params['start_date'] = pd.to_datetime(start_date)
    if end_date:
        where_clause.append("soh.OrderDate <= :end_date")
        params['end_date'] = pd.to_datetime(end_date)
    where_sql = (' AND '.join(where_clause)) if where_clause else '1=1'

    sql = text(f"""
    SELECT
        soh.SalesOrderID,
        soh.OrderDate,
        soh.DueDate,
        soh.ShipDate,
        soh.Status,
        soh.OnlineOrderFlag,
        soh.SalesOrderNumber,
        soh.CustomerID,
        soh.TerritoryID,
        soh.TotalDue
    FROM Sales.SalesOrderHeader AS soh
    WHERE {where_sql}
    ORDER BY soh.OrderDate DESC
    """)
    return pd.read_sql(sql, engine, params=params)

def get_sales_order_details(engine=None, sales_order_ids=None):
    if engine is None:
        engine = get_engine_from_env()
    params = {}
    where_clause = []
    if sales_order_ids:
        # Prevent huge IN lists in SQL by batching in Python if necessary.
        where_clause.append("sod.SalesOrderID IN :order_ids")
        params['order_ids'] = tuple(sales_order_ids)
    where_sql = (' AND '.join(where_clause)) if where_clause else '1=1'

    sql = text(f"""
    SELECT
        sod.SalesOrderID,
        sod.SalesOrderDetailID,
        sod.ProductID,
        sod.OrderQty,
        sod.UnitPrice,
        sod.LineTotal
    FROM Sales.SalesOrderDetail AS sod
    WHERE {where_sql}
    """)
    return pd.read_sql(sql, engine, params=params)

def get_customers(engine=None):
    from sqlalchemy import text
    import pandas as pd

    if engine is None:
        engine = get_engine_from_env()

    sql = text("""
SELECT
    c.CustomerID,
    p.FirstName,
    p.LastName,
    a.City,
    a.StateProvinceID,
    st.Name AS Territory
FROM Sales.Customer c
INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
LEFT JOIN Person.BusinessEntityAddress bea ON p.BusinessEntityID = bea.BusinessEntityID
LEFT JOIN Person.Address a ON bea.AddressID = a.AddressID
LEFT JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
LEFT JOIN Sales.SalesTerritory st ON sp.TerritoryID = st.TerritoryID
    """)
    return pd.read_sql(sql, engine)
