import os
import datetime as dt
from collections import defaultdict
 
import pyodbc
from pymongo import MongoClient
from dotenv import load_dotenv
 
from dgdb_orm import (
    create_dgdb_tables,
    start_etl_run,
    end_etl_run,
    log_validation_result,
)
 
load_dotenv()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5000"))
 
HOTEL_SRC = os.getenv("SQLSERVER_HOTEL_SOURCE_CONN_STR")
SPRINT4_SRC = os.getenv("SQLSERVER_SPRINT4_SOURCE_CONN_STR")
DW_CUSTOMER = os.getenv("SQLSERVER_CUSTOMERSPEND_DW_CONN_STR")
DW_REVENUE = os.getenv("SQLSERVER_REVENUEGEOGRAPHY_DW_CONN_STR")
DW_PAYROLL = os.getenv("SQLSERVER_EMPLOYEEPAYROLL_DW_CONN_STR")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB_GIFTSHOP", "hotel")
 
REQUIRED_VARS = [
    "SQLSERVER_HOTEL_SOURCE_CONN_STR",
    "SQLSERVER_SPRINT4_SOURCE_CONN_STR",
    "SQLSERVER_CUSTOMERSPEND_DW_CONN_STR",
    "SQLSERVER_REVENUEGEOGRAPHY_DW_CONN_STR",
    "SQLSERVER_EMPLOYEEPAYROLL_DW_CONN_STR",
    "SQLSERVER_DGDB_CONN_STR",
    "MONGO_URI",
]
def ensure_env():
    missing = [name for name in REQUIRED_VARS if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Missing required .env values: {', '.join(missing)}")
 
def connect_sql(conn_str: str):
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    cur = conn.cursor()
    cur.fast_executemany = True
    return conn, cur
 
def chunks(data, size):
    for i in range(0, len(data), size):
        yield data[i:i + size]
 
def date_key_day(d: dt.date) -> int:
    return d.year * 10000 + d.month * 100 + d.day
 
def date_key_month(d: dt.date) -> int:
    return d.year * 100 + d.month
 
def date_key_year(y: int) -> int:
    return y
 
def month_name(m: int) -> str:
    return dt.date(2000, m, 1).strftime("%B")
 
def weekday_name(d: dt.date) -> str:
    return d.strftime("%A")
 
def normalize_payment(method) -> str:
    if not method:
        return "UNKNOWN"
    method = str(method).strip()
    return method.split()[0] if " " in method else method
 
def refresh_dim_customer(cur):
    cur.execute("SELECT CustomerEmail, CustomerKey FROM dbo.DimCustomer")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_geo_customer(cur):
    cur.execute("SELECT City, State, Zip, GeographyKey FROM dbo.DimGeography")
    return {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
 
def refresh_dim_payment(cur):
    cur.execute("SELECT MethodName, PaymentMethodKey FROM dbo.DimPaymentMethod")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_guest(cur):
    cur.execute("SELECT GuestStatus, GuestStatusKey FROM dbo.DimGuestStatus")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_date_set(cur):
    cur.execute("SELECT DateKey FROM dbo.DimDate")
    return {r[0] for r in cur.fetchall()}
  
def refresh_dim_geo_revenue(cur):
    cur.execute("SELECT State, GeographyKey FROM dbo.DimGeography")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_hotel(cur):
    cur.execute("SELECT HotelID, HotelKey FROM dbo.DimHotel")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_shop(cur):
    cur.execute("SELECT ShopID, ShopKey FROM dbo.DimShop")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_job(cur):
    cur.execute("SELECT JobTitle, JobKey FROM dbo.DimJob")
    return {r[0]: r[1] for r in cur.fetchall()}
 
def refresh_dim_employee(cur):
    cur.execute("SELECT EmployeeID, EmployeeKey FROM dbo.DimEmployee")
    return {r[0]: r[1] for r in cur.fetchall()}

# 1) CustomerSpendDW
def load_customer_spend_dw():
    print("Loading CustomerSpendDW...")
    src_conn, src_cur = connect_sql(HOTEL_SRC)
    dw_conn, dw_cur = connect_sql(DW_CUSTOMER)
    mongo = MongoClient(MONGO_URI)
    gift_db = mongo[MONGO_DB]
    processed = 0
    rejected = 0
 
    try:
        dim_date = refresh_dim_date_set(dw_cur)
        dim_customer = refresh_dim_customer(dw_cur)
        dim_geo = refresh_dim_geo_customer(dw_cur)
        dim_guest = refresh_dim_guest(dw_cur)
        dim_payment = refresh_dim_payment(dw_cur)
 
        guest_rows = []
        for g in ["Hotel Guest", "Non-Hotel Gift Shop Customer"]:
            if g not in dim_guest:
                guest_rows.append((g,))
        if guest_rows:
            dw_cur.executemany(
                "INSERT INTO dbo.DimGuestStatus (GuestStatus) VALUES (?)",
                guest_rows,
            )
            dw_conn.commit()
            dim_guest = refresh_dim_guest(dw_cur)
 
        src_cur.execute("""
            SELECT
                CAST(t.transaction_date AS date) AS TxDate,
                c.email,
                c.first_name,
                c.last_name,
                c.id AS HotelCustomerID,
                c.city,
                c.state,
                c.zip,
                t.payment_method,
                SUM(t.amount) AS HotelSpend,
                COUNT(*) AS HotelTxnCount
            FROM dbo.transact t
            JOIN dbo.customer c ON t.customer_id = c.id
            GROUP BY CAST(t.transaction_date AS date),
                     c.email, c.first_name, c.last_name, c.id,
                     c.city, c.state, c.zip, t.payment_method
        """)
        hotel_rows = src_cur.fetchall()
        processed += len(hotel_rows)
 
        gift_pipeline = [
            {
                "$match": {
                    "customer.email": {"$exists": True, "$nin": [None, ""]}
                }
            },
            {
                "$project": {
                    "tx_date": {"$dateTrunc": {"date": "$timestamp", "unit": "day"}},
                    "email": "$customer.email",
                    "fname": "$customer.fname",
                    "lname": "$customer.lname",
                    "gift_customer_id": "$customer.id",
                    "main_customer_id": "$customer.main_customer_id",
                    "city": {"$ifNull": ["$customer.city", ""]},
                    "state": {"$ifNull": ["$customer.state", ""]},
                    "zip": {"$ifNull": ["$customer.zip", ""]},
                    "payment_method": "$payment.method",
                    "total": "$total"
                }
            },
            {
                "$group": {
                    "_id": {
                        "tx_date": "$tx_date",
                        "email": "$email",
                        "city": "$city",
                        "state": "$state",
                        "zip": "$zip",
                        "payment_method": "$payment_method"
                    },
                    "fname": {"$first": "$fname"},
                    "lname": {"$first": "$lname"},
                    "gift_customer_id": {"$first": "$gift_customer_id"},
                    "main_customer_id": {"$first": "$main_customer_id"},
                    "gift_spend": {"$sum": "$total"},
                    "gift_txn_count": {"$sum": 1}
                }
            }
        ]
        gift_rows = list(gift_db["transactions"].aggregate(gift_pipeline, allowDiskUse=True))
        processed += len(gift_rows)
 
        new_dates = []
        new_customers = []
        new_geos = []
        new_payments = []
 
        seen_dates = set()
        seen_customers = set()
        seen_geos = set()
        seen_payments = set()
 
        for r in hotel_rows:
            if not r.email or not r.TxDate:
                rejected += 1
                continue
 
            dk = date_key_day(r.TxDate)
            if dk not in dim_date and dk not in seen_dates:
                new_dates.append((
                    dk,
                    r.TxDate,
                    r.TxDate.day,
                    r.TxDate.month,
                    month_name(r.TxDate.month),
                    ((r.TxDate.month - 1) // 3) + 1,
                    r.TxDate.year,
                    weekday_name(r.TxDate)
                ))
                seen_dates.add(dk)
 
            if r.email not in dim_customer and r.email not in seen_customers:
                new_customers.append((
                    r.email,
                    r.first_name,
                    r.last_name,
                    r.HotelCustomerID,
                    None,
                    None
                ))
                seen_customers.add(r.email)
 
            geo_key = (r.city, r.state, r.zip)
            if geo_key not in dim_geo and geo_key not in seen_geos:
                new_geos.append(geo_key)
                seen_geos.add(geo_key)
 
            pm = normalize_payment(r.payment_method)
            if pm not in dim_payment and pm not in seen_payments:
                new_payments.append((pm,))
                seen_payments.add(pm)
 
        for r in gift_rows:
            grp = r.get("_id", {})
            txdt = grp.get("tx_date")
            email = grp.get("email")
 
            if not email or not isinstance(txdt, dt.datetime):
                rejected += 1
                continue
 
            d = txdt.date()
            dk = date_key_day(d)
 
            if dk not in dim_date and dk not in seen_dates:
                new_dates.append((
                    dk,
                    d,
                    d.day,
                    d.month,
                    month_name(d.month),
                    ((d.month - 1) // 3) + 1,
                    d.year,
                    weekday_name(d)
                ))
                seen_dates.add(dk)
 
            if email not in dim_customer and email not in seen_customers:
                new_customers.append((
                    email,
                    r.get("fname"),
                    r.get("lname"),
                    None,
                    r.get("gift_customer_id"),
                    r.get("main_customer_id")
                ))
                seen_customers.add(email)
 
            city = grp.get("city", "")
            state = grp.get("state", "")
            zip_code = grp.get("zip", "")
            geo_key = (city, state, zip_code)
 
            if geo_key not in dim_geo and geo_key not in seen_geos:
                new_geos.append(geo_key)
                seen_geos.add(geo_key)
 
            pm = normalize_payment(grp.get("payment_method"))
            if pm not in dim_payment and pm not in seen_payments:
                new_payments.append((pm,))
                seen_payments.add(pm)
 
        if new_dates:
            dw_cur.executemany("""
                INSERT INTO dbo.DimDate
                (DateKey, FullDate, [Day], [Month], MonthName, [Quarter], [Year], DayOfWeek)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, new_dates)
            dw_conn.commit()
            dim_date = refresh_dim_date_set(dw_cur)
 
        if new_customers:
            dw_cur.executemany("""
                INSERT INTO dbo.DimCustomer
                (CustomerEmail, FirstName, LastName, HotelCustomerID, GiftShopCustomerID, MainCustomerID)
                VALUES (?, ?, ?, ?, ?, ?)
            """, new_customers)
            dw_conn.commit()
            dim_customer = refresh_dim_customer(dw_cur)
 
        if new_geos:
            dw_cur.executemany(
                "INSERT INTO dbo.DimGeography (City, State, Zip) VALUES (?, ?, ?)",
                new_geos
            )
            dw_conn.commit()
            dim_geo = refresh_dim_geo_customer(dw_cur)
 
        if new_payments:
            dw_cur.executemany(
                "INSERT INTO dbo.DimPaymentMethod (MethodName) VALUES (?)",
                new_payments
            )
            dw_conn.commit()
            dim_payment = refresh_dim_payment(dw_cur)
 
        dw_cur.execute("""
            SELECT DateKey, CustomerKey, GeographyKey, GuestStatusKey, ISNULL(PaymentMethodKey,-1)
            FROM dbo.FactCustomerSpend
        """)
        fact_sig = {(r[0], r[1], r[2], r[3], r[4]) for r in dw_cur.fetchall()}
 
        fact_rows = []
 
        for r in hotel_rows:
            if not r.email or not r.TxDate:
                continue
 
            dk = date_key_day(r.TxDate)
            ck = dim_customer[r.email]
            gk = dim_geo[(r.city, r.state, r.zip)]
            gsk = dim_guest["Hotel Guest"]
            pmk = dim_payment[normalize_payment(r.payment_method)]
 
            sig = (dk, ck, gk, gsk, pmk)
            if sig in fact_sig:
                continue
 
            fact_rows.append((
                dk,
                ck,
                None,
                gk,
                gsk,
                pmk,
                float(r.HotelSpend or 0),
                0.0,
                int(r.HotelTxnCount or 0),
                0
            ))
            fact_sig.add(sig)
 
        for r in gift_rows:
            grp = r.get("_id", {})
            txdt = grp.get("tx_date")
            email = grp.get("email")
 
            if not email or not isinstance(txdt, dt.datetime):
                continue
 
            d = txdt.date()
            dk = date_key_day(d)
 
            if email not in dim_customer:
                continue
 
            city = grp.get("city", "")
            state = grp.get("state", "")
            zip_code = grp.get("zip", "")
            geo_tuple = (city, state, zip_code)
 
            if geo_tuple not in dim_geo:
                continue
 
            ck = dim_customer[email]
            gk = dim_geo[geo_tuple]
            gsk = dim_guest["Non-Hotel Gift Shop Customer"]
            pmk = dim_payment.get(normalize_payment(grp.get("payment_method")))
 
            if pmk is None:
                continue
 
            sig = (dk, ck, gk, gsk, pmk)
            if sig in fact_sig:
                continue
 
            fact_rows.append((
                dk,
                ck,
                None,
                gk,
                gsk,
                pmk,
                0.0,
                float(r.get("gift_spend", 0.0)),
                0,
                int(r.get("gift_txn_count", 0))
            ))
            fact_sig.add(sig)
 
        if fact_rows:
            for batch in chunks(fact_rows, BATCH_SIZE):
                dw_cur.executemany("""
                    INSERT INTO dbo.FactCustomerSpend
                    (DateKey, CustomerKey, RoomTypeKey, GeographyKey, GuestStatusKey, PaymentMethodKey,
                     HotelSpendAmount, GiftShopSpendAmount, HotelTransactionCount, GiftShopTransactionCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                dw_conn.commit()
 
        return processed, rejected
 
    finally:
        src_conn.close()
        dw_conn.close()
        mongo.close()

# 2) RevenueGeographyDW
def load_revenue_geography_dw():
    print("Loading RevenueGeographyDW...")
    src_conn, src_cur = connect_sql(HOTEL_SRC)
    dw_conn, dw_cur = connect_sql(DW_REVENUE)
    mongo = MongoClient(MONGO_URI)
    gift_db = mongo[MONGO_DB]
 
    processed = 0
    rejected = 0
 
    try:
        dim_date = refresh_dim_date_set(dw_cur)
        dim_geo = refresh_dim_geo_revenue(dw_cur)
        dim_hotel = refresh_dim_hotel(dw_cur)
        dim_shop = refresh_dim_shop(dw_cur)
 
        src_cur.execute("SELECT id, name, city, state, zip FROM dbo.property")
        hotel_dim_rows = []
        for r in src_cur.fetchall():
            if r.id not in dim_hotel:
                hotel_dim_rows.append((r.id, r.name, r.city, r.state, r.zip))
        if hotel_dim_rows:
            dw_cur.executemany("""
                INSERT INTO dbo.DimHotel (HotelID, HotelName, City, State, Zip)
                VALUES (?, ?, ?, ?, ?)
            """, hotel_dim_rows)
            dw_conn.commit()
            dim_hotel = refresh_dim_hotel(dw_cur)
 
        shop_docs = list(gift_db["shops"].find({}, {"_id": 0}))
        shop_dim_rows = []
        for d in shop_docs:
            sid = d.get("id")
            if sid not in dim_shop:
                shop_dim_rows.append((
                    sid,
                    d.get("name"),
                    d.get("city"),
                    d.get("state"),
                    d.get("zip"),
                    d.get("date_opened")
                ))
        if shop_dim_rows:
            dw_cur.executemany("""
                INSERT INTO dbo.DimShop (ShopID, ShopName, City, State, Zip, DateOpened)
                VALUES (?, ?, ?, ?, ?, ?)
            """, shop_dim_rows)
            dw_conn.commit()
            dim_shop = refresh_dim_shop(dw_cur)
 
        src_cur.execute("""
            SELECT
                YEAR(t.transaction_date) AS Yr,
                MONTH(t.transaction_date) AS Mo,
                c.state AS CustState,
                t.hotel_id,
                SUM(t.amount) AS HotelRevenue,
                COUNT(*) AS HotelTxnCount
            FROM dbo.transact t
            JOIN dbo.customer c ON t.customer_id = c.id
            GROUP BY YEAR(t.transaction_date), MONTH(t.transaction_date), c.state, t.hotel_id
        """)
        hotel_rows = src_cur.fetchall()
        processed += len(hotel_rows)
 
        gift_pipeline = [
            {
                "$match": {
                    "customer.state": {"$exists": True, "$nin": [None, ""]}
                }
            },
            {
                "$project": {
                    "tx_month": {"$dateTrunc": {"date": "$timestamp", "unit": "month"}},
                    "state": {"$ifNull": ["$customer.state", ""]},
                    "shop_id": "$store_id",
                    "total": "$total"
                }
            },
            {
                "$group": {
                    "_id": {
                        "tx_month": "$tx_month",
                        "state": "$state",
                        "shop_id": "$shop_id"
                    },
                    "gift_revenue": {"$sum": "$total"},
                    "gift_txn_count": {"$sum": 1}
                }
            }
        ]
        gift_rows = list(gift_db["transactions"].aggregate(gift_pipeline, allowDiskUse=True))
        processed += len(gift_rows)
 
        new_dates = []
        new_geos = []
        seen_dates = set()
        seen_geos = set()
 
        for r in hotel_rows:
            if not r.CustState:
                rejected += 1
                continue
 
            dk = int(r.Yr) * 100 + int(r.Mo)
            if dk not in dim_date and dk not in seen_dates:
                new_dates.append((
                    dk,
                    int(r.Mo),
                    month_name(int(r.Mo)),
                    ((int(r.Mo) - 1) // 3) + 1,
                    int(r.Yr)
                ))
                seen_dates.add(dk)
 
            if r.CustState not in dim_geo and r.CustState not in seen_geos:
                new_geos.append((r.CustState,))
                seen_geos.add(r.CustState)
 
        for r in gift_rows:
            grp = r.get("_id", {})
            txm = grp.get("tx_month")
            state = grp.get("state")
 
            if not state or not isinstance(txm, dt.datetime):
                rejected += 1
                continue
 
            d = txm.date()
            dk = date_key_month(d)
 
            if dk not in dim_date and dk not in seen_dates:
                new_dates.append((
                    dk,
                    d.month,
                    month_name(d.month),
                    ((d.month - 1) // 3) + 1,
                    d.year
                ))
                seen_dates.add(dk)
 
            if state not in dim_geo and state not in seen_geos:
                new_geos.append((state,))
                seen_geos.add(state)
 
        if new_dates:
            dw_cur.executemany("""
                INSERT INTO dbo.DimDate (DateKey, [Month], MonthName, [Quarter], [Year])
                VALUES (?, ?, ?, ?, ?)
            """, new_dates)
            dw_conn.commit()
            dim_date = refresh_dim_date_set(dw_cur)
 
        if new_geos:
            dw_cur.executemany(
                "INSERT INTO dbo.DimGeography (State) VALUES (?)",
                new_geos
            )
            dw_conn.commit()
            dim_geo = refresh_dim_geo_revenue(dw_cur)
 
        dw_cur.execute("""
            SELECT DateKey, GeographyKey, ISNULL(HotelKey,-1), ISNULL(ShopKey,-1)
            FROM dbo.FactRevenueGeography
        """)
        fact_sig = {(r[0], r[1], r[2], r[3]) for r in dw_cur.fetchall()}
 
        fact_rows = []
 
        for r in hotel_rows:
            if not r.CustState:
                continue
 
            dk = int(r.Yr) * 100 + int(r.Mo)
            gk = dim_geo[r.CustState]
            hk = dim_hotel.get(r.hotel_id)
 
            sig = (dk, gk, hk if hk is not None else -1, -1)
            if sig in fact_sig:
                continue
 
            fact_rows.append((
                dk,
                gk,
                hk,
                None,
                float(r.HotelRevenue or 0),
                0.0,
                int(r.HotelTxnCount or 0),
                0
            ))
            fact_sig.add(sig)
 
        for r in gift_rows:
            grp = r.get("_id", {})
            txm = grp.get("tx_month")
            state = grp.get("state")
            shop_id = grp.get("shop_id")
 
            if not state or not isinstance(txm, dt.datetime):
                continue
 
            dk = date_key_month(txm.date())
            if state not in dim_geo:
                continue
 
            gk = dim_geo[state]
            sk = dim_shop.get(shop_id)
 
            sig = (dk, gk, -1, sk if sk is not None else -1)
            if sig in fact_sig:
                continue
 
            fact_rows.append((
                dk,
                gk,
                None,
                sk,
                0.0,
                float(r.get("gift_revenue", 0.0)),
                0,
                int(r.get("gift_txn_count", 0))
            ))
            fact_sig.add(sig)
 
        if fact_rows:
            for batch in chunks(fact_rows, BATCH_SIZE):
                dw_cur.executemany("""
                    INSERT INTO dbo.FactRevenueGeography
                    (DateKey, GeographyKey, HotelKey, ShopKey, HotelRevenue, GiftShopRevenue, HotelTransactionCount, GiftTransactionCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                dw_conn.commit()
 
        return processed, rejected
 
    finally:
        src_conn.close()
        dw_conn.close()
        mongo.close()
 
# 3) EmployeePayrollDW from WideWorldImportersDW
def load_employee_payroll_dw():
    print("Loading EmployeePayrollDW...")
    src_conn, src_cur = connect_sql(SPRINT4_SRC)
    dw_conn, dw_cur = connect_sql(DW_PAYROLL)
    processed = 0
    rejected = 0
 
    try:
        dim_date = refresh_dim_date_set(dw_cur)
        dim_job = refresh_dim_job(dw_cur)
        dim_hotel = refresh_dim_hotel(dw_cur)
        dim_employee = refresh_dim_employee(dw_cur)
 
        src_cur.execute("""
            SELECT
                e.[Employee Key] AS SourceEmployeeKey,
                e.[WWI Employee ID] AS EmployeeID,
                e.[Employee] AS EmployeeName,
                e.[Preferred Name] AS PreferredName,
                e.[Is Salesperson] AS IsSalesperson
            FROM [Dimension].[Employee] e
        """)
        employees = src_cur.fetchall()
        processed += len(employees)
 
        src_cur.execute("""
            SELECT
                YEAR(s.[Invoice Date Key]) AS SaleYear,
                s.[Salesperson Key] AS SourceEmployeeKey,
                SUM(s.[Total Including Tax]) AS SalesAmount,
                COUNT(*) AS SaleCount
            FROM [Fact].[Sale] s
            GROUP BY YEAR(s.[Invoice Date Key]), s.[Salesperson Key]
        """)
        sales = src_cur.fetchall()
        processed += len(sales)
 
        years = sorted({int(r.SaleYear) for r in sales if r.SaleYear is not None})
        new_dates = [(y, y) for y in years if y not in dim_date]
        if new_dates:
            dw_cur.executemany(
                "INSERT INTO dbo.DimDate (DateKey, [Year]) VALUES (?, ?)",
                new_dates
            )
            dw_conn.commit()
            dim_date = refresh_dim_date_set(dw_cur)
 
        job_rows = []
        for title in ["Salesperson", "Non-Salesperson"]:
            if title not in dim_job:
                job_rows.append((title,))
        if job_rows:
            dw_cur.executemany(
                "INSERT INTO dbo.DimJob (JobTitle) VALUES (?)",
                job_rows
            )
            dw_conn.commit()
            dim_job = refresh_dim_job(dw_cur)
 
        if 1 not in dim_hotel:
            dw_cur.execute("""
                INSERT INTO dbo.DimHotel (HotelID, HotelName, City, State, Zip)
                VALUES (1, 'WideWorldImportersDW', 'N/A', 'N/A', 'N/A')
            """)
            dw_conn.commit()
            dim_hotel = refresh_dim_hotel(dw_cur)
 
        unique_employees = {}
        for r in employees:
            if r.EmployeeID is not None and r.EmployeeID not in unique_employees:
                unique_employees[r.EmployeeID] = r
 
        new_employees = []
        for emp_id, r in unique_employees.items():
            if emp_id not in dim_employee:
                job_title = "Salesperson" if r.IsSalesperson else "Non-Salesperson"
                jk = dim_job[job_title]
                new_employees.append((
                    emp_id,
                    r.PreferredName,
                    r.EmployeeName,
                    jk,
                    dim_hotel[1]
                ))
 
        if new_employees:
            dw_cur.executemany("""
                INSERT INTO dbo.DimEmployee (EmployeeID, FirstName, LastName, JobKey, HotelKey)
                VALUES (?, ?, ?, ?, ?)
            """, new_employees)
            dw_conn.commit()
            dim_employee = refresh_dim_employee(dw_cur)
 
        src_emp_map = {}
        src_emp_salesperson = {}
        for r in employees:
            src_emp_map[r.SourceEmployeeKey] = r.EmployeeID
            src_emp_salesperson[r.EmployeeID] = r.IsSalesperson
 
        dw_cur.execute("SELECT DateKey, EmployeeKey FROM dbo.FactEmployeePayroll")
        payroll_sig = {(r[0], r[1]) for r in dw_cur.fetchall()}
 
        payroll_rows = []
        for r in sales:
            emp_id = src_emp_map.get(r.SourceEmployeeKey)
            if emp_id is None or emp_id not in dim_employee:
                rejected += 1
                continue
 
            dk = date_key_year(int(r.SaleYear))
            ek = dim_employee[emp_id]
            job_title = "Salesperson" if src_emp_salesperson.get(emp_id) else "Non-Salesperson"
            jk = dim_job[job_title]
            hk = dim_hotel[1]
 
            sig = (dk, ek)
            if sig in payroll_sig:
                continue
 
            payroll_rows.append((
                dk,
                ek,
                jk,
                hk,
                float(r.SalesAmount or 0),
                float(r.SalesAmount or 0),
                int(r.SaleCount or 0)
            ))
            payroll_sig.add(sig)
 
        if payroll_rows:
            for batch in chunks(payroll_rows, BATCH_SIZE):
                dw_cur.executemany("""
                    INSERT INTO dbo.FactEmployeePayroll
                    (DateKey, EmployeeKey, JobKey, HotelKey, NetPay, BaseSalary, EmployeeCount)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, batch)
                dw_conn.commit()
 
        dw_cur.execute("SELECT DateKey, HotelKey FROM dbo.FactStaffingRevenue")
        staffing_sig = {(r[0], r[1]) for r in dw_cur.fetchall()}
 
        by_year = defaultdict(lambda: {"revenue": 0.0, "count": 0})
        for r in sales:
            y = int(r.SaleYear)
            by_year[y]["revenue"] += float(r.SalesAmount or 0)
            by_year[y]["count"] += int(r.SaleCount or 0)
 
        staffing_rows = []
        for y, vals in by_year.items():
            dk = date_key_year(y)
            hk = dim_hotel[1]
            sig = (dk, hk)
            if sig in staffing_sig:
                continue
 
            staffing_rows.append((
                dk,
                hk,
                vals["revenue"],
                vals["revenue"],
                vals["count"]
            ))
            staffing_sig.add(sig)
 
        if staffing_rows:
            for batch in chunks(staffing_rows, BATCH_SIZE):
                dw_cur.executemany("""
                    INSERT INTO dbo.FactStaffingRevenue
                    (DateKey, HotelKey, HotelRevenue, TotalBaseSalary, EmployeeCount)
                    VALUES (?, ?, ?, ?, ?)
                """, batch)
                dw_conn.commit()
 
        return processed, rejected
 
    finally:
        src_conn.close()
        dw_conn.close()
 
# DGDB VALIDATIONS
def validate_customer_fk(run_id):
    conn, cur = connect_sql(DW_CUSTOMER)
    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.FactCustomerSpend f
            LEFT JOIN dbo.DimCustomer d
                ON f.CustomerKey = d.CustomerKey
            WHERE d.CustomerKey IS NULL
        """)
        failed = cur.fetchone()[0]
 
        cur.execute("SELECT COUNT(*) FROM dbo.FactCustomerSpend")
        checked = cur.fetchone()[0]
 
        log_validation_result(
            run_id,
            "Customer FK Integrity",
            "CustomerKey must exist in DimCustomer",
            checked,
            checked - failed,
            failed
        )
    finally:
        conn.close()
 
def validate_no_null_amounts(run_id):
    conn, cur = connect_sql(DW_REVENUE)
    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.FactRevenueGeography
            WHERE HotelRevenue IS NULL OR GiftShopRevenue IS NULL
        """)
        failed = cur.fetchone()[0]
 
        cur.execute("SELECT COUNT(*) FROM dbo.FactRevenueGeography")
        checked = cur.fetchone()[0]
 
        log_validation_result(
            run_id,
            "No Null Revenue",
            "Revenue fields must not be null",
            checked,
            checked - failed,
            failed
        )
    finally:
        conn.close()
 
def validate_date_coverage(run_id):
    conn, cur = connect_sql(DW_REVENUE)
    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM dbo.FactRevenueGeography f
            LEFT JOIN dbo.DimDate d
                ON f.DateKey = d.DateKey
            WHERE d.DateKey IS NULL
        """)
        failed = cur.fetchone()[0]
 
        cur.execute("SELECT COUNT(*) FROM dbo.FactRevenueGeography")
        checked = cur.fetchone()[0]
 
        log_validation_result(
            run_id,
            "Date Coverage",
            "All DateKeys must exist in DimDate",
            checked,
            checked - failed,
            failed
        )
    finally:
        conn.close()
 
def validate_customer_overlap(run_id):
    hotel_conn, hotel_cur = connect_sql(HOTEL_SRC)
    mongo = MongoClient(MONGO_URI)
    gift_db = mongo[MONGO_DB]
 
    try:
        hotel_cur.execute("""
            SELECT
                LOWER(LTRIM(RTRIM(first_name))) AS first_name,
                LOWER(LTRIM(RTRIM(last_name))) AS last_name,
                LOWER(LTRIM(RTRIM(city))) AS city,
                LOWER(LTRIM(RTRIM(state))) AS state,
                LOWER(LTRIM(RTRIM(zip))) AS zip
            FROM dbo.customer
            WHERE first_name IS NOT NULL
              AND last_name IS NOT NULL
              AND city IS NOT NULL
              AND state IS NOT NULL
              AND zip IS NOT NULL
        """)
        hotel_customers = {
            (r.first_name, r.last_name, r.city, r.state, r.zip)
            for r in hotel_cur.fetchall()
        }
 
        gift_customers = set()
        for doc in gift_db["transactions"].find(
            {},
            {
                "_id": 0,
                "customer.fname": 1,
                "customer.lname": 1,
                "customer.city": 1,
                "customer.state": 1,
                "customer.zip": 1,
            }
        ):
            cust = doc.get("customer", {})
            fname = (cust.get("fname") or "").strip().lower()
            lname = (cust.get("lname") or "").strip().lower()
            city = (cust.get("city") or "").strip().lower()
            state = (cust.get("state") or "").strip().lower()
            zip_code = (cust.get("zip") or "").strip().lower()
 
            if fname and lname and city and state and zip_code:
                gift_customers.add((fname, lname, city, state, zip_code))
 
        matched = len(hotel_customers.intersection(gift_customers))
        checked = len(gift_customers)
 
        log_validation_result(
            run_id,
            "Cross Source Customer Overlap",
            "Measures overlap between hotel and gift shop customers using first name, last name, city, state, and zip.",
            checked,
            matched,
            checked - matched
        )
    finally:
        hotel_conn.close()
        mongo.close()

if __name__ == "__main__":
    ensure_env()
    create_dgdb_tables()
 
    print("Starting ETL with DGDB logging...")
 
    run_id = start_etl_run("ETL for 3 Warehouses")
 
    total_processed = 0
    total_rejected = 0
 
    try:
        p, r = load_customer_spend_dw()
        total_processed += p
        total_rejected += r
 
        p, r = load_revenue_geography_dw()
        total_processed += p
        total_rejected += r
 
        p, r = load_employee_payroll_dw()
        total_processed += p
        total_rejected += r
 
        validate_customer_fk(run_id)
        validate_no_null_amounts(run_id)
        validate_date_coverage(run_id)
        validate_customer_overlap(run_id)
 
        end_etl_run(
            run_id,
            status="Success",
            records_processed=total_processed,
            records_rejected=total_rejected,
            notes="ETL completed successfully"
        )
 
        print("ETL complete with DGDB logging.")
 
    except Exception as e:
        end_etl_run(
            run_id,
            status="Failure",
            records_processed=total_processed,
            records_rejected=total_rejected,
            notes=str(e)
        )
        raise