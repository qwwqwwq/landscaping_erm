"""Export contractor attrition reports from BigQuery to CSV files."""

from google.cloud import bigquery
import os
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = bigquery.Client(project="landscaping-erm")
TABLE = "`landscaping-erm`.erm_dbt_marts.mart_contractor_attrition"
TODAY = datetime.now().strftime("%Y-%m-%d")

REPORTS = {
    "report1_10yr_active_1yr_inactive": {
        "filter": "attrition_10yr_inactive_1yr = true",
        "description": "Contractors active in last 10 years, no order in past 1 year",
    },
    "report2a_5yr_active_6mo_inactive": {
        "filter": "attrition_5yr_inactive_6mo = true",
        "description": "Contractors active in last 5 years, no order in past 6 months",
    },
    "report2b_5yr_active_1yr_inactive": {
        "filter": "attrition_5yr_inactive_1yr = true",
        "description": "Contractors active in last 5 years, no order in past 1 year",
    },
    "report2c_5yr_active_2yr_inactive": {
        "filter": "attrition_5yr_inactive_2yr = true",
        "description": "Contractors active in last 5 years, no order in past 2 years",
    },
}

COLUMNS = [
    "customer_name",
    "best_phone_number",
    "email",
    "billing_city",
    "billing_state",
    "classification_reason",
    "is_house_account",
    "salesperson_code",
    "total_orders",
    "lifetime_revenue",
    "avg_order_value",
    "first_order_date",
    "last_order_date",
    "days_since_last_order",
    "months_since_last_order",
]

for report_name, report_config in REPORTS.items():
    query = f"""
    select {', '.join(COLUMNS)}
    from {TABLE}
    where {report_config['filter']}
    order by lifetime_revenue desc
    """
    df = client.query(query).to_dataframe()
    filename = f"{report_name}_{TODAY}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_csv(filepath, index=False)
    print(f"{report_config['description']}")
    print(f"  -> {filename} ({len(df)} contractors)\n")

# Also export full mart for ad-hoc analysis
query = f"select * from {TABLE} order by lifetime_revenue desc"
df = client.query(query).to_dataframe()
filepath = os.path.join(OUTPUT_DIR, f"all_contractor_attrition_{TODAY}.csv")
df.to_csv(filepath, index=False)
print(f"Full attrition dataset: all_contractor_attrition_{TODAY}.csv ({len(df)} contractors)")
