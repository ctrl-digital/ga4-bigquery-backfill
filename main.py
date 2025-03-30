import json
import os
import logging
import time
from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange
from google.cloud import bigquery
from google.auth import default
from google.api_core.exceptions import InvalidArgument, NotFound

# Load environment variables
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")
DEST_TABLE_ID = os.getenv("DEST_TABLE_ID")

if not all([GA4_PROPERTY_ID, BQ_PROJECT_ID, BQ_DATASET_ID, DEST_TABLE_ID]):
    raise ValueError("ERROR: Missing one or more required environment variables.")

dest_table_path = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{DEST_TABLE_ID}"

# Initialize Google Clients
credentials, project_id = default()
ga4_client = BetaAnalyticsDataClient(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_and_insert_ga4_data(start_date, end_date):
    """Fetch GA4 data one day at a time and insert it into BigQuery."""
    dimensions = [
        "date", "deviceCategory", "sessionCampaignId", "sessionCampaignName", 
        "sessionSourceMedium", "transactionId", "eventName"
    ]
    metrics = [
        {"name": "ecommercePurchases"},
        {"name": "sessions"},
        {"name": "totalRevenue"},
        {"name": "keyEvents:add_to_cart"},
        {"name": "keyEvents:purchase"},
        {"name": "keyEvents:begin_checkout"},
        {"name": "eventValue"}
    ]
    
    date_cursor = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    
    while date_cursor <= end_date_obj:
        single_day = date_cursor.strftime("%Y-%m-%d")
        logging.info(f"Fetching data for {single_day}")
        
        report_request = RunReportRequest(
            property=f"properties/{GA4_PROPERTY_ID}",
            date_ranges=[DateRange(start_date=single_day, end_date=single_day)],
            dimensions=[{"name": dim} for dim in dimensions],
            metrics=metrics,
            limit=250000  # Consider handling pagination if the number of rows exceeds 250,000
        )
        
        try:
            response = ga4_client.run_report(report_request)
            logging.info(f"API response received for {single_day}")

            batch_data = []
            for row in response.rows:
                try:
                    batch_data.append({
                        "date": row.dimension_values[0].value,
                        "device": row.dimension_values[1].value,
                        "campaign_id": row.dimension_values[2].value,
                        "campaign_name": row.dimension_values[3].value,
                        "source_medium": row.dimension_values[4].value,
                        "transaction_id": row.dimension_values[5].value,
                        "event_name": row.dimension_values[6].value,
                        "ecommerce_purchases": int(row.metric_values[0].value),
                        "sessions": int(row.metric_values[1].value),
                        "total_revenue": float(row.metric_values[2].value),
                        "event_value": float(row.metric_values[6].value),
                        "event_add_to_cart": int(row.metric_values[3].value),
                        "event_purchase": int(row.metric_values[4].value),
                        "event_begin_checkout": int(row.metric_values[5].value)
                    })
                except (IndexError, ValueError) as e:
                    logging.error(f"Error processing row for {single_day}: {e}")
                    continue

            if batch_data:
                insert_data_into_bigquery(batch_data)
        except Exception as e:
            logging.error(f"Error fetching data for {single_day}: {e}")

        date_cursor += timedelta(days=1)

def insert_data_into_bigquery(batch_data):
    """Insert batch data into BigQuery."""
    try:
        errors = bq_client.insert_rows_json(dest_table_path, batch_data)
        if errors:
            logging.error(f"BigQuery insertion errors: {errors}")
        else:
            logging.info(f"Successfully inserted {len(batch_data)} rows into BigQuery.")
    except Exception as e:
        logging.error(f"Failed to insert data into BigQuery: {e}")

def backfill_data_from_GA4_to_BQ(request):
    """Trigger function to fetch and insert GA4 data into BigQuery."""
    req_data = request.get_json(silent=True) or {}
    # default values for start- and end date
    start_date = req_data.get("start_date", "2024-05-01")
    end_date = req_data.get("end_date", "2024-05-01")
    logging.info(f"Processing data from {start_date} to {end_date}")
    fetch_and_insert_ga4_data(start_date, end_date)
    return json.dumps({"status": "Data processing complete."})


# For local testing: simulate an HTTP request to trigger backfill_data_from_GA4_to_BQ
if __name__ == "__main__":
    class localRequest:
        def get_json(self, silent=True):
            return {
                "start_date": "2024-05-01",
                "end_date": "2024-05-01"
            }

    backfill_data_from_GA4_to_BQ(localRequest())