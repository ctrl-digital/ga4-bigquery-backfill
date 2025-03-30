# GA4 Data Backfill to BigQuery

**Author:** Developed by Gabriel Rask during a thesis project at Ctrl Digital  
**Project Sponsor:** Ted Solomon

This Python script fetches data from Google Analytics 4 (GA4) using the Google Analytics Data API and inserts it into Google BigQuery. It is designed to backfill data based on a specified date range.


## Overview

This project supports two common authentication modes:

- **Local Development:**  
  Use your personal credentials via Application Default Credentials (ADC). This is great for testing and exploration.
  
- **Production/Automation:**  
  Use a dedicated service account that has the necessary permissions for both fetching data from GA4 and writing data to BigQuery.


## Requirements

- Python 3.6+
- Google Cloud SDK (for authentication and project management)
- Required Python libraries (see below)

### Install Required Python Libraries
You can install the necessary libraries by running the following command:

```bash
pip install google-analytics-data google-cloud-bigquery google-auth
```

### Environment Variables

The script requires the following environment variables to be set:

1. `GA4_PROPERTY_ID` - The Google Analytics 4 property ID for the GA4 project.
2. `BQ_PROJECT_ID` - The Google Cloud project ID where your BigQuery dataset is located.
3. `BQ_DATASET_ID` - The BigQuery dataset where the data will be stored.
4. `DEST_TABLE_ID` - The BigQuery table where the data will be inserted.

Example of setting environment variables on Linux/macOS:
```bash
export GA4_PROPERTY_ID="YOUR_GA4_PROPERTY_ID"
export BQ_PROJECT_ID="YOUR_PROJECT_ID"
export BQ_DATASET_ID="YOUR_DATASET_ID"
export DEST_TABLE_ID="YOUR_TABLE_ID"
```

On Windows, you can set environment variables using:
```bash
set GA4_PROPERTY_ID=YOUR_GA4_PROPERTY_ID
set BQ_PROJECT_ID=YOUR_PROJECT_ID
set BQ_DATASET_ID=YOUR_DATASET_ID
set DEST_TABLE_ID=YOUR_TABLE_ID
```


## Google Cloud Permissions

Before running the script, make sure the **Google Analytics Data API** is enabled in your Google Cloud project. You can enable it by visiting:

https://console.developers.google.com/apis/api/analyticsdata.googleapis.com/overview?project=YOUR_PROJECT_ID

Replace `YOUR_PROJECT_ID` with your actual project ID, then click “Enable.”


### Authentication and Permissions

The script uses **Service Account** credentials (recommended) or your personal account credentials via ADC for local testing.

The acccount used must have following permissions:
- `Viewer` role on GA4 property.
- `bigquery.dataEditor` role on BigQuery.


```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```


### GA4 Data Export Setup

Make sure that you have set up your GA4 property correctly, and that you have the correct dimensions and metrics configured in your report. The script fetches the following dimensions and metrics:

**Dimensions**:
- `date`
- `deviceCategory`
- `sessionCampaignId`
- `sessionCampaignName`
- `sessionSourceMedium`
- `transactionId`
- `eventName`

**Metrics**:
- `ecommercePurchases`
- `sessions`
- `totalRevenue`
- `keyEvents:add_to_cart`
- `keyEvents:purchase`
- `keyEvents:begin_checkout`
- `eventValue`


**See also these websites:**
- https://ga-dev-tools.google/ga4/query-explorer/
- https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/


## Usage

1. **Start the script** by running the main function, which triggers fetching and inserting GA4 data into BigQuery. The `backfill_data_from_GA4_to_BQ` function takes a `POST` request, which should provide the `start_date` and `end_date` for the data range to be processed.

### Example of running it with Flask or any HTTP framework:
You can expose the function via an HTTP endpoint in a web service such as Flask or FastAPI for easier triggering.

### Local testing
For local testing, the script includes a dummy request simulation:

```
# Local testing: simulate HTTP request
if __name__ == "__main__":
    class localRequest:
        def get_json(self, silent=True):
            return {"start_date": "2024-05-01", "end_date": "2024-05-01"}
    
    backfill_data_from_GA4_to_BQ(localRequest())
```    

### Data Fetching Process

- The script loops over each day in the specified date range and fetches data from GA4 via `RunReportRequest` using the **Google Analytics Data API**.
- The script then processes this data and batches it for insertion into BigQuery.

### BigQuery Insert

After fetching the data, it will be inserted into a **BigQuery table** using `insert_rows_json`.

Make sure to specify the BigQuery table schema beforehand:

```
[
  { "name": "date", "type": "STRING", "mode": "NULLABLE" },
  { "name": "device", "type": "STRING", "mode": "NULLABLE" },
  { "name": "campaign_id", "type": "STRING", "mode": "NULLABLE" },
  { "name": "campaign_name", "type": "STRING", "mode": "NULLABLE" },
  { "name": "source_medium", "type": "STRING", "mode": "NULLABLE" },
  { "name": "transaction_id", "type": "STRING", "mode": "NULLABLE" },
  { "name": "event_name", "type": "STRING", "mode": "NULLABLE" },
  { "name": "ecommerce_purchases", "type": "INTEGER", "mode": "NULLABLE" },
  { "name": "sessions", "type": "INTEGER", "mode": "NULLABLE" },
  { "name": "total_revenue", "type": "FLOAT", "mode": "NULLABLE" },
  { "name": "event_value", "type": "FLOAT", "mode": "NULLABLE" },
  { "name": "event_add_to_cart", "type": "INTEGER", "mode": "NULLABLE" },
  { "name": "event_purchase", "type": "INTEGER", "mode": "NULLABLE" },
  { "name": "event_begin_checkout", "type": "INTEGER", "mode": "NULLABLE" }
]
```

## Logging

The script uses the Python `logging` module to log the following events:
- Data fetching progress
- Errors while processing each row of data
- Success or failure of the BigQuery insertion


## Important Notes

1. **API Limits**: The Google Analytics Data API has rate limits. If you are processing a large amount of data, ensure you do not hit the rate limit. You may need to implement pagination if you exceed the `250,000` row limit in a single request.

2. **Date Ranges**: The script fetches data one day at a time and processes it in daily batches. Ensure the date range provided is not too large, as fetching a year’s worth of data could potentially be slow.

3. **Missing Data**: If certain data fields are missing or not available, ensure that the GA4 property is correctly configured and includes the necessary dimensions and metrics.

4. **Error Handling**: In case of API or BigQuery errors, the script logs detailed error messages. Review these logs for debugging purposes if something goes wrong.

5. **Rate Limiting**: If you are fetching a large number of rows, consider implementing retry logic or backoff mechanisms for error handling.


## Future Enhancements

- **Retry Logic**: Implement a retry mechanism in case of temporary failures in the GA4 API or BigQuery insertion.
- **Paginering**: Handle the case where more than `250,000` rows are fetched from the GA4 API by implementing pagination.
- **Error Notifications**: Add functionality to send email or Slack notifications if errors occur.


### Sample Output

If the process runs successfully, you will see logs like:

```
2024-02-01 - INFO - Fetching data for 2024-02-01
2024-02-01 - INFO - API response received for 2024-02-01
2024-02-01 - INFO - Successfully inserted 500 rows into BigQuery.
```

If errors occur, you will see:

```
2024-02-01 - ERROR - Error processing row for 2024-02-01: <error message>
2024-02-01 - ERROR - Failed to insert data into BigQuery: <error message>
```

### Happy data backfilling!