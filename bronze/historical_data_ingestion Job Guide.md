# Databricks Job Setup Guide: Historical Data Ingestion
This guide documents the configuration for the **"Bronze Layer - Historical Data Load"** job in Databricks Workflows. This job ingests the year-wise historical CSV files (Ratings, Tags) into the Bronze layer as External Delta Tables.

---

## Step 1: Job Configuration
1.  Navigate to **Workflows** (Jobs icon) in the Databricks sidebar.
2.  Click **Create Job**.
3.  **Job Name:** `bronze_historical_data_ingestion`.

---

## Step 2: Configure Email Notifications
To receive alerts when the job finishes or fails:

1.  On the **Job Details** panel (right side of the screen), look for the **"Notifications"** section.
2.  Click **+ Add Notification**.
3.  **On Success:** Enter your email address (to confirm the load completed).
4.  **On Failure:** Enter your email address (critical for immediate debugging).
5.  Click **Add**.

---

## Step 3: Configure Tasks
Create **2 Parallel Tasks** pointing to the **same notebook**: `bronze_historical_data_load`.
**Note:** The `catalog_name` and `schema_name` parameters default to `movielens` and `bronze` in the code, so you only need to provide the parameters listed below for each task.

### Task 1: Load Ratings
* **Task Name:** `Load_Ratings`
* **Type:** Notebook
* **Path:** Select `bronze_historical_data_load`
* **Parameters:**
    * `table_name`: `ratings`
    * `start_year`: `YYYY`
    * `end_year`: `YYYY`
    * `s3_source_path`: `YOUR S3 PATH`
    * `s3_target_path`: `YOUR S3 PATH`

### Task 2: Load Tags
* **Task Name:** `Load_Tags`
* **Type:** Notebook
* **Path:** Select `bronze_historical_data_load`
* **Parameters:**
    * `table_name`: `tags`
    * `start_year`: `YYYY`
    * `end_year`: `YYYY`
    * `s3_source_path`: `YOUR S3 PATH`
    * `s3_target_path`: `YOUR S3 PATH`

---

## Step 4: Execution & Verification
1.  **Dependencies:** Ensure both tasks are set to run in parallel (no dependency arrows between them in the visual graph).
2.  **Run:** Click the blue **Run now** button in the top right corner.
3.  **Monitor:** You will receive an email notification when the job starts and finishes (as configured in Step 2).
4.  **Verify:**
    * Wait for all tasks to turn **Green**.
    * Go to **Catalog Explorer** → `movielens` → `bronze`.
    * Confirm both tables exist and contain data.
    * Click "Sample Data" on `ratings` and `tags` to verify multi-year ingestion.

---
