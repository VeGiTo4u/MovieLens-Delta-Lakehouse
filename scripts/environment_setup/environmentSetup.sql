-- =========================================================
-- MovieLens Delta Lakehouse - Environment Bootstrap Script
-- Purpose: Initial Unity Catalog and Storage Permission Setup
-- This script prepares the catalog, schemas, and external
-- storage permissions required before any data ingestion.
-- =========================================================

-- ---------------------------------------------------------
-- STEP 1: Create a dedicated catalog for the MovieLens project.
-- A catalog is the top-level container in Unity Catalog.
-- It isolates all project assets (schemas, tables, views).
-- ---------------------------------------------------------
CREATE CATALOG IF NOT EXISTS movieLens;

-- ---------------------------------------------------------
-- STEP 2: Switch context to the MovieLens catalog.
-- Ensures all subsequent schema and table operations
-- happen inside this catalog instead of the default one.
-- ---------------------------------------------------------
USE CATALOG movieLens;

-- ---------------------------------------------------------
-- STEP 3: Create Medallion Architecture Schemas.
-- Each schema represents a logical data layer.
--
-- bronze → Raw ingested data (append-only)
-- silver → Cleaned & standardized data
-- gold   → Analytical/star schema & aggregates
-- ---------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------------------------------------------------
-- STEP 4: Grant External Location Permissions.
-- External locations map Unity Catalog to S3 folders.
-- Permissions allow Databricks users to read/write data.
--
-- 'account users' is a built-in Databricks group that
-- represents all workspace users.
-- For production, this would be replaced with
-- role-based groups like data_engineers / analysts.
-- ---------------------------------------------------------

-- Raw Layer (Source Files / Immutable Backups)
GRANT ALL PRIVILEGES
ON EXTERNAL LOCATION ext_raw_data
TO `account users`;

-- Bronze Layer (Raw Delta Tables / Append Only)
GRANT ALL PRIVILEGES
ON EXTERNAL LOCATION ext_bronze_data
TO `account users`;

-- Silver Layer (Cleaned / Typed / Validated Tables)
GRANT ALL PRIVILEGES
ON EXTERNAL LOCATION ext_silver_data
TO `account users`;

-- Gold Layer (Star Schema / Aggregates / BI Tables)
GRANT ALL PRIVILEGES
ON EXTERNAL LOCATION ext_gold_data
TO `account users`;

-- =========================================================
-- After this script:
-- 1. Catalog and schemas are ready.
-- 2. S3 storage folders are accessible.
-- 3. Delta tables can be created without permission errors.
-- 4. Pipeline execution (Bronze → Silver → Gold) is enabled.
-- =========================================================