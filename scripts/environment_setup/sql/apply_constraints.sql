-- =========================================================
-- MovieLens Delta Lakehouse - Native Delta Constraints
-- Purpose: Replaces PySpark post-write validation with 
-- natively enforced Delta Lake constraints.
--
-- Note: Primary Key uniqueness is guaranteed by the ETL 
-- pipeline's MERGE and row_number() logic. These constraints 
-- guarantee ETL metadata completeness on write.
-- =========================================================

USE CATALOG movieLens;

-- ---------------------------------------------------------
-- BRONZE LAYER CONSTRAINTS
-- ---------------------------------------------------------
-- movies
ALTER TABLE bronze.movies ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.movies ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.movies ALTER COLUMN _source_system SET NOT NULL;

-- links
ALTER TABLE bronze.links ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.links ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.links ALTER COLUMN _source_system SET NOT NULL;

-- genome_scores
ALTER TABLE bronze.genome_scores ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.genome_scores ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.genome_scores ALTER COLUMN _source_system SET NOT NULL;

-- genome_tags
ALTER TABLE bronze.genome_tags ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.genome_tags ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.genome_tags ALTER COLUMN _source_system SET NOT NULL;

-- ratings (Incremental)
ALTER TABLE bronze.ratings ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.ratings ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.ratings ALTER COLUMN _source_system SET NOT NULL;
ALTER TABLE bronze.ratings ALTER COLUMN _batch_year SET NOT NULL;

-- tags (Incremental)
ALTER TABLE bronze.tags ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE bronze.tags ALTER COLUMN _notebook_path SET NOT NULL;
ALTER TABLE bronze.tags ALTER COLUMN _source_system SET NOT NULL;
ALTER TABLE bronze.tags ALTER COLUMN _batch_year SET NOT NULL;

-- ---------------------------------------------------------
-- SILVER LAYER CONSTRAINTS
-- ---------------------------------------------------------
-- movies
ALTER TABLE silver.movies ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.movies ALTER COLUMN _dq_status SET NOT NULL;

-- links
ALTER TABLE silver.links ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.links ALTER COLUMN _dq_status SET NOT NULL;

-- genome_scores
ALTER TABLE silver.genome_scores ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.genome_scores ALTER COLUMN _dq_status SET NOT NULL;

-- genome_tags
ALTER TABLE silver.genome_tags ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.genome_tags ALTER COLUMN _dq_status SET NOT NULL;

-- ratings
ALTER TABLE silver.ratings ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.ratings ALTER COLUMN _dq_status SET NOT NULL;
ALTER TABLE silver.ratings ALTER COLUMN _batch_year SET NOT NULL;

-- tags
ALTER TABLE silver.tags ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE silver.tags ALTER COLUMN _dq_status SET NOT NULL;
ALTER TABLE silver.tags ALTER COLUMN _batch_year SET NOT NULL;

-- ---------------------------------------------------------
-- GOLD LAYER CONSTRAINTS
-- ---------------------------------------------------------
ALTER TABLE gold.dim_movies ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.dim_genres ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.dim_genome_tags ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.dim_external_links ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.dim_date ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.fact_ratings ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.fact_genome_scores ALTER COLUMN _job_run_id SET NOT NULL;
ALTER TABLE gold.bridge_movies_genres ALTER COLUMN _job_run_id SET NOT NULL;
