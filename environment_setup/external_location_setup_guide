# Databricks Free Edition — External Location Setup (Manual UI Method)

**Project:** MovieLens Delta Lakehouse  
**Goal:** Configure secure S3 access from a Databricks Free Edition workspace using Unity Catalog External Locations — via the Databricks UI (no Quickstart / no CloudFormation).

---

## Quick summary
1. Create a single S3 bucket and folders for the Medallion layers (raw / bronze / silver / gold).  
2. Create an AWS IAM Role with least-privilege S3 permissions.  
3. In Databricks, create External Locations and a Storage Credential (paste the IAM role ARN).  
4. Copy the Databricks-generated trust policy into the role's Trust Relationship in AWS.  
5. Grant permissions inside Databricks for users to read/write the External Locations.  
6. Validate by creating a Delta table on the External Location.

---

## Prerequisites
- AWS account with IAM and S3 privileges to create roles/policies and a bucket.
- Databricks Free or Trial workspace with Unity Catalog enabled.
- Permissions in Databricks to create Catalog External Locations and grant permissions.

---

## 1 — S3 bucket and folder structure
Create one S3 bucket (example name: `project-data`). Inside it create folders for the Medallion architecture:

- project-data/
  - raw/
  - bronze/
  - silver/
  - gold/

These folders map to raw → bronze → silver → gold Delta layers.

---

## 2 — Create an IAM role (least-privilege)
1. In AWS Console: IAM → Roles → Create role.  
2. Role type: choose AWS service → EC2 (works for Databricks Free Edition).  
3. Attach an inline S3 policy with least privilege (example below).  
4. Name the role (example: `databricks-data-role`).  
5. Copy the Role ARN (you'll paste this into Databricks).

Example inline policy (replace `project-data` with your bucket name):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::project-data"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::project-data/*"
    }
  ]
}
```

Note: if you use prefix restrictions (e.g., only `bronze/`), scope the ARN to `arn:aws:s3:::project-data/bronze/*` and include a `Condition` for the bucket listing as needed.

---

## 3 — Create External Location in Databricks (create storage credential)
1. Databricks Workspace → Data → Catalog → External Data → External Locations → Create.  
2. Fill the form:
   - Name: `ext_bronze_data` (example)
   - Storage Type: S3
   - URL: `s3://project-data/bronze/`
3. Storage Credential:
   - Click **Create new**
   - Paste the IAM Role ARN (from step 2)
   - Save
4. Do NOT close the Databricks creation dialog yet — Databricks will show a Trust Policy JSON that must be copied into AWS.

---

## 4 — Apply Databricks trust policy in AWS (critical)
After requesting the Storage Credential, Databricks provides a Trust Policy JSON. To complete the setup:

1. In AWS Console → IAM → Roles → open the role you created (e.g., `databricks-data-role`).  
2. Go to **Trust relationships** → **Edit trust relationship**.  
3. Replace the existing trust policy JSON with the policy provided by Databricks (paste exactly).  
4. Save.

This allows Databricks to assume the role securely. Return to Databricks and finish creation — Databricks will validate the External Location using the updated trust relationship.

Tip: if Databricks fails to validate, double-check that:
- The role ARN in the Databricks storage credential exactly matches the role in AWS.
- The trust policy pasted in AWS is the one provided by Databricks (no extra characters).
- AWS role has the inline S3 permissions from step 2.

---

## 5 — Create External Locations for every layer
Repeat the External Location creation for each folder:

- ext_raw_data  → s3://project-data/raw/
- ext_bronze_data → s3://project-data/bronze/
- ext_silver_data → s3://project-data/silver/
- ext_gold_data → s3://project-data/gold/

You can reuse the same IAM role / storage credential for all External Locations if the role has access to the entire bucket, or create separate credentials with tighter scoping per layer.

---

## 6 — Grant Databricks permissions on External Locations
For each External Location:
1. Catalog → External Locations → select the location → Permissions → Grant.  
2. Grant to: `account users` (or specific groups/groups-of-users).  
3. Give privileges:
   - READ FILES
   - WRITE FILES
   - (or ALL PRIVILEGES for demos)

Adjust privileges according to your security requirements.

---

## 7 — Validation
Run this in Databricks SQL to test writing a Delta table to the External Location path (adjust catalog/schema as needed):

```sql
CREATE TABLE movielens.bronze.test_ext_table
USING DELTA
LOCATION 's3://project-data/bronze/'
AS SELECT 1;
```

Then read it back:

```sql
SELECT * FROM movielens.bronze.test_ext_table;
```

If the CREATE TABLE / SELECT succeeds, Databricks has write/read access to the S3 External Location.

Expected S3 layout after the test:

```
s3://project-data/bronze/test_ext_table/
 ├── _delta_log/
 └── part-00000-*.snappy.parquet
```

---

## 8 — Troubleshooting checklist
- Validation error in Databricks when creating External Location:
  - Ensure the IAM role's Trust Relationship contains the Databricks-provided JSON.
  - Confirm the role ARN matches exactly.
  - Confirm the role has S3 permissions for the specified bucket/path.
- Access denied errors when reading/writing:
  - Verify that Databricks users/groups have READ/WRITE FILES privileges on the External Location.
  - Confirm the S3 bucket policy (if present) doesn't block access.
- Typos in S3 path: ensure `s3://project-data/bronze/` uses the exact bucket name and path.

---

## 9 — Best practices & notes
- Use one bucket per project to reduce cross-project permission complexity.
- Scope the IAM S3 permissions to the minimal prefixes required (e.g., only `bronze/*`) when feasible.
- The trust policy must be the Databricks-provided JSON — it contains the Databricks account identifiers required for AssumeRole.
- Unity Catalog manages logical table names; physical files reside in S3 under the External Location path.
- Delta tables always create `_delta_log` and may produce multiple parquet files — that's expected.

---

## Outcome
After completing these steps:
- Databricks can securely read/write the S3 paths via Unity Catalog External Locations.
- Unity Catalog governance is enforced for those locations.
- You can safely build Bronze → Silver → Gold Delta Lake pipelines.

---
