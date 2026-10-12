# CMM POS Data Feeds – README

## Overview

All files are located in this Databricks repo:
[CMM POS Feed Repo](https://adb-291758323461480.0.azuredatabricks.net/browse/folders/4399643338160207?o=291758323461480)

This repo provides **visibility into which files are used for which workflows**, along with their associated **validation notebooks**.
- **Note:** This Databricks repo is **not** set up as a Git repo. The repo is under I920466 (Lovleen Mishra)'s directory, in the ap-feed-hhs repo. If there are data feed changes to be made, clone the notebook corresponding to the data feed that needs adjustment, and test changes there.

---

## Important Notes

### Making Code Changes (Client Requests)
1. **Clone** the existing notebook for the specific data feed **inside this Databricks repo**.
2. **Test all changes in the cloned file first.** Do not edit the original notebook directly.

Simple Summary: Main notebooks are used to generate basket and department level reports (some notebooks only generate one file, such as competitive and storeless). Group Files is the notebook that contains the path of the main notebooks to run in the databricks job. 
Validation notebooks are used (instructions below) to validate that the client's numbers are accurate.

### Jobs & Control Notebooks
- Jobs point to the **specific notebooks** listed below.
- **Every period** during data feed deliveries, confirm that each Job points to the **correct notebook**.
- If changes are made due to client requests, **update the Jobs** to point to the new notebooks.
- To check which notebook a Job uses, review and adjust (if needed) the **filepath in each data feed's control notebook**.

> **Example:** `multi-week-control-nb` runs during the Job and points to one of the files in this repo, depending on which data feed is being run. If any changes are made, make sure it points to the correct file.

---

## Azure Output Locations

| Output | Location |
|---|---|
| **Basket, store-level** (per period) | https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/bask_store_files |
| **Dept, store-level** (per period) | https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/dept_store_files |
| **Combined file** (after running a notebook or single/multi-period Job) | `tmkrprtnrs-users/l920466/ap-pos-feed/send` |

---

## Data Feeds

### 1. Core POS
- **Notebook:** `01_cmm_pos_feed_adweek`
- **Jobs:**
  - CMM POS Feed BAU – Single-Group
  - CMM POS Feed BAU – Multi-Group
- **Validation:** Validate Period Updates

### 2. Omnichannel (OMNI)
- **Notebook:** `Cloned_01_cmm_pos_feed_OMNI_hhs_david_update_april_2026`
- **Jobs:**
  - CMM POS Feed Omnichannel – Single-Group
  - CMM POS Feed Omnichannel – Multi-Group
- **Validation:** Validate Omnichannel Updates

### 3. Ecommerce (ECOMM)
- **Notebook:** `Cloned_01_cmm_pos_feed_ECOMM_hhs_david_update_april_2026`
- **Jobs:**
  - CMM POS Feed Ecomm – Single-Group
  - CMM POS Feed Ecomm – Multi-Group
- **Validation:** Validate Ecomm

### 4. Competitor
- **Notebook:** `035_competitor_effogeo`
- **Fiscal date initialization:**
  - **Single-period pull:** use **Cell 7**
  - **Restatement:** use **Cell 8** (instead of Cell 7)
- **Validation:** Validate Competitor

### 5. Storeless / Birmingham
- **Notebook:** `Cloned_01_cmm_pos_feed_storeless_hhs_david_update_april_2026`
- **Output:** Export the output of **Cell 42** to Excel.
  - Expect **4 rows per period**, one row per fiscal week for the Birmingham fulfillment center (store code **`540_00042`**).
- **Validation:** Validate_Storeless_New

---

## Division Level KPI Tree
- **Report:** Geography Deep Dive (Power BI)
- **Details:** Geography breakdown. The link goes to a bookmarked report built within the **MIR dashboard**.
- **Point of Contact:** Tripp Patterson
- https://app.powerbi.com/groups/me/apps/c2f93e3c-4941-4326-9bb7-21bd0414c998/reports/e2953654-b692-4ce5-a738-8069a04778e0/56ba1c4b636509354045?ctid=5f9dc6bd-f38a-454a-864c-c803691193c5&experience=power-bi

---

## Delivery Cadence
- **AP / Mutinex** request data feeds **every period**.
- **Restatements** are requested a few times a year.
- A **restatement is also required** whenever data feed logic changes (e.g., department → sub-department, change in data granularity).
- After all feeds are delivered, a **QA file** is sent back.

> **NOTE:** It's not yet confirmed whether Mutinex will send QA files in the same format as AP. The validation notebooks may need to be updated.

---

## Data Validation (QA) Process

### Step 1: Receive QA Files
After processing, AP sends **data validation files for each of the 4 runs** (expect them within **2–4 days**). These files are uploaded to the **Adapta portal**.

### Step 2: Upload to Azure
1. Download the files from the Adapta portal.
2. Manually upload them to:
   https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/qa/
3. **Create a folder named for the fiscal month** of the runs.

### Step 3: Organize Files by Feed
Inside the fiscal month folder, create the following subfolders and upload each file to the matching one:

| Feed | File Name | Folder |
|---|---|---|
| **Core POS** | POS Data Review | `from-ap` |
| **Ecomm HHs** (by preferred division) | Ecomm HHs POS Data Review | `from-ap-ecom` |
| **Omnichannel HHs** | Omni HHs POS Data Review | `from-ap-omni` |
| **Competitor** | Competitor Data Review | `from-ap-comp` |
| **Storeless** | Storeless Data Review | `from-ap-storeless` |

### Step 4: Run Validation Notebooks
1. Set the **run date** parameter to the **date the workflow ran**.
   - Ignore the fiscal date parameter **unless** the notebook had to be run multiple times.
2. Update **all QA folder paths** to the correct **fiscal month**.
3. Run the notebook. **If it completes without errors, the data is validated.** 
