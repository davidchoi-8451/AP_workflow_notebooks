Where to find individual period outputs:
Basket, store level output - https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/bask_store_files
Dept, store level output - https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/dept_store_files
Once you run a notebook for a specific period or a single/multi-period job, the combined file will be found within the “send” folder in Azure: 
tmkrprtnrs-users - Microsoft Azure (i920466/ap-pos-feed/send)

Data Feed Core POS:
01_cmm_pos_feed_adweek - Databricks
Single Period and Multi-Period CORE POS Jobs:
CMM POS Feed BAU - Single-Group - Databricks
CMM POS Feed BAU - Multi-Group - Databricks
Validation:
Validate Period Updates - Databricks

Data Feed OMNI:
Cloned_01_cmm_pos_feed_OMNI_hhs_david_update_april_2026 - Databricks
Single Period and Multi-Period OMNI Jobs:
CMM POS Feed Omnichannel - Single-Group - Databricks
CMM POS Feed Omnichannel - Multi-Group - Databricks
Validation:
Validate Omnichannel Updates - Databricks

Data Feed ECOMM:
Cloned_01_cmm_pos_feed_ECOMM_hhs_david_update_april_2026 - Databricks
Single Period and Multi-Period ECOMM Jobs:
CMM POS Feed Ecomm - Single-Group - Databricks
CMM POS Feed Ecomm - Multi-Group - Databricks
Validation:
Validate Ecomm - Databricks

Competitor Feed:
035_competitor_effogeo - Databricks
-	Use Cell 8 instead of 7 for fiscal date initialization when running restatement – otherwise use cell 7 for single period competitor pull
Validation:
Validate Competitor - Databricks

Storeless/Birmingham Feed:
Cloned_01_cmm_pos_feed_storeless_hhs_david_update_april_2026 - Databricks
-	Export output of Cell 42 into Excel. Should be 4 rows per period (1 row per Fiscal Week of Birmingham fulfillment center store code 540_00042)
Validation:
Validate_Storeless_New - Databricks

Division Level KPI Tree:
Geography Deep Dive - Power BI
-	Geography Breakdown, Link above is to Bookmarked report that I built within the MIR dashboard.
-	Point of contact: Tripp Patterson

AP / Mutinex will ask for data feeds every period, including restatements a few times a year. In addition, if any data feed logic were to be changed (such as department to sub-department, granularity of data, etc.), a restatement will need to happen as well. Once all data feeds are delivered, we will get a file back for QA. Not sure if Mutinex will send us QA files in the same format as AP, so the validation notebooks may need to be changed (?)
Data Validation Steps:
1.	Once AP receives the files, they will process and then send us data validation files for each of the 4 runs (expect them in 2-4 days)
2.	These files are uploaded to the Adapta portal
3.	I download, and manually upload these files to the azure portal location https://sa8451krprtnrdev.blob.core.windows.net/tmkrprtnrs-users/l920466/ap-pos-feed/qa/  {create folder with fiscal month of runs here}
4.	In the QA/fiscal month folder, create folders labeled as follows and upload the correct file to each folder
a.	Core POS (the file will be named POS Data Review) goes in a folder named from-ap 
b.	Ecomm HHs by preferred division (the file will be named Ecomm HHs POS Data Review) goes in a folder named from-ap-ecom
c.	Omnichannel HHs data (the file will be named Omni HHs POS Data Review) goes in a folder named from-ap-omni
d.	Competitor data review files go in a folder named from-ap-comp
e.	Storeless data review files go in a folder named from-ap-storeless.
5.	To run the data validation notebooks, you’ll need to change the run date in the parameters to the date the workflow ran (ignore the fiscal date unless you had to run the notebook multiple times) and ensure all the paths to the QA folder are properly updated with the correct fiscal month. After that, you should be able to run, and if it runs through without erroring out, the data is validated. 
