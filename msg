Hey Guna, updated the PR (3125710) per your feedback - dropped the country parameter entirely. File is /CTRLFW/FDP/prd1/utils/fdp_adm_hold.sh (or ansible/project_files/utils/fdp_adm_hold.sh in the repo). Script now matches directly on task_id (e.g. 5198_AE), and if nothing matches, strips the trailing country suffix and retries against the base id - so services without country-specific rows still work exactly like before. Signature's untouched, still just env/type/task_id/ods.

Tested it - 5 scenarios on synthetic data, 2 more against a real copy of the prd1 hold file, all matching expected results including exit codes, not just the printed output.

While I've got you - a few things still open from earlier, and one of them might be more urgent than I first flagged:

1. country_prod_service_dict in fdp_products_dag_prd.py (/CTRLFW/FDP/prd1/tools_v2/executables/airflow_home/dags/fdp_products_dag_prd.py, line 21) - hand-filled like country_dict just above it, or loaded from a file like product_data below it? Don't want to guess and build the wrong shape.

2. Same file, line 377 - the country-suffix logic is commented out, right next to a "workaround, fix properly next release" note. Know why it was disabled? Don't want to re-enable it blind if it broke something before.

3. This one's the one I want to flag clearly - the actual hold data file, /CTRLFW/FDP/prd1/data/mfu/incoming/adm_hold_*.csv, already has country-suffixed rows signed off by name - 5198_AE ("Confirmed by KK"), ACCT-EBBS-ACCOUNT_AE ("as per Rajan this service need decom"), a few others with real names attached. Since fdp_adm_hold.sh had no country awareness until this fix, and nothing has ever called it with a country-suffixed id, these specific rows were most likely never actually matching anything - meaning whatever these people thought was on hold may have been flowing through unheld this whole time. Wanted to check if this was already known, or if it's new - changes how urgent this is.

Ingestion side I can start scoping once the PR's merged. Daas side needs 1 and 2 answered first. Point 3 might be worth a look regardless of timeline.
