# Spark Configuration constants
SPARK_DRIVER_MEMORY: str = "4g"
SPARK_EXECUTOR_MEMORY: str = "4g"
SPARK_WORKER_MEMORY: str = "2g"

# Data paths
DATA_PATH: str = "data/nyc_311.csv"

# Column Indices (NYPD Dataset)
COL_OFNS: int = 8      # OFNS_DESC
COL_BORO: int = 13     # BORO_NM
COL_AGENCY: int = 16   # JURIS_DESC (Agency/Jurisdiction)
COL_LOC: int = 15      # PREM_TYP_DESC (Location type)
COL_STATUS: int = 11   # CRM_ATPT_CPTD_CD (Completed vs Attempted)
COL_VIC_AGE_GROUP: int = 32 # VIC_AGE_GROUP (Based on head -n 1 check)
