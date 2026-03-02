import snowflake.connector
from datetime import datetime, timedelta
import json
import calendar

with open(r"{json file}", "r") as file:
    config = json.load(file)

User_Name = config["User_Name"]
period = config["Period"]
# Calculate yesterday's date
dt = datetime.strptime(period, '%Y-%m')
working_period = dt.strftime('%Y%m')      # → '202501'
startd = dt.strftime('%Y-%m-%d')  # → '2025-01-01'
last_day = calendar.monthrange(dt.year, dt.month)[1]
endd = dt.replace(day=last_day).strftime('%Y-%m-%d')  # '2025-01-31'

# Generate the full file path
file_path = f"@GRP_DB_CMG_DGT.CMG_CIA_DB.CMG_CIA_SI/new_channels/new_channels_raw/raw"  
 
# Generate the SQL query
query = f"""
    COPY INTO {file_path}
    FROM ( 
        select * from GRP_DB_CMG_DGT.CIA_ADHOCS.newchannel_sellin                     ##Find the right snowflake file. 
    ) 
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = NONE)
    HEADER = TRUE 
    OVERWRITE = TRUE;
"""

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=User_Name,
    account='globe-prd.privatelink',
    warehouse='PRD_VWH_BU_CMG',
    database='GRP_DB_CMG_DGT',
    schema='CMG_CIA_DB',
    role='FR_PRD_CMG_DANL_FTE',
    authenticator = 'externalbrowser'
)
    
try:
    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    print("⭐ Query executed successfully!")
    
except snowflake.connector.errors.ProgrammingError as e:
    print(f"⛔ An error occurred: {e}")
    
finally:
    # Clean up
    cursor.close()
    conn.close()
