from google.cloud import storage
from datetime import datetime
import json
# 1. Handle the date logic

with open(json file path) as file:  ##COPY PASTE THE JSON FILE
    config = json.load(file)

    
period = config["Month"]
working_period = datetime.strptime(period, '%Y-%m')
period_2 = working_period.strftime('%Y%m') # Results in '202601'

def delete_specific_files(bucket_name, folder_prefix, file_pattern):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # list_blobs with a prefix is faster and costs less (Class B operations)
    blobs = bucket.list_blobs(prefix=folder_prefix)

    deleted_count = 0
    for blob in blobs:
        # Extract just the filename from the full path
        filename = blob.name.split('/')[-1]
        
        # Check if filename matches your pattern
        if filename.startswith(file_pattern):
            print(f"Deleting: {blob.name}")
            blob.delete()
            deleted_count += 1

    if deleted_count == 0:
        print(f"No files found matching pattern: {file_pattern}")
    else:
        print(f"✅ Cleanup complete. Total files deleted: {deleted_count}")

# 2. Configuration (Added 'f' to PATTERN)
BUCKET_NAME = "cmg-cia-buckets"
FOLDER_PATH = "sellout_data/d_amax_prod_sku/"               ###PUT THE PATH HERE BUT EXCLUDE The cmg-cia-buckets
# Use an f-string here so {period_2} is replaced with '202601'
PATTERN = f"BP_SKU_PERLOC_{period_2}" ###IT will depend

delete_specific_files(BUCKET_NAME, FOLDER_PATH, PATTERN)
