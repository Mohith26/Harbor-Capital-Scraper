import os
from google.cloud import storage

# --- CONFIGURATION ---
# Replace this with your actual bucket name
BUCKET_NAME = "harbor-capital-comps-files" 

def upload_to_gcs(source_file_path, destination_blob_name):
    """
    Uploads a file to Google Cloud Storage.
    
    When running on Cloud Run, this uses the server's internal identity 
    (Service Account) automatically. No JSON keys required.
    """
    try:
        # 1. Initialize Client (Auto-auth)
        storage_client = storage.Client()

        # 2. Get the Bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # 3. Create a "Blob" (The object placeholder)
        blob = bucket.blob(destination_blob_name)

        # 4. Upload the file
        blob.upload_from_filename(source_file_path)
        
        print(f"File {source_file_path} uploaded to {destination_blob_name}.")
        
        # 5. Return the Public Link
        # Note: This requires your bucket to be "Publicly Readable".
        # If your bucket is private, this link won't work for users outside Google.
        return blob.public_url 

    except Exception as e:
        print(f"Upload failed: {e}")
        return None