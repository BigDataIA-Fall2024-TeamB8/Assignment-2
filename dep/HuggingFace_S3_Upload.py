import os
import requests
from bs4 import BeautifulSoup
import boto3

# S3 Configuration (now using environment variables)
s3_bucket = os.getenv('S3_BUCKET', 'gaia-pdf-unprocessed')  # Default bucket if env var not set

# AWS Credentials (now retrieved from environment variables)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

# Hugging Face access token (now using environment variable)
huggingface_access_token = os.getenv('HUGGINGFACE_ACCESS_TOKEN')

# Local folder to store the files temporarily
local_folder = './gaia_files'
os.makedirs(local_folder, exist_ok=True)

# URL of the dataset page (for both test and validation directories)
dataset_urls = [
    'https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/validation/',
    'https://huggingface.co/datasets/gaia-benchmark/GAIA/tree/main/2023/test/'
]

def scrape_file_urls(dataset_url):
    """Scrape the dataset page for .pdf file URLs in test and validation directories."""
    file_urls = []
    headers = {
        "Authorization": f"Bearer {huggingface_access_token}"
    }
    response = requests.get(dataset_url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for all links to files (filter to only include .pdf files)
        for link in soup.find_all('a'):
            href = link.get('href')
            # Filter for .pdf files in the test or validation directories
            if href and href.endswith('.pdf') and ('/test/' in href or '/validation/' in href):
                
                file_url = 'https://huggingface.co' + href.replace('/blob/', '/resolve/')
                file_urls.append(file_url)
        
        print(f"Found {len(file_urls)} .pdf file URLs in {dataset_url.split('/')[-2]} directory.")
    else:
        print(f"Failed to retrieve the dataset page. Status code: {response.status_code}")

    return file_urls

def download_file(url, local_path):
    """Download a file from a URL to a local path."""
    headers = {
        "Authorization": f"Bearer {huggingface_access_token}"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {url} to {local_path}")
            return True  # Return True on success
        else:
            print(f"Failed to download {url}. Status code: {response.status_code}")
            return False  # Return False on failure
    except Exception as e:
        print(f"Error downloading {url}: {e}")
        return False  

def upload_to_s3(local_path, s3_key):
    """Upload a local file to the specified S3 bucket."""
    try:
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        print(f"Uploaded {local_path} to S3: {s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_path} to S3. Error: {e}")

# Loop through both dataset URLs (test and validation directories)
for dataset_url in dataset_urls:
    # Scrape the dataset page for file URLs
    file_urls = scrape_file_urls(dataset_url)

    # Output the found URLs for debugging purposes
    print("PDF Files Found:")
    for url in file_urls:
        print(url)

    # Loop through the file URLs, download them, and upload them to S3
    for file_url in file_urls:
        # Extract the relative path after '/main/' to maintain directory structure in S3
        relative_path = file_url.split('/main/')[-1]
        file_name = os.path.basename(relative_path).split("?")[0]  
        local_file_path = os.path.join(local_folder, file_name)

        # Ensure the local directory exists and has write permission
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # Download file (only upload if download is successful)
        if download_file(file_url, local_file_path):
            s3_key = f'gaia/{relative_path}'
            upload_to_s3(local_file_path, s3_key)

            
        else:
            print(f"Skipping upload for {file_url} due to download failure.")
