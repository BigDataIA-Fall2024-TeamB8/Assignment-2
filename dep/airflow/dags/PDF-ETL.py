from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import fitz  # PyMuPDF
import requests
import json
import os

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize S3 client
s3 = boto3.client('s3')

# Function to list all PDF files in the S3 bucket
def list_pdfs_in_s3(bucket_name, **kwargs):
    pdf_keys = []
    response = s3.list_objects_v2(Bucket=bucket_name)
    
    for content in response.get('Contents', []):
        if content['Key'].endswith('.pdf'):
            pdf_keys.append(content['Key'])
    
    print(f"PDF files found in {bucket_name}: {pdf_keys}")
    
    # Push the list of PDFs to XCom so other tasks can retrieve it
    kwargs['ti'].xcom_push(key='pdf_keys', value=pdf_keys)

# Function to download PDFs from S3
def download_pdfs_from_s3(bucket_name, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    for pdf_key in pdf_keys:
        download_path = f'/tmp/{os.path.basename(pdf_key)}'
        s3.download_file(bucket_name, pdf_key, download_path)
        print(f"Downloaded {pdf_key} to {download_path}")

# Function to extract text and images using PyMuPDF
def extract_with_pymupdf(**kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    for pdf_key in pdf_keys:
        pdf_filename = os.path.basename(pdf_key)
        pdf_path = f'/tmp/{pdf_filename}'
        pymupdf_output_path = f'/tmp/pymupdf_{pdf_filename}.json'
        
        doc = fitz.open(pdf_path)
        extracted_data = {"text": "", "images": []}
        
        for page in doc:
            extracted_data["text"] += page.get_text("text")
        
        # Save the extracted data as JSON
        with open(pymupdf_output_path, 'w') as json_file:
            json.dump(extracted_data, json_file)
        
        print(f"Extracted data with PyMuPDF stored at {pymupdf_output_path}")
        
        # Push the output path to XCom for later tasks
        kwargs['ti'].xcom_push(key=f'pymupdf_{pdf_key}', value=pymupdf_output_path)

def extract_with_nanonets(api_key, model_id, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    nanonets_url = f"https://app.nanonets.com/api/v2/OCR/Model/{model_id}/LabelFile/"
    
    for pdf_key in pdf_keys:
        pdf_filename = os.path.basename(pdf_key)
        pdf_path = f'/tmp/{pdf_filename}'
        nanonets_output_path = f'/tmp/nanonets_{pdf_filename}.json'
        
        with open(pdf_path, 'rb') as pdf_file:
            files = {'file': pdf_file}
            headers = {
                'Authorization': 'e7f8c0d5-8322-11ef-8b43-62b74a5a5aaf'  # Directly passing the API key without Basic
            }
            response = requests.post(nanonets_url, files=files, headers=headers)
            
            if response.status_code == 200:
                extracted_data = response.json()
                
                # Save the extracted data as JSON
                with open(nanonets_output_path, 'w') as json_file:
                    json.dump(extracted_data, json_file)
                
                print(f"Extracted data with Nanonets stored at {nanonets_output_path}")
                kwargs['ti'].xcom_push(key=f'nanonets_{pdf_key}', value=nanonets_output_path)
            else:
                print(f"Failed to extract with Nanonets API. Status Code: {response.status_code}")
                print(f"Response Text: {response.text}")





# Function to upload JSON results to the S3 bucket
def upload_to_s3(bucket_name, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    
    for pdf_key in pdf_keys:
        # Get the file paths from XCom
        pymupdf_output_path = kwargs['ti'].xcom_pull(key=f'pymupdf_{pdf_key}', task_ids='extract_with_pymupdf')
        nanonets_output_path = kwargs['ti'].xcom_pull(key=f'nanonets_{pdf_key}', task_ids='extract_with_nanonets')
        
        # S3 keys for storing the JSON files
        pymupdf_s3_key = f'pymupdf/{os.path.basename(pymupdf_output_path)}'
        nanonets_s3_key = f'nanonets/{os.path.basename(nanonets_output_path)}'
        
        # Upload PyMuPDF results
        if pymupdf_output_path:
            s3.upload_file(pymupdf_output_path, bucket_name, pymupdf_s3_key)
            print(f"Uploaded {pymupdf_output_path} to {pymupdf_s3_key}")
        
        # Upload Nanonets results
        if nanonets_output_path:
            s3.upload_file(nanonets_output_path, bucket_name, nanonets_s3_key)
            print(f"Uploaded {nanonets_output_path} to {nanonets_s3_key}")

# Main DAG definition
with DAG('pdf_extraction_stages',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Define bucket details
    source_bucket_name = 'gaia-pdf-unprocessed'
    target_bucket_name = 'gaia-pdf-processed'

    # Nanonets API Key and Model ID
    nanonets_api_key = 'e7f8c0d5-8322-11ef-8b43-62b74a5a5aaf'  # Replace with actual Nanonets API key
    nanonets_model_id = '509bd1de-e661-40e7-82bf-7ceb4b29e916'  # Replace with actual Nanonets model ID

    # Stage 1: List all PDF files in the source S3 bucket
    list_pdfs_task = PythonOperator(
        task_id='list_pdfs_in_s3',
        python_callable=list_pdfs_in_s3,
        op_kwargs={'bucket_name': source_bucket_name},
    )

    # Stage 2: Download PDFs from the source S3 bucket
    download_pdfs_task = PythonOperator(
        task_id='download_pdfs',
        python_callable=download_pdfs_from_s3,
        op_kwargs={'bucket_name': source_bucket_name},
    )

    # Stage 3: Process each PDF using PyMuPDF
    extract_pymupdf_task = PythonOperator(
        task_id='extract_with_pymupdf',
        python_callable=extract_with_pymupdf,
        provide_context=True,
    )

    # Stage 4: Process each PDF using Nanonets
    extract_nanonets_task = PythonOperator(
        task_id='extract_with_nanonets',
        python_callable=extract_with_nanonets,
        op_kwargs={'api_key': nanonets_api_key, 'model_id': nanonets_model_id},
        provide_context=True,
    )

    # Stage 5: Upload processed files to the target S3 bucket
    upload_results_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={'bucket_name': target_bucket_name},
        provide_context=True,
    )

    # Task dependencies: each task follows the previous one
    list_pdfs_task >> download_pdfs_task >> extract_pymupdf_task >> extract_nanonets_task >> upload_results_task
