from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import fitz  # PyMuPDF
import requests
import json
import os
import base64

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

def extract_with_dochorizon(api_key, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    dochorizon_url_generic = "https://dochorizon.klippa.com/api/services/document_capturing/v1/generic"

    for pdf_key in pdf_keys:
        pdf_filename = os.path.basename(pdf_key)
        pdf_path = f'/tmp/{pdf_filename}'
        dochorizon_output_path = f'/tmp/dochorizon_{pdf_filename}.json'

        # Prepare headers for DocHorizon API
        headers = {
            'x-api-key': api_key,
            'Content-Type': 'application/json',
        }

        # Correctly base64 encode the file
        with open(pdf_path, 'rb') as pdf_file:
            encoded_pdf = base64.b64encode(pdf_file.read()).decode('utf-8')

        # Try using "1" to indicate all pages
        page_ranges = "1"  # Using 1 to denote the first page and possibly all pages

        # Payload for the generic document capturing API
        payload_generic = {
            "documents": [
                {
                    "content_type": "application/pdf",  # Set to PDF type
                    "data": encoded_pdf,  # Base64 encoded PDF data
                    "filename": pdf_filename,  # Name of the PDF file
                    "page_ranges": page_ranges  # All pages will be processed
                    # Optional fields: 'password', 'url' can be added if needed
                }
            ]
        }

        # Debug: Print the payload and headers to verify correctness before the request
        print(f"Headers: {headers}")
        print(f"Payload: {json.dumps(payload_generic, indent=2)}")

        # Call the DocHorizon generic capturing API
        response_generic = requests.post(dochorizon_url_generic, headers=headers, json=payload_generic)

        # Check if the request was successful
        if response_generic.status_code == 200:
            extracted_data_generic = response_generic.json()

            # Save the extracted data from the generic capturing API as JSON
            generic_output_path = f'/tmp/dochorizon_generic_{pdf_filename}.json'
            with open(generic_output_path, 'w') as json_file:
                json.dump(extracted_data_generic, json_file)

            print(f"Extracted document data with DocHorizon (generic capture) stored at {generic_output_path}")
            kwargs['ti'].xcom_push(key=f'dochorizon_generic_{pdf_key}', value=generic_output_path)
        else:
            # Log the failure
            print(f"Failed to extract with DocHorizon generic capture API. Status Code: {response_generic.status_code}")
            print(f"Response: {response_generic.text}")







def upload_to_s3(bucket_name, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    
    for pdf_key in pdf_keys:
        # Get the file paths from XCom
        pymupdf_output_path = kwargs['ti'].xcom_pull(key=f'pymupdf_{pdf_key}', task_ids='extract_with_pymupdf')
        dochorizon_output_path = kwargs['ti'].xcom_pull(key=f'dochorizon_generic_{pdf_key}', task_ids='extract_with_dochorizon')
        
        if pymupdf_output_path and dochorizon_output_path:  # Ensure paths are valid
            # S3 keys for storing the JSON files
            pymupdf_s3_key = f'pymupdf/{os.path.basename(pymupdf_output_path)}'
            dochorizon_s3_key = f'dochorizon/{os.path.basename(dochorizon_output_path)}'
            
            # Upload PyMuPDF results
            s3.upload_file(pymupdf_output_path, bucket_name, pymupdf_s3_key)
            print(f"Uploaded {pymupdf_output_path} to {pymupdf_s3_key}")
            
            # Upload DocHorizon results
            s3.upload_file(dochorizon_output_path, bucket_name, dochorizon_s3_key)
            print(f"Uploaded {dochorizon_output_path} to {dochorizon_s3_key}")
        else:
            print(f"Error: One or both of the output paths are None for {pdf_key}.")


# Main DAG definition
with DAG('pdf_extraction_with_dochorizon',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Define bucket details
    source_bucket_name = 'gaia-pdf-unprocessed'
    target_bucket_name = 'gaia-pdf-processed'

    # DocHorizon API Key
    dochorizon_api_key = 'Zgc0KsyxqAipgReDICIryPYpKqZKpJJZ'  # Replace with actual DocHorizon API key

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

    # Stage 4: Process each PDF using DocHorizon
    extract_dochorizon_task = PythonOperator(
        task_id='extract_with_dochorizon',
        python_callable=extract_with_dochorizon,
        op_kwargs={'api_key': dochorizon_api_key},
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
    list_pdfs_task >> download_pdfs_task >> extract_pymupdf_task >> extract_dochorizon_task >> upload_results_task


