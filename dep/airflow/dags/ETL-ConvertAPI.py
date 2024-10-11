from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
import fitz  # PyMuPDF
import json
import os
import requests
import csv
import base64

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Fetch AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize S3 client with environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Fetch ConvertAPI Secret from environment variables
convertapi_secret = os.getenv('CONVERTAPI_SECRET_KEY')

# Function to list all PDF files in the S3 bucket
def list_pdfs_in_s3(bucket_name, **kwargs):
    pdf_keys = []
    response = s3.list_objects_v2(Bucket=bucket_name)
    for content in response.get('Contents', []):
        if content['Key'].endswith('.pdf'):
            pdf_keys.append(content['Key'])
    
    print(f"PDF files found in {bucket_name}: {pdf_keys}")
    kwargs['ti'].xcom_push(key='pdf_keys', value=pdf_keys)

# Function to download PDFs from S3
def download_pdfs_from_s3(bucket_name, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')
    download_paths = []
    
    for pdf_key in pdf_keys:
        download_path = f'/tmp/{os.path.basename(pdf_key)}'
        s3.download_file(bucket_name, pdf_key, download_path)
        print(f"Downloaded {pdf_key} to {download_path}")
        download_paths.append(download_path)
    
    kwargs['ti'].xcom_push(key='download_paths', value=download_paths)

# Function to extract text, tables, and images using PyMuPDF
def extract_with_pymupdf(**kwargs):
    download_paths = kwargs['ti'].xcom_pull(key='download_paths', task_ids='download_pdfs')
    
    for pdf_path in download_paths:
        pdf_filename = os.path.basename(pdf_path)
        base_output_path = f'/tmp/pymupdf_{pdf_filename}'

        doc = fitz.open(pdf_path)
        extracted_text = []
        extracted_tables = []
        extracted_images = []

        for page in doc:
            # Extract text
            extracted_text.append(page.get_text("text"))

            # Extract tables (simplified for now - using text positions as heuristics for tables)
            blocks = page.get_text("dict")["blocks"]
            table_data = []
            for block in blocks:
                if block['type'] == 0:  # text block
                    lines = block['lines']
                    for line in lines:
                        row = " ".join([span['text'] for span in line['spans']])
                        table_data.append(row)
            if table_data:
                extracted_tables.append(table_data)

            # Extract images
            for img in page.get_images(full=True):
                xref = img[0]
                base_image = doc.extract_image(xref)
                image_bytes = base_image["image"]
                image_path = f'{base_output_path}_img_{xref}.png'
                with open(image_path, 'wb') as img_file:
                    img_file.write(image_bytes)
                extracted_images.append(image_path)

        # Save text data as JSON
        text_output_path = f'{base_output_path}.json'
        with open(text_output_path, 'w') as json_file:
            json.dump({"text": extracted_text}, json_file)
        print(f"Extracted text stored at {text_output_path}")
        
        # Save tables as CSV
        if extracted_tables:
            table_output_path = f'{base_output_path}.csv'
            with open(table_output_path, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(extracted_tables)
            print(f"Extracted tables stored at {table_output_path}")
        
        # XCom push the output paths
        kwargs['ti'].xcom_push(key=f'pymupdf_{pdf_filename}_text', value=text_output_path)
        if extracted_tables:
            kwargs['ti'].xcom_push(key=f'pymupdf_{pdf_filename}_tables', value=table_output_path)
        if extracted_images:
            kwargs['ti'].xcom_push(key=f'pymupdf_{pdf_filename}_images', value=extracted_images)

# Function to extract text, tables, and images using ConvertAPI (store in separate files)
def extract_with_convertapi(**kwargs):
    download_paths = kwargs['ti'].xcom_pull(key='download_paths', task_ids='download_pdfs')

    for pdf_path in download_paths:
        pdf_filename = os.path.basename(pdf_path)
        base_output_path = f'/tmp/convertapi_{pdf_filename}'

        # Convert Text
        def extract_text():
            url = "https://v2.convertapi.com/convert/pdf/to/txt"
            headers = {'Authorization': f'Bearer {convertapi_secret}'}
            files = {'File': open(pdf_path, 'rb')}
            response = requests.post(url, headers=headers, files=files)
            if response.status_code == 200:
                # Extract and decode the Base64 text data
                response_json = response.json()
                file_data_encoded = response_json['Files'][0]['FileData']
                decoded_text = base64.b64decode(file_data_encoded).decode('utf-8')  # Decoding Base64
                return decoded_text  # Return decoded text as raw text
            else:
                raise Exception(f"ConvertAPI Text Extraction Failed with Status Code {response.status_code}")

        # Convert Tables (CSV)
        def extract_tables():
            url = "https://v2.convertapi.com/convert/pdf/to/csv"
            headers = {'Authorization': f'Bearer {convertapi_secret}'}
            files = {'File': open(pdf_path, 'rb')}
            response = requests.post(url, headers=headers, files=files)
            if response.status_code == 200:
                # Convert the Base64 CSV response to a decoded CSV string
                csv_data = base64.b64decode(response.json()['Files'][0]['FileData']).decode('utf-8')
                return csv_data
            else:
                raise Exception(f"ConvertAPI Table Extraction Failed with Status Code {response.status_code}")

        # Convert Images with error handling for 500 status code, and save images as PNG
        def extract_images():
            url = "https://v2.convertapi.com/convert/pdf/to/extract-images"
            headers = {'Authorization': f'Bearer {convertapi_secret}'}
            files = {'File': open(pdf_path, 'rb')}
            data = {'ImageOutputFormat': 'png'}
            response = requests.post(url, headers=headers, files=files, data=data)
            if response.status_code == 200:
                return response.json()  # We'll need to decode Base64 images later
            elif response.status_code == 500:
                print(f"Failed to extract images. Status Code: {response.status_code}")
                return {'error': 'Server Error (500)'}
            else:
                raise Exception(f"ConvertAPI Image Extraction Failed with Status Code {response.status_code}")

        # Perform all extractions and store results
        text_result = extract_text()  # Decoded text
        table_result = extract_tables()

        # Before extracting images, check if there are any images in the PDF
        has_images = False
        with fitz.open(pdf_path) as doc:
            for page in doc:
                if page.get_images(full=True):
                    has_images = True
                    break

        if has_images:
            image_result = extract_images()
        else:
            image_result = {'info': 'No images found in PDF'}
            print(f"No images found in {pdf_filename}")

        # Save decoded text as raw JSON
        text_output_path = f'{base_output_path}.json'
        with open(text_output_path, 'w') as json_file:
            json.dump({'text': text_result}, json_file)
        print(f"ConvertAPI text stored at {text_output_path}")

        # Save tables as raw CSV
        table_output_path = f'{base_output_path}.csv'
        with open(table_output_path, 'w') as csv_file:
            csv_file.write(table_result)
        print(f"ConvertAPI tables stored at {table_output_path}")

        # Decode and save images
        if has_images:
            image_paths = []
            for image_data in image_result.get('Files', []):
                image_filename = f"{base_output_path}_{image_data['FileName']}"
                with open(image_filename, 'wb') as img_file:
                    img_file.write(base64.b64decode(image_data['FileData']))  # Decode Base64 images
                image_paths.append(image_filename)
                print(f"ConvertAPI image stored at {image_filename}")
            kwargs['ti'].xcom_push(key=f'convertapi_{pdf_filename}_images', value=image_paths)

        # Push results to XCom
        kwargs['ti'].xcom_push(key=f'convertapi_{pdf_filename}_text', value=text_output_path)
        kwargs['ti'].xcom_push(key=f'convertapi_{pdf_filename}_tables', value=table_output_path)


# Function to upload results to S3
def upload_to_s3(bucket_name, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key='pdf_keys', task_ids='list_pdfs_in_s3')

    for pdf_key in pdf_keys:
        pdf_filename = os.path.basename(pdf_key)

        # PyMuPDF Output Paths
        text_output_path = kwargs['ti'].xcom_pull(key=f'pymupdf_{pdf_filename}_text', task_ids='extract_with_pymupdf')
        tables_output_path = kwargs['ti'].xcom_pull(key=f'pymupdf_{pdf_filename}_tables', task_ids='extract_with_pymupdf')
        images_output_paths = kwargs['ti'].xcom_pull(key=f'pymupdf_{pdf_filename}_images', task_ids='extract_with_pymupdf')

        # Upload PyMuPDF results to S3
        if text_output_path:
            s3.upload_file(text_output_path, bucket_name, f'pymupdf/{os.path.basename(text_output_path)}')
        if tables_output_path:
            s3.upload_file(tables_output_path, bucket_name, f'pymupdf/{os.path.basename(tables_output_path)}')
        if images_output_paths:
            for img_path in images_output_paths:
                s3.upload_file(img_path, bucket_name, f'pymupdf/{os.path.basename(img_path)}')

        # ConvertAPI Output Paths
        convert_text_output_path = kwargs['ti'].xcom_pull(key=f'convertapi_{pdf_filename}_text', task_ids='extract_with_convertapi')
        convert_tables_output_path = kwargs['ti'].xcom_pull(key=f'convertapi_{pdf_filename}_tables', task_ids='extract_with_convertapi')
        convert_images_output_paths = kwargs['ti'].xcom_pull(key=f'convertapi_{pdf_filename}_images', task_ids='extract_with_convertapi')

        # Upload ConvertAPI results to S3
        if convert_text_output_path:
            s3.upload_file(convert_text_output_path, bucket_name, f'convertapi/{os.path.basename(convert_text_output_path)}')
        if convert_tables_output_path:
            s3.upload_file(convert_tables_output_path, bucket_name, f'convertapi/{os.path.basename(convert_tables_output_path)}')
        if convert_images_output_paths:
            for img_path in convert_images_output_paths:
                s3.upload_file(img_path, bucket_name, f'convertapi/{os.path.basename(img_path)}')

        print(f"Uploaded results for {pdf_filename} to S3.")

# Main DAG definition
with DAG('pdf_extraction_with_pymupdf_and_convertapi',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Define bucket details
    source_bucket_name = 'gaia-pdf-unprocessed'
    target_bucket_name = 'gaia-pdf-processed'

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

    # Stage 4: Process each PDF using ConvertAPI
    extract_convertapi_task = PythonOperator(
        task_id='extract_with_convertapi',
        python_callable=extract_with_convertapi,
        provide_context=True,
    )

    # Stage 5: Upload results to the target S3 bucket
    upload_results_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={'bucket_name': target_bucket_name},
        provide_context=True,
    )

    # Task dependencies
    list_pdfs_task >> download_pdfs_task >> [extract_pymupdf_task, extract_convertapi_task] >> upload_results_task
