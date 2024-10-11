from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import openai
import boto3
import base64
from pinecone import Pinecone, ServerlessSpec, PodSpec

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Initialize S3 client
s3 = boto3.client('s3')

# OpenAI API key
openai.api_key = "sk-VRxp2cquh2akDWO3iUjw3LkECmSUK1YGr2cWMHw_DoT3BlbkFJmZ3KP7g8ZUKxewbi1MwlrrvoxJ-CmxUS8dWOMR9gIA"  # Direct assignment for testing purposes

# Initialize Pinecone client using API key
pc = Pinecone(api_key="a9f542d0-f0a0-4f8c-acb1-0835ca7c3802")  # Direct assignment for testing purposes

# Function to list processed PDF files in the respective S3 folders (PyMuPDF or ConvertAPI)
def list_processed_pdfs_in_s3(bucket_name, folder, **kwargs):
    pdf_keys = []
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)
    for content in response.get('Contents', []):
        if content['Key'].endswith('.json'):
            pdf_keys.append(content['Key'])
    
    print(f"Processed PDF files found in {bucket_name}/{folder}: {pdf_keys}")
    kwargs['ti'].xcom_push(key=f'processed_pdf_keys_{folder}', value=pdf_keys)

# Function to download processed JSON files from S3
def download_processed_pdfs_from_s3(bucket_name, folder, **kwargs):
    pdf_keys = kwargs['ti'].xcom_pull(key=f'processed_pdf_keys_{folder}', task_ids=f'list_processed_pdfs_in_s3_{folder}')
    download_paths = []
    
    for pdf_key in pdf_keys:
        download_path = f'/tmp/{pdf_key.split("/")[-1]}'
        s3.download_file(bucket_name, pdf_key, download_path)
        print(f"Downloaded {pdf_key} to {download_path}")
        download_paths.append(download_path)
    
    kwargs['ti'].xcom_push(key=f'downloaded_paths_{folder}', value=download_paths)

# Function to create Pinecone index
def create_pinecone_index(index_name, dimension=1536):
    try:
        # List the existing indexes
        existing_indexes = pc.list_indexes()
        
        # Check if the index already exists
        if index_name not in existing_indexes:
            # Create the index
            pc.create_index(
                name=index_name,
                dimension=dimension,
                metric='cosine',  # Choose your metric
                spec=ServerlessSpec(
                    cloud='aws',
                    region='us-east-1'  # Updated to a supported region
                )
            )
            print(f"Created Pinecone index: {index_name}")
        else:
            print(f"Pinecone index {index_name} already exists.")
    except Exception as e:
        print(f"Error creating Pinecone index: {e}")

def chunk_text(text, max_tokens=8192):
    """
    Break text into chunks based on the max token limit.
    We'll split based on spaces to ensure we don't cut words in half.
    """
    words = text.split()
    for i in range(0, len(words), max_tokens):
        yield " ".join(words[i:i + max_tokens])

def embed_and_store_pdf_pages(index_name, folder, **kwargs):
    download_paths = kwargs['ti'].xcom_pull(key=f'downloaded_paths_{folder}', task_ids=f'download_processed_pdfs_from_s3_{folder}')

    # Ensure Pinecone index exists for this task
    create_pinecone_index(index_name)

    for file_path in download_paths:
        try:
            # Load extracted JSON data
            with open(file_path, 'r') as f:
                extracted_data = json.load(f)

            # Connect to the Pinecone index
            index = pc.Index(index_name)

            if folder == 'pymupdf':
                # Handle PyMuPDF JSON structure
                for page_number, page_data in enumerate(extracted_data.get("pages", [])):
                    page_content = page_data.get("text", "") + "\n" + str(page_data.get("tables", "")) + "\n" + str(page_data.get("images", ""))

                    # Break the content into smaller chunks if it exceeds token limit
                    for chunk_idx, chunk in enumerate(chunk_text(page_content, max_tokens=8192)):
                        # Generate embeddings using OpenAI API
                        response = openai.Embedding.create(input=chunk, model="text-embedding-ada-002")
                        embedding = response['data'][0]['embedding']

                        # Store embedding in Pinecone
                        index.upsert(vectors=[{
                            "id": f"{file_path.split('/')[-1]}_page_{page_number}_chunk_{chunk_idx}",
                            "values": embedding,
                            "metadata": {"file_name": file_path.split('/')[-1], "page_number": page_number, "chunk_idx": chunk_idx}
                        }])

                        print(f"Stored embedding for {file_path}, page {page_number}, chunk {chunk_idx} in index {index_name}")

            elif folder == 'convertapi':
                # Handle ConvertAPI JSON structure
                text_data = extracted_data.get("text", {}).get("Files", [])
                table_data = extracted_data.get("tables", {}).get("Files", [])
                image_data = extracted_data.get("images", {}).get("Files", [])

                # Combine all file data (text, tables, images) for embedding
                for idx, file_data in enumerate(text_data + table_data + image_data):
                    content = file_data.get("FileData", "")

                    if content:
                        try:
                            # Decode Base64 content for embedding
                            try:
                                content_decoded = base64.b64decode(content).decode('utf-8')
                            except (UnicodeDecodeError, ValueError) as e:
                                print(f"Skipping content index {idx} due to decoding error: {e}")
                                continue

                            # Break the content into smaller chunks if it exceeds token limit
                            for chunk_idx, chunk in enumerate(chunk_text(content_decoded, max_tokens=8192)):
                                # Generate embeddings using OpenAI API
                                response = openai.Embedding.create(input=chunk, model="text-embedding-ada-002")
                                embedding = response['data'][0]['embedding']

                                # Store embedding in Pinecone
                                index.upsert(vectors=[{
                                    "id": f"{file_path.split('/')[-1]}_content_{idx}_chunk_{chunk_idx}",
                                    "values": embedding,
                                    "metadata": {"file_name": file_path.split('/')[-1], "content_index": idx, "chunk_idx": chunk_idx}
                                }])

                                print(f"Stored embedding for {file_path}, content index {idx}, chunk {chunk_idx} in index {index_name}")

                        except Exception as e:
                            print(f"Failed to process content index {idx} in file {file_path}: {e}")

        except Exception as e:
            print(f"Failed to process {file_path}: {e}")


# Main DAG definition
with DAG('pinecone_embedding_test_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Define bucket and folder for PyMuPDF and ConvertAPI processed PDFs
    processed_pdf_bucket = 'gaia-pdf-processed'
    pymupdf_folder = 'pymupdf'
    convertapi_folder = 'convertapi'

    # Task 1a: List processed PDFs in PyMuPDF folder
    list_pymupdf_pdfs_task = PythonOperator(
        task_id=f'list_processed_pdfs_in_s3_pymupdf',  # Remove the forward slash
        python_callable=list_processed_pdfs_in_s3,
        op_kwargs={'bucket_name': processed_pdf_bucket, 'folder': pymupdf_folder},
    )

    # Task 1b: List processed PDFs in ConvertAPI folder
    list_convertapi_pdfs_task = PythonOperator(
        task_id=f'list_processed_pdfs_in_s3_convertapi',  # Remove the forward slash
        python_callable=list_processed_pdfs_in_s3,
        op_kwargs={'bucket_name': processed_pdf_bucket, 'folder': convertapi_folder},
    )

    # Task 2a: Download processed PDFs from PyMuPDF folder
    download_pymupdf_pdfs_task = PythonOperator(
        task_id=f'download_processed_pdfs_from_s3_pymupdf',  # Remove the forward slash
        python_callable=download_processed_pdfs_from_s3,
        op_kwargs={'bucket_name': processed_pdf_bucket, 'folder': pymupdf_folder},
    )

    # Task 2b: Download processed PDFs from ConvertAPI folder
    download_convertapi_pdfs_task = PythonOperator(
        task_id=f'download_processed_pdfs_from_s3_convertapi',  # Remove the forward slash
        python_callable=download_processed_pdfs_from_s3,
        op_kwargs={'bucket_name': processed_pdf_bucket, 'folder': convertapi_folder},
    )

    # Task 3a: Embed and store PyMuPDF files in 'pymupdf-index'
    embed_pymupdf_task = PythonOperator(
        task_id='embed_pymupdf_pages',
        python_callable=embed_and_store_pdf_pages,
        op_kwargs={'index_name': 'pymupdf-index', 'folder': pymupdf_folder},
        provide_context=True,
    )

    # Task 3b: Embed and store ConvertAPI files in 'convertapi-index'
    embed_convertapi_task = PythonOperator(
        task_id='embed_convertapi_pages',
        python_callable=embed_and_store_pdf_pages,
        op_kwargs={'index_name': 'convertapi-index', 'folder': convertapi_folder},
        provide_context=True,
    )

    # Task dependencies for PyMuPDF
    list_pymupdf_pdfs_task >> download_pymupdf_pdfs_task >> embed_pymupdf_task
    
    # Task dependencies for ConvertAPI
    list_convertapi_pdfs_task >> download_convertapi_pdfs_task >> embed_convertapi_task
