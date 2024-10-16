# -*- coding: utf-8 -*-
"""Architectural_Diagram.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1Mghzgj3u2EI-W7cRS93q6ZGoSx98bBHm
"""

!pip install diagrams
!apt-get install -y graphviz

from google.colab import files
uploaded = files.upload()

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.client import Users
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import S3
from diagrams.aws.database import RDS
from diagrams.programming.framework import Fastapi
from diagrams.generic.compute import Rack as PyMuPDF
from diagrams.generic.compute import Rack as ConvertAPI
from diagrams.onprem.compute import Server
from diagrams.generic.compute import Rack as SwaggerUI
from diagrams.generic.compute import Rack as JWTAuth
from diagrams.custom import Custom

# Path to your custom Streamlit logo (uploaded to the environment)
streamlit_icon_path = "/content/streamlit.png"
openai_icon_path = "/content/openai.png"

# Diagram structure
with Diagram("Data Architecture", show=True, direction="TB"):

    with Cluster("Data Extraction and Database Loading"):
        huggingface_pdfs = S3("PDF files from HuggingFace")
        s3_unprocessed = S3("AWS S3 Bucket(Unprocessed)")
        airflow = Airflow("Airflow")
        pymupdf = PyMuPDF("PyMuPDF")
        convert_api = ConvertAPI("ConverAPI")

        huggingface_pdfs >> s3_unprocessed >> airflow
        pymupdf >> airflow
        convert_api >> airflow

    with Cluster("Fast API and Streamlit Application"):
        s3_processed = S3("AWS S3 Bucket(Processed)")
        rds_db = RDS("AWS RDS Database")
        fastapi = Fastapi("FastAPI")
        openai_api = Custom("OpenAI API", openai_icon_path)
        swagger_ui = SwaggerUI("Swagger UI")
        jwt_auth = JWTAuth("JWT Auth")

        # Cluster for Streamlit functionalities
        with Cluster("Streamlit Application"):
            login_signup = Custom("Login/Sign-up", streamlit_icon_path)
            pdf_selection = Custom("PDF Selection", streamlit_icon_path)
            summarization = Custom("Summarization", streamlit_icon_path)
            streamlit_ui = Custom("Streamlit UI", streamlit_icon_path)

        user = Users("User")

        # Relations within clusters
        airflow >> s3_processed
        s3_processed >> rds_db >> fastapi
        fastapi >> openai_api
        swagger_ui >> fastapi
        jwt_auth >> fastapi
        fastapi >> streamlit_ui >> user
        fastapi >> rds_db
        user >> openai_api
        openai_api >> streamlit_ui
        user >> streamlit_ui

        # Streamlit functionalities connections
        user >> login_signup >> streamlit_ui
        pdf_selection >> streamlit_ui
        summarization >> streamlit_ui

from IPython.display import Image, display
display(Image(filename="/content/data_architecture.png"))

