import os
import streamlit as st
import base64
import requests
import boto3
import pandas as pd
from io import BytesIO

# Load environment variables
API_URL = os.getenv("API_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# S3 bucket names
unprocessed_bucket = 'gaia-pdf-unprocessed'
processed_bucket = 'gaia-pdf-processed'

# Initialize S3 client using environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Function to list all PDF files in S3 bucket
def list_pdfs(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pdf')]
        else:
            return []
    except Exception as e:
        st.error(f"Error fetching PDF list: {e}")
        return []

# Function to fetch and display PDF in an iframe
def display_pdf_from_s3(pdf_key):
    try:
        pdf_object = s3.get_object(Bucket=unprocessed_bucket, Key=pdf_key)
        pdf_bytes = pdf_object['Body'].read()

        # Display the PDF inside an iframe in the app
        pdf_viewer = st.expander("📄 Preview Selected PDF")
        with pdf_viewer:
            pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')
            pdf_data_uri = f"data:application/pdf;base64,{pdf_base64}"
            st.markdown(f'<iframe src="{pdf_data_uri}" width="700" height="1000"></iframe>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Error fetching PDF for preview: {e}")

# Function to fetch CSV and images from processed S3 bucket
def fetch_csv_and_images(pdf_name, extraction_method):
    csv_key = f"{extraction_method}/{extraction_method}_{pdf_name}.pdf.csv"
    try:
        csv_obj = s3.get_object(Bucket=processed_bucket, Key=csv_key)
        csv_data = csv_obj['Body'].read()  # Read as bytes
        
        st.write(f"### Extracted Tables (CSV) for {pdf_name}")

        # Try reading the CSV with pandas, fallback to raw text if it fails
        try:
            df = pd.read_csv(BytesIO(csv_data), on_bad_lines='skip')
            st.dataframe(df)  # Display as a table like an Excel sheet
        except Exception as e:
            st.warning(f"Error parsing CSV as a table: {e}")
            # Fallback: Display raw CSV content as text
            st.text_area("Raw CSV Data", value=csv_data.decode('utf-8'), height=300)
    except Exception as e:
        st.error(f"Error fetching CSV: {e}")

    image_keys = []
    try:
        response = s3.list_objects_v2(Bucket=processed_bucket, Prefix=f"{extraction_method}/{extraction_method}_{pdf_name}")
        if 'Contents' in response:
            image_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.png')]
    except Exception as e:
        st.error(f"Error listing image files: {e}")

    if image_keys:
        st.write(f"### Extracted Images for {pdf_name}")

        # Display each image under each other
        for image_key in image_keys:
            try:
                img_obj = s3.get_object(Bucket=processed_bucket, Key=image_key)
                img_bytes = img_obj['Body'].read()

                # Display each image with proper scaling
                st.image(img_bytes, use_column_width=True)
            except Exception as e:
                st.error(f"Error fetching image {image_key}: {e}")







# Function to make authenticated requests
def make_authenticated_request(endpoint, method="GET", params=None):
    headers = {"Authorization": f"Bearer {st.session_state.token}"}
    if method == "GET":
        response = requests.get(f"{API_URL}{endpoint}", headers=headers, params=params)
    elif method == "POST":
        response = requests.post(f"{API_URL}{endpoint}", headers=headers, params=params)
    return response

# Initialize session state variables
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "selected_pdf" not in st.session_state:
    st.session_state.selected_pdf = None
if "extraction_method" not in st.session_state:
    st.session_state.extraction_method = None
if "page" not in st.session_state:
    st.session_state.page = "Home"
if "summary" not in st.session_state:
    st.session_state.summary = None
if "signup_successful" not in st.session_state:
    st.session_state.signup_successful = False
if "ok_clicked" not in st.session_state:
    st.session_state.ok_clicked = False

# Sign-up function
def signup(username, password, email):
    data = {"username": username, "password": password, "email": email}
    response = requests.post(f"{API_URL}/signup", json=data)
    if response.status_code == 200:
        st.success("Signed up successfully!")
        st.session_state.signup_successful = True
    else:
        st.error(f"Error: {response.status_code} - {response.text}")

# Login function
def login(username, password):
    data = {"username": username, "password": password}
    response = requests.post(f"{API_URL}/login", data=data)
    if response.status_code == 200:
        token = response.json().get("access_token")
        st.session_state.token = token  # Save token to session state
        st.session_state.logged_in = True
        st.session_state.page = "PDF Selection"
        st.rerun()
    else:
        st.error(f"Login failed: {response.status_code} - {response.text}")

# Page navigation function
def navigate_to_page(page):
    st.session_state.page = page
    st.rerun()

# Function to reset PDF selection
def pick_another_pdf():
    st.session_state.selected_pdf = None
    st.session_state.page = "PDF Selection"
    st.rerun()

# Logout function to reset session state without triggering a no-op warning
def logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.session_state.page = "Home"
    st.session_state.logged_in = False
    st.rerun()

# Function to display the header with the logo and app name
def display_header():
    st.markdown(
        """
        <div style='display: flex; align-items: center; background-color: #f0f0f0; padding: 10px;'>
            <h1 style='color: #007BFF;'>AI-based PDF Summarizer</h1>
        </div>
        """,
        unsafe_allow_html=True
    )

# Function to display the logout button for PDF selection and summarization pages
def display_logout_button():
    st.sidebar.button("🔓 Logout", on_click=logout, key="logout_button")

# Sidebar display
def display_sidebar():
    if st.session_state.page not in ["PDF Selection", "Summarization"]:
        st.sidebar.title("Navigation")
        page_selection = st.sidebar.selectbox("Go to", ["Home", "Login", "Sign Up"])
        if page_selection == "Login":
            st.session_state.page = 'Login'
        elif page_selection == "Sign Up":
            st.session_state.page = 'SignUp'
        else:
            st.session_state.page = 'Home'

# Homepage content
def homepage_content():
    st.markdown(
        """
        <div style='font-weight: bold;'>
            <h2>Spending too much time on lengthy PDF files?</h2>
            <p>Say goodbye to time-consuming PDF summaries with our AI PDF Summarizer. Save time and enhance your learning experience.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Display the image
    image_path = "images/icon.png"
    st.image(image_path, caption="AI-based PDF Summarizer", use_column_width=True)

# Login page
def login_section():
    st.markdown("<h2>🔐 Login</h2>", unsafe_allow_html=True)
    username = st.text_input("👤 Username")
    password = st.text_input("🔑 Password", type="password")
    if st.button("Login"):
        login(username, password)

# Sign-up page with a success dialog box
def signup_section():
    st.markdown("<h2>📝 Sign Up</h2>", unsafe_allow_html=True)
    new_username = st.text_input("👤 New Username")
    new_password = st.text_input("🔑 New Password", type="password")
    email = st.text_input("📧 Email")
    
    if not st.session_state.signup_successful:
        if st.button("Sign Up"):
            signup(new_username, new_password, email)

    if st.session_state.signup_successful and not st.session_state.ok_clicked:
        st.success("User successfully registered!")
        if st.button("OK"):
            # Set the OK clicked flag and change the page to Login
            st.session_state.ok_clicked = True
            st.session_state.page = 'Login'
            # Workaround to trigger the rerun
            st.write('<meta http-equiv="refresh" content="0">', unsafe_allow_html=True)


# PDF Extraction Page
def pdf_extraction_page():
    st.subheader("📂 PDF Extraction")

    pdf_files = list_pdfs(unprocessed_bucket)
    if not pdf_files:
        st.write("No PDF files found in the unprocessed bucket.")
    else:
        st.session_state.selected_pdf = st.selectbox("Choose a PDF", pdf_files)
        if st.session_state.selected_pdf:
            display_pdf_from_s3(st.session_state.selected_pdf)

            extraction_method = st.radio(
                "Select the extraction method below:", 
                ("pymupdf", "convertapi")
            )

            # Button to extract text, tables, and images with a spinner
            if st.button("Extract Text, Tables, and Images"):
                with st.spinner("Extracting text, tables, and images..."):
                    fetch_csv_and_images(st.session_state.selected_pdf.split('/')[-1].replace('.pdf', ''), extraction_method)

            # Button to summarize and ask questions (comes after extraction)
            if st.button("Proceed to Summarization and QA"):
                st.session_state.extraction_method = extraction_method
                st.session_state.page = "Summarization"
                st.rerun()


# Function to process extracted text (handles list or string)
def process_extracted_text(extracted_text):
    if isinstance(extracted_text, list):
        return " ".join(extracted_text)  # Join list items into a single string
    return extracted_text

# PDF Summarization and Question Answering Page
def summarization_page():
    st.subheader("📝 Summarization & Question Answering")

    if st.session_state.selected_pdf and st.session_state.extraction_method:
        pdf_name = st.session_state.selected_pdf.split('/')[-1].replace('.pdf', '')
        extraction_method = st.session_state.extraction_method

        st.write(f"**PDF Name:** {st.session_state.selected_pdf}")
        st.write(f"**Extraction Method:** {extraction_method}")

        # Summarization button with a spinner
        if not st.session_state.summary and st.button("Summarize PDF"):
            with st.spinner("Summarizing the PDF..."):
                try:
                    response = make_authenticated_request(f"/summarize/", method="GET", params={"file_name": pdf_name, "method": extraction_method})
                    extracted_text = process_extracted_text(response.json().get("text", ""))  # Process text
                    if response.status_code == 200:
                        # Save the summary to session state
                        st.session_state.summary = response.json().get("summary", "No summary available")
                    else:
                        st.write(f"Error fetching summary: {response.json()}")
                except Exception as e:
                    st.error(f"Error fetching summary: {e}")

        # If the summary is already generated, display it
        if st.session_state.summary:
            st.write("### Summary of the PDF")
            # Display the summary in a scrollable text box
            st.write(st.session_state.summary)

            # Question Answering Section
            st.write("---")
            st.markdown("### Ask Questions Based on the Summary")
            user_question = st.text_input("Enter your question", key="user_question_input")

            if st.button("Ask Question to OpenAI", key="ask_openai_button"):
                if user_question:
                    with st.spinner("Processing your question..."):
                        try:
                            response = make_authenticated_request(f"/ask-question/", method="POST", params={"file_name": pdf_name, "method": extraction_method, "question": user_question})
                            answer = response.json().get("answer", "No answer available")
                            st.write(f"**Answer:** {answer}")
                        except Exception as e:
                            st.error(f"Error fetching answer: {e}")
                else:
                    st.error("Please enter a question.")
        else:
            st.write("No summary to be displayed.")

        # Button to go back to PDF selection
        if st.button("🔙 Pick another PDF"):
            st.session_state.summary = None
            pick_another_pdf()

# Main App Flow
def app():
    display_header()

    if st.session_state.page in ["Home", "Login", "SignUp"]:
        display_sidebar()

    if st.session_state.page == 'Home':
        homepage_content()
    elif st.session_state.page == 'Login':
        login_section()
    elif st.session_state.page == 'SignUp':
        signup_section()
    elif st.session_state.page == "PDF Selection" and st.session_state.logged_in:
        display_logout_button()
        pdf_extraction_page()
    elif st.session_state.page == "Summarization" and st.session_state.logged_in:
        display_logout_button()
        summarization_page()

# Run the app
if __name__ == '__main__':
    app()
