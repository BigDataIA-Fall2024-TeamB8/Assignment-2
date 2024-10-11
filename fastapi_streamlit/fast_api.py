from fastapi import FastAPI, Depends, HTTPException
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from typing import Optional
import pyodbc
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import boto3
import json
import openai
import os
import tiktoken

# Load environment variables for RDS SQL Server connection
RDS_ENDPOINT = os.getenv("RDS_ENDPOINT")
RDS_USERNAME = os.getenv("RDS_USERNAME")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")
RDS_DATABASE = os.getenv("RDS_DATABASE")

# Connection string for Microsoft SQL Server on RDS
CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={RDS_ENDPOINT},1433;"
    f"DATABASE={RDS_DATABASE};"
    f"UID={RDS_USERNAME};"
    f"PWD={RDS_PASSWORD}"
)

# Load environment variables for other configurations
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 70
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Password hashing configuration
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# FastAPI OAuth2 token handler
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

app = FastAPI()

# Initialize S3 client with environment variables
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Set OpenAI API Key
openai.api_key = OPENAI_API_KEY

# S3 bucket information
processed_bucket_name = 'gaia-pdf-processed'

# Pydantic User model with email validation
class User(BaseModel):
    username: str
    password: str
    email: EmailStr

# Function to hash passwords
def hash_password(password: str):
    return pwd_context.hash(password)

# Function to verify passwords
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Function to create access tokens
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

# Function to count tokens using tiktoken
def count_tokens(text: str) -> int:
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    return len(encoding.encode(text))

# Function to chunk text with overlap
def chunk_text_with_overlap(text: str, max_tokens: int = 13000, overlap_tokens: int = 500) -> list:
    encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")
    tokens = encoding.encode(text)
    chunks = []

    for i in range(0, len(tokens), max_tokens - overlap_tokens):
        chunk = tokens[i:i + max_tokens]
        chunks.append(encoding.decode(chunk))

    return chunks

# Function to summarize text with OpenAI
def summarize_text(text: str) -> str:
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful assistant that summarizes text."},
            {"role": "user", "content": f"Summarize the following text:\n\n{text}"}
        ],
        max_tokens=150
    )
    return response['choices'][0]['message']['content'].strip()

# Function to answer questions based on the extracted text
def answer_question_with_relevance(text: str, question: str) -> str:
    if isinstance(text, list):
        text = " ".join(text)

    if not isinstance(text, str) or len(text.strip()) == 0:
        raise ValueError("Extracted text is empty or not a valid string.")

    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that answers questions based on the provided text."},
                {"role": "user", "content": f"Based on the following text:\n\n{text}\n\nAnswer this question: {question}"}
            ],
            max_tokens=200  # Allow for more detailed answers
        )
        answer = response['choices'][0]['message']['content'].strip()
        return answer if answer else "No answer available"
    except Exception as e:
        return f"Error: {str(e)}"

# Function to aggregate answers from multiple chunks
def aggregate_answers(answers: list) -> str:
    combined_answer = " ".join(answers)
    return combined_answer.strip()

# Function to connect to the database using pyodbc
def get_db_connection():
    return pyodbc.connect(CONNECTION_STRING)

# Signup endpoint without authentication
@app.post("/signup")
def signup(user: User):
    if not user.username or not user.password or not user.email:
        raise HTTPException(status_code=400, detail="All fields (username, password, email) are required.")

    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if the user already exists
    cursor.execute("SELECT * FROM Users WHERE username = ?", (user.username,))
    if cursor.fetchone():
        raise HTTPException(status_code=400, detail="User already exists")

    # Insert new user into the database
    hashed_password = hash_password(user.password)
    cursor.execute("INSERT INTO Users (username, password, Email_ID) VALUES (?, ?, ?)", 
                   (user.username, hashed_password, user.email))
    conn.commit()
    return {"message": "User signed up successfully!"}

# Login endpoint
@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not form_data.username or not form_data.password:
        raise HTTPException(status_code=400, detail="Both username and password are required.")

    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM Users WHERE username = ?", (form_data.username,))
    user_record = cursor.fetchone()
    
    if not user_record or not verify_password(form_data.password, user_record[2]): 
        raise HTTPException(status_code=400, detail="Invalid credentials")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)
    
    return {"access_token": access_token, "token_type": "bearer"}

# Protected endpoint
@app.get("/protected_endpoint", dependencies=[Depends(oauth2_scheme)])
def protected(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    return {"message": f"Welcome {username} to the protected service!"}

# Summarize PDF endpoint
@app.get("/summarize/", dependencies=[Depends(oauth2_scheme)])
async def summarize_pdf(file_name: str, method: str, token: str = Depends(oauth2_scheme)):
    s3_key = f'{method}/{method}_{file_name}.pdf.json'

    try:
        obj = s3.get_object(Bucket=processed_bucket_name, Key=s3_key)
        text_data = json.loads(obj['Body'].read().decode('utf-8'))

        extracted_text = text_data.get("text", "")
        if isinstance(extracted_text, list):
            extracted_text = " ".join(extracted_text)
        
        if not isinstance(extracted_text, str) or len(extracted_text.strip()) == 0:
            raise HTTPException(status_code=500, detail="Error: Extracted text is empty or not a string.")

        token_count = count_tokens(extracted_text)
        
        if token_count > 13000:
            chunks = chunk_text_with_overlap(extracted_text)
            summarized_chunks = [summarize_text(chunk) for chunk in chunks]
            summarized_text = "\n\n".join(summarized_chunks)
        else:
            summarized_text = summarize_text(extracted_text)

        return {"summary": summarized_text}

    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"File not found in S3: {s3_key}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error summarizing PDF: {str(e)}")

# Ask question endpoint
@app.post("/ask-question/", dependencies=[Depends(oauth2_scheme)])
async def ask_question(file_name: str, method: str, question: str, token: str = Depends(oauth2_scheme)):
    s3_key = f'{method}/{method}_{file_name}.pdf.json'

    try:
        obj = s3.get_object(Bucket=processed_bucket_name, Key=s3_key)
        text_data = json.loads(obj['Body'].read().decode('utf-8'))

        text = text_data.get("text", "")
        if isinstance(text, list):
            text = " ".join(text)

        if not isinstance(text, str) or len(text.strip()) == 0:
            raise HTTPException(status_code=500, detail="Error: Extracted text is empty or not a string.")

        # Proceed with answering the question
        token_count = count_tokens(text)
        if token_count > 13000:
            chunks = chunk_text_with_overlap(text, max_tokens=13000, overlap_tokens=500)
            answers = []
            for chunk in chunks:
                answer = answer_question_with_relevance(chunk, question)
                if "No answer available" not in answer:
                    answers.append(answer)

            final_answer = " ".join(answers) if answers else "No answer available"
        else:
            final_answer = answer_question_with_relevance(text, question)

        return {"answer": final_answer}

    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"File not found in S3: {s3_key}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error answering question: {str(e)}")
