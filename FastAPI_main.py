from fastapi import FastAPI, Depends, HTTPException
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel
import pyodbc
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

# Configuration
SECRET_KEY = "00cc8d148a56993bacc5bf7ccb40b931ea1a07531a3a4dc93eb8fd27e6696bfe"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Database connection
connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=RUTUJA_PATIL;DATABASE=Credential_Database;UID=damg7370_demo;PWD=Omsai@123"

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# FastAPI OAuth2 token handler
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

app = FastAPI()

# User Pydantic model
class User(BaseModel):
    username: str
    password: str
    email: str

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

# Function to connect to the database
def get_db_connection():
    return pyodbc.connect(connection_string)

# Signup endpoint
@app.post("/signup")
def signup(user: User):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if the user already exists
    cursor.execute("SELECT * FROM users WHERE username = ?", (user.username,))
    if cursor.fetchone():
        raise HTTPException(status_code=400, detail="User already exists")
    
    # Insert new user into the database
    hashed_password = hash_password(user.password)
    cursor.execute("INSERT INTO users (username, password, Email_ID) VALUES (?, ?, ?)", 
                   (user.username, hashed_password, user.email))
    conn.commit()
    return {"message": "User signed up successfully!"}

# Login endpoint
@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Fetch user data from the database
    cursor.execute("SELECT * FROM users WHERE username = ?", (form_data.username,))
    user_record = cursor.fetchone()
    
    if not user_record or not verify_password(form_data.password, user_record[2]): 
        raise HTTPException(status_code=400, detail="Invalid credentials")

    # Create access token
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)
    
    return {"access_token": access_token, "token_type": "bearer"}

# Protected endpoint for testing
@app.get("/protected_endpoint")
def protected(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Decode the JWT token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    return {"message": f"Welcome {username} to the protected service!"}
