# Automating Text Extraction and Client - Facing Application Development

## Attestation and contribution declaration:
WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR
ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK
Contribution overall:
1. Sathvik Vadavatha : Data Extraction, Airflow Pipeline, Docker Containers, Deployment
2. Rutuja Patil: FastAPI, JWT Authentication, Endpoint testing
3. Sakshi Aade: Streamlit, Testing Validations

# Demo link:
# Streamlit application link: http://54.164.101.19:8501/

------
# Application demo: https://drive.google.com/file/d/1F5hy4KcokiVmt1q3c51olVOWzAlcX3OZ/view?usp=sharing
------

All links and submissions provided in repositry+codelabs document

Here’s the README file for setting up the project with Docker based on your directory structure.
markdownCopy code# Assignment 2: Dockerized Airflow, FastAPI, and Streamlit Setup
This project sets up Airflow, FastAPI, and Streamlit using Docker for managing workflows, API services, and a web interface for summarizing and asking questions on PDF content. 
## Architectral Diagram:
![image](https://github.com/user-attachments/assets/c78b24d8-a971-4190-983d-579727b009f4)


## Project Structure
The directory structure of the project is as follows:
 
dep/
├── airflow/
│   ├── .env
│   ├── dags/
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   ├── logs/
│   ├── plugins/
│   ├── postgres_data/
│   └── requirements.txt
├── fastapi_streamlit/
│   ├── .env
│   ├── docker-compose.yaml
│   ├── Dockerfile.fastapi
│   ├── Dockerfile.streamlit
│   ├── fast_api.py
│   ├── streamlit_app.py
│   ├── requirements.txt
│   ├── images/
│   └── __pycache__/
├── HuggingFace_S3_Upload.py
└── docker-compose.yaml


---

## Prerequisites
1. **AWS EC2 instance** (or any server) with Docker and Docker Compose installed.
2. **SSH access** to your EC2 instance.
3. A `.pem` key to connect to your EC2 instance.
4. **.env files** with sensitive information such as AWS credentials, database credentials, and API keys.

---

## EC2 Setup with Docker and Docker Compose

### Step 1: Launch EC2 Instance
- Choose **Amazon Linux 2** or **Ubuntu** as your operating system.

### Step 2: Connect to the EC2 Instance

ssh -i /path/to/your-key.pem ec2-user@<EC2_PUBLIC_IP_ADDRESS>
### EC2 Setup with Docker and Docker Compose

1. **Launch an EC2 instance** with Amazon Linux 2 or Ubuntu as the operating system.
 
 2. **Connect to the EC2 instance**:
   
ssh -i /path/to/your-key.pem ec2-user@<EC2_PUBLIC_IP_ADDRESS>
 
3. Update the instance:
 
For Ubuntu: sudo apt-get update && sudo apt-get upgrade -y
For Amazon Linux 2: sudo yum update -y


Install Docker: 

For Ubuntu:sudo yum update -y
For Amazon Linux 2: sudo apt-get update && sudo apt-get upgrade -y

4. Install Docker:
   
For Amazon Linux 2: sudo amazon-linux-extras install docker -y
For Ubuntu:  sudo apt-get install docker.io -y


5. Start and enable Docker: sudo service docker start
sudo systemctl enable docker

6. Install Docker Compose:

sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version


7. Add the EC2 user to the Docker group (so you don’t have to use sudo for Docker commands):
   sudo usermod -aG docker ec2-user

 
8. Log out and log back in for the group changes to take effect:

exit
ssh -i /path/to/your-key.pem ec2-user@<EC2_PUBLIC_IP_ADDRESS>


### File Transfer to EC2:


Transfer the project files from your local machine to the EC2 instance:
scp -i /path/to/your-key.pem -r /local/path/to/Assignment2 ec2-user@<EC2_PUBLIC_IP_ADDRESS>:/home/ec2-user/


### Directory Overview


airflow/: Contains files for Airflow setup.
.env: Environment variables for Airflow.
docker-compose.yaml: Docker Compose file for setting up Airflow services.
Dockerfile: Dockerfile for building the Airflow image.
requirements.txt: Python dependencies for Airflow.
fastapi_streamlit/: Contains files for FastAPI and Streamlit setup.
.env: Environment variables for FastAPI and Streamlit.
docker-compose.yaml: Docker Compose file for setting up FastAPI and Streamlit services.
Dockerfile.fastapi: Dockerfile for building the FastAPI image.
Dockerfile.streamlit: Dockerfile for building the Streamlit image.
fast_api.py: FastAPI application code.
streamlit_app.py: Streamlit application code.
requirements.txt: Python dependencies for FastAPI and Streamlit.


### Docker Setup and Deployment
### Build Docker Images:

Navigate to the airflow/ and fastapi_streamlit/ directories separately and run the following commands to build the Docker images:
For Airflow:
cd /home/ec2-user/Assignment2/airflow
docker-compose build

 
For FastAPI and Streamlit:
cd /home/ec2-user/Assignment2/fastapi_streamlit
docker-compose build


Run the Containers:

After building the images, you can run the containers:
For Airflow:
cd /home/ec2-user/Assignment2/airflow
docker-compose up -d

 
For FastAPI and Streamlit:
cd /home/ec2-user/Assignment2/fastapi_streamlit
docker-compose up -d


Check Running Containers:
Verify if the containers are up and running:
docker ps

### Access the Applications:
Airflow: You can access Airflow via your EC2 instance's public IP on the port you've configured (typically port 8080).
FastAPI: FastAPI runs on port 8000 (or any port you have configured).
Streamlit: Streamlit runs on port 8501 (or any port you have configured).


### Additional Notes
Environment Variables: Ensure that your .env files contain the correct values, such as AWS credentials, database credentials, and API keys.
Airflow DAGS: Place your DAGs in the dags/ directory of the airflow/ folder.
Image Upload: For FastAPI and Streamlit, ensure that uploaded images are stored in the images/ folder in the fastapi_streamlit/ directory.


### Troubleshooting
If any services fail to start, check the logs with:
Ensure that all environment variables are correctly set in the .env files.
Ensure that the security group associated with the EC2 instance allows inbound traffic on the required ports (e.g., 8000, 8501, 8080).


### Stopping the Containers
To stop the running containers:
codedocker-compose down


### Conclusion
This setup will help you run Airflow, FastAPI, and Streamlit using Docker in a production-like environment. If you have any issues, check the logs and ensure that your EC2 instance is properly configured with the necessary security group rules.

 
 
