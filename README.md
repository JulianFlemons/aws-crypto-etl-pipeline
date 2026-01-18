# aws-crypto-etl-pipeline
A serverless AWS ETL pipeline extracting real-time crypto data from CoinGecko API to RDS PostgreSQL using Lambda, S3, and AWS Glue.
<img width="3580" height="960" alt="Crypto AWS ETL Pipeline Diagram (1)" src="https://github.com/user-attachments/assets/58fb367e-5210-481d-826e-965c080b17a0" />
📌 Project Overview
This project demonstrates a serverless, event-driven ETL pipeline designed to ingest, transform, and store cryptocurrency market data. By integrating Terraform for infrastructure as code with AWS Glue for data orchestration, the pipeline ensures a scalable and reproducible workflow from API extraction to relational storage.

🏗 Architecture Diagram
Architecture created in Lucidchart featuring a Terraform-managed S3 bucket, AWS Lambda, Glue, and RDS.

🛠 Tech Stack
Infrastructure as Code: Terraform (used for S3 bucket provisioning).

Data Ingestion: AWS Lambda triggered by Amazon EventBridge.

Data Lake: Amazon S3 (Raw and Processed tiers).

Data Cataloging: AWS Glue Crawlers and Glue Data Catalog.

Data Transformation: AWS Glue ETL Jobs (Python/PySpark).

Data Warehouse: Amazon RDS (PostgreSQL).

🚀 Pipeline Workflow
Infrastructure Provisioning: Terraform is used to create the core Amazon S3 bucket, ensuring consistent configurations for versioning and encryption.

Extraction: An AWS Lambda function fetches real-time data from the CoinGecko API and stores the raw JSON in s3://julian-crypto-s3-bucket/raw/.

Metadata Discovery: An AWS Glue Crawler scans the raw S3 prefix to infer the schema and populate the AWS Glue Data Catalog.

Transformation: An AWS Glue ETL Job reads from the Data Catalog, performs data cleaning and type casting, and writes the results to s3://julian-crypto-s3-bucket/processed/.

Loading: The processed data is loaded into an Amazon RDS PostgreSQL instance for final storage and downstream analytical queries.

📁 Repository Structure
/terraform: Contains the .tf files used to provision the S3 storage.

/src/lambda: Python code for the API data extraction.

/src/glue: Transformation logic for the Glue ETL jobs.

/diagrams: High-resolution architecture diagrams.

🔧 Setup Instructions
Terraform: Navigate to /terraform, run terraform init, and terraform apply to create the S3 bucket.

Lambda: Deploy the extraction script and set an EventBridge trigger for your desired interval.

Glue: Run the Crawler to populate the catalog before executing the ETL job.
