🏥 AWS Healthcare ETL Project  


📌 Project Overview  
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline for healthcare data using AWS managed services. The pipeline is designed to handle both Full Load and Delta (Incremental) Load processing in a scalable, serverless, and production-ready manner.

Amazon S3 is used as the central data lake for storing raw and curated healthcare datasets. AWS Glue is used to perform data transformation using Python-based ETL jobs. Delta processing is enabled using metadata stored in Amazon DynamoDB. AWS Glue Crawlers and the Glue Data Catalog are used for schema discovery, and Amazon Athena enables SQL-based analytics. Amazon SNS is used to send ETL job completion notifications.

Actual healthcare data is not included in the repository to maintain data privacy. The project focuses on architecture, configuration, and ETL logic following real-world data engineering best practices.

---

🏗 Architecture Overview  
Amazon S3 stores raw healthcare data for Full Load and Delta Load processing.  
AWS Glue performs ETL transformations.  
Amazon DynamoDB stores metadata required for incremental processing.  
AWS Glue Crawlers scan curated data and update the Glue Data Catalog.  
Amazon Athena is used for querying and validating transformed data.  
Amazon SNS sends notifications on job execution status.

---

🔄 Data Load Types  

Full Load  
The Full Load process ingests the complete historical dataset from Amazon S3. It is typically used during initial data ingestion or when a full reload of data is required. The transformed output is written to curated S3 locations.

Delta Load  
The Delta Load process ingests only new or updated records. It uses metadata stored in DynamoDB, such as last processed timestamps, to identify incremental changes. This approach improves performance and reduces processing cost.

---

📂 Project Structure Explanation  

athena 🔍  
Includes Athena SQL queries used for data validation, reporting, and analytics on curated healthcare datasets.

config ⚙️  
Stores configuration files and parameters used by Glue jobs to support configuration-driven ETL execution.

crawler 📚  
Contains documentation related to AWS Glue Crawlers, which are responsible for schema discovery and catalog updates.

mapping 🔄  
Defines source-to-target field mappings used during data transformation and business logic implementation.

outputs 📤  
Stores sample outputs or execution results used for validation and demonstration purposes.

s3 🗂️  
Documents the Amazon S3 data layout, including raw and curated zones used in Full Load and Delta Load processing.

sns 📣  
Contains SNS notification samples and documentation related to ETL job completion alerts.

src ⚙️  
Contains the core ETL implementation code including Full Load and Delta Load Glue job scripts, transformation logic, DynamoDB metadata handling, and SNS notification publishing.

.gitignore 🚫  
Ensures unnecessary or sensitive files are excluded from version control.

architecture 📐  
Contains the complete project architecture documentation and diagrams, including the overall ETL architecture.


---

🔁 End-to-End ETL Flow  
Healthcare source data is uploaded to Amazon S3.  
AWS Glue executes Full Load or Delta Load ETL jobs.  
Delta jobs read and update processing metadata in DynamoDB.  
Transformed data is written to curated S3 locations.  
Glue Crawlers update the Glue Data Catalog.  
Amazon Athena queries curated datasets.  
Amazon SNS sends job completion notifications.

---

🌟 Key Highlights  
Supports Full Load and Delta Load processing  
Metadata-driven incremental ETL design  
Automated schema discovery  
Serverless analytics using Athena  
Clean and modular project structure  
Production-oriented AWS ETL architecture

---

✅ Conclusion  
This healthcare ETL project demonstrates a real-world AWS batch data processing architecture. It showcases best practices in ETL design, incremental processing, metadata management, and analytics using fully managed AWS services.