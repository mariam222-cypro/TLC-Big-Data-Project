# TLC-Big-Data-Project

 ## Overview

 This project uses the large-scale taxi and limousine data provided by the New York City Taxi and Limousine Commission (TLC). The data is used to explore various aspects of taxi usage, including number of active taxis, number of occupied taxis, average wait time, average fare price, number of pickups and drop-offs, and most popular pickup and drop-off locations. We also break down the data by trip type and payment type.

 ## Getting Started

 ### Prerequisites

 - AWS account: This project uses various AWS services, including S3, Lambda, Kinesis Data Streams, and Kinesis Firehose.
 - Elasticsearch and Kibana: Used for as no-sql database and a data visualization tool in the real-time part of the project.
 - Snowflake: Used as the data warehouse for batching part of the project.
 - Power BI: Used for batch dashboard  visualization related to the Snowflake warehouse.

 ### Data

 The data for the project is  obtained from the TLC's official website. After obtaining the data, we upload it to an S3 bucket.

## Pipeline Main Architecture 
<img width="986" alt="Screenshot 2023-07-18 at 2 12 24 PM" src="https://github.com/mariam222-cypro/TLC-Big-Data-Project/assets/77588576/c332ed8c-d1f7-48af-bd20-dae8dee67e86">

 ## Detailed Pipeline

 The data pipeline in this project consists of two paths:

 1. **Path 0A - Streaming API and Queueing System**:

      <img width="556" alt="Screenshot 2023-07-18 at 2 15 49 PM" src="https://github.com/mariam222-cypro/TLC-Big-Data-Project/assets/77588576/6eb5d769-72e6-4433-aad7-4da0ac3f179c">

  - **S3**: The raw data files were uploaded to an S3 bucket.
 - **AWS Lambda**: Lambda function was created that triggers whenever new data is added to the S3 bucket.
   - This function reads the new data and puts it into a Kinesis Data Stream after applying the nessesery transformation.

 2. **Path 0B - Queueing System**:
    
       <img width="607" alt="Screenshot 2023-07-18 at 2 17 18 PM" src="https://github.com/mariam222-cypro/TLC-Big-Data-Project/assets/77588576/a56c5532-37ec-447f-9dcd-bb0ea3712e17">
    
- The Kinesis Data Stream serves as a buffer for the incoming data to manage real-time ingestion and processing.
- Kinesis Data Streams capture and store data records in the order that they're generated, and allow multiple consumers to process the data in parallel.

 3. **Path 1 - Real-Time data streaming**:
    
    <img width="906" alt="Screenshot 2023-07-18 at 2 16 12 PM" src="https://github.com/mariam222-cypro/TLC-Big-Data-Project/assets/77588576/8f90fd79-3c11-4a9f-a0b8-a05c7bb0f3e5">

     - **Kinesis Data Streams**: The Data Stream serves as a buffer for the incoming data.
     - **Kinesis Firehose**: Attach a Firehose delivery stream to the Data Stream. The FireHose was Configured to transform the incoming data with a Lambda function and then load the transformed data into an Elasticsearch domain.
     - **Elasticsearch**: The transformed data is stored in Elasticsearch and was analyzed and visualized with Kibana.

 2. **Path 2 - Batching data Process**:
    
    <img width="929" alt="Screenshot 2023-07-18 at 2 16 28 PM" src="https://github.com/mariam222-cypro/TLC-Big-Data-Project/assets/77588576/bd3a4275-eb53-4f27-abfd-9ebcaa1e0476">


   - **Kinesis Data Streams**: From the same Kinesis Data Stream as above, another consumer is set up.
     - **AWS Lambda**: This Lambda function applies dimensional modeling to the data from the Stream and puts it into another S3 bucket.
     - **S3**: This bucket acts as a staging area for the modeled data.
     - **Snowflake**: The data was loaded from the S3 bucket into a Snowflake data warehouse.
     - **Power BI**: The Power BI was connected to Snowflake to create visualizations based on the warehouse data.

 ## Contact

 Please raise an issue in the GitHub repo for any questions or comments. We're always happy to hear from users and contributors.
