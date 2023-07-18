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

 ## Data Pipeline

 The data pipeline in this project consists of two paths:

 1. **Path 1 - Real-Time data streaming**:

     - **S3**: Upload the raw data files to an S3 bucket.
     - **AWS Lambda**: Create a Lambda function that triggers whenever new data is added to the S3 bucket. This function reads the new data and puts it into a Kinesis Data Stream after applying the nessesery transformation.
     - **Kinesis Data Streams**: The Data Stream serves as a buffer for the incoming data.
     - **Kinesis Firehose**: Attach a Firehose delivery stream to the Data Stream. The FireHose was Configured to transform the incoming data with a Lambda function and then load the transformed data into an Elasticsearch domain.
     - **Elasticsearch**: The transformed data is stored in Elasticsearch and was analyzed and visualized with Kibana.

 2. **Path 2 - Batching data Process**:

     - **Kinesis Data Streams**: From the same Kinesis Data Stream as above, another consumer is set up.
     - **AWS Lambda**: This Lambda function applies dimensional modeling to the data from the Stream and puts it into another S3 bucket.
     - **S3**: This bucket acts as a staging area for the modeled data.
     - **Snowflake**: The data was loaded from the S3 bucket into a Snowflake data warehouse.
     - **Power BI**: The Power BI was connected to Snowflake to create visualizations based on the warehouse data.

 ## Contact

 Please raise an issue in the GitHub repo for any questions or comments. We're always happy to hear from users and contributors.
