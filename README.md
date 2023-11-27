# Coding Challenge Mbition

## 1. **Problem Statement:**

The data file is in JSON format and is readily available for use. The data is available in multilevel JSON format. The data from the files has to be extracted and transformed to be in a format that can be readily loaded to a relational data warehouse. The data model for the data warehouse should be designed based on the provided JSON data. The designed data tables should be able to provide business insights pertaining to development activity.

**Objective:** 

The objective of this project is to construct a data warehouse which is capable of answering business queries pertaining to development activity. This involves building a data model for a data warehouse that efficiently stores the data and enables user to optimally run queries on the tables for answering business questions. The extract transform and load is to be handled using PySpark and ensure data quality throughout the data life cycle.

## 2. Assumptions and Scope:

**In Scope:**

For this challenge the following are in scope 

1. Create Data model for the data provided related to development activity.
2. Load data from the data file and do the necessary transformations using PySpark
3. The transformed data frames can be directly loaded to the tables designed in the data model
4. The transformed data should have the capability to answer the business questions provided with the challenge 
5. Fields which are needed for answering the questions in challenge only is focused for extraction

**Not In Scope for this Challenge:** 

1. Load data from the transformations to Redshift/Postgres
2. Fields that are not part of the questions is not given importance for extraction 

## 3. **Approach and Strategy:**

**Solution:**

High Level design for the steps included in the scope is below 

![Current Implementation](Current%20Implementation.png)

## 4. **Implementation:**

**Technology Stack:** 

- PySpark for Extract, Transform and Load

**Datawarehouse Data Model:**

![Datawarehouse Data Model](GitHub%20DataModel.png)


**Decision-making Process:** 

- PySpark is chosen for the data processing and analysis because of the following reasons
    - **High** **Scalability and Distributed computing** enables to process huge datasets and perform analysis with ease
    - It possess significant speed while processing large volume of data due to its capacity to do in memory processing and other optimisation techniques like partitioning, caching
    - Ease of use as it offers supports python
- Decided to not use the Redshift load from PySpark due to resource constraints
- Used Dataframes themselves for answering the questions

**Code Structure:** 

- Git Repository
- The data from the two files is read and loaded to Data frames one each for pull request and Issues
- The facts like commits, deletes, additions etc related to Pull request and Issues are related and loaded to a Data frame which will be the input for `FACT_PR_ISSUES_METRICS` table
- Issues data is extracted with its metadata to a data frame that can be loaded to a dimension table `DIM_ISSUES`
- The data for Labels from array structure is extracted and loaded to Labels data frame, this holds the labels related to each pull request

**Data Quality**

Data quality here is assured by making the following considerations in code 

- **Duplicate Removal:** Removing the duplicate rows from the data frame for the cases like `FACT_PR_ISSUES_METRICS` and `DIM_ISSUES` using the primary key and form `DIM_LABELS` table using the entire row
- **Data Type check:** Ensured that the data types of extracted columns match the expected types as that of the source column.
- Further checks that can be done:
    - Handling Null and missing values
    - To maintain data consistency validate the data columns for any predefined constrains, like length, not null etc.

**Data Security**

Data Security can be enhanced by making the following 

- Data can be protected by classifying the confidentiality of data fields and encrypt them accordingly
- Role bases access control can be implemented to to have high data integrity

## 5. **Results and Evaluation:**

- All of the Extract Transform and Loads are done using PySpark
- All of the 4 questions part of the challenge are answered
- All the business questions part of the challenge are answered using the PySpark for the interest of time and for the purpose of coding challenge
- In real working environment a data warehouse can be modelled as per the above created data model and the tables can be directly loaded from the above designed data frames

## 6. How to Run and Test the Solution

1. Configure the Python interpreter in the respective IDE if not configured already. Follow the instructions [here](https://www.youtube.com/watch?v=4rTIjbsnJ-w) to configure the interpreter in Pycharm
2. Change the path of the input data file accordingly.
3. Run the code form the IDE using the Run button or by executing the following command in the terminal by going inside the source directory:
```
python Driver.py
```

## 6. Future Enhancements:

**What would be my approach for a project of large scale for the development activity use cases**

### **Approach and Strategy:**

The high level system design for the mentioned problem statement is as below:

 ![Future Enhancement](Further%20Enhancements.png)


**Description of data workflows (from left to right):**

1. Step Functions are used to periodically schedule, monitor, orchestrate and eventually retry ETL jobs. ETL jobs can be implemented using any AWS Processing service (e.g., Glue, EMR, Lambdas) according to use case. This approach grants the best flexibility in terms of performance and optimal costs. These services support PySpark.
2. ETL jobs write their results to Data Store, which are limited to S3s buckets. Reason for this is to allow Data Source to be managed through Lake Formation, which can register S3 buckets only ([ref](https://docs.aws.amazon.com/lake-formation/latest/dg/register-data-lake.html))
3. Glue Crawler scans Data Sources, creating/updating Glue Data Catalog. Crawlers will be either a. Scheduled periodically (where data is not expected to change over time) b. Be part of Step Functions executed in ETL jobs (where DTs schema can be updated).
4. Once the data lands in S3, same can be loaded to Amazon Redshift using multiple options like ****AWS data pipelines, AWS Glue ETL Jobs, AWS Lambda or Custom Scripts****
5. Additionally the table being cataloged can be accessed via Redshift spectrum
6. Native AWS customers (Athena queries, Sage maker etc.) can access Data Catalog (i.e., Databases and Tables) through Lake Formation (which is responsible for [Access Control](https://w.amazon.com/bin/view/EUCX/Lifecycle/Projects/Quicksilver/Design/Clio/#H7.2AccessControl))

**Pros:**

- This solution makes sure that the with a single read and transform the pipeline can now  support multiple customers.
- The load on the redshift is reduced as all the transformations are handled outside of Redshift
- Having S3 as the Data Target layer enables multiple diverse use cases including machine learning use cases

**Cons:**

- Security and compliance issue with data share across multiple regions
