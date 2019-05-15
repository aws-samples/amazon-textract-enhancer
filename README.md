# Amazon Textract Enhancer - Overview

This workshop demonstrates how to build a text parser and feature extractor with Amazon Textract. With amazon Textract you can  detect text from a PDF document or a scanned image of a printed document to extract lines of text, using [Text Detection API](https://docs.aws.amazon.com/textract/latest/dg/API_DetectDocumentText.html). In addition, you can also use [Document Analysis API](https://docs.aws.amazon.com/textract/latest/dg/API_AnalyzeDocument.html) to extract tables and forms from the scanned document.

It is straightforward to invoke this APIs from AWS CLI or using Boto3 Python library and pass either a pointer to the document image stored in S3 or the raw image bytes to obtain results. However handling large volumes of documents this way becomes impractical for several reasons:
- Making a synchronous call to query Textract API is not possible for multi-page PDF documents
- Synchronous call will exceed provisioned throughput if used for a large number of documents within a short period of time
- If multiple query with same document is needed, triggerign multiple Textract API invocation, cost increases rapidly
- Textract sends analysis results with rich metadata, but the strucutres of tables and forms are not immediately apparent without some post-processing

In this Textract enhancer solution, as demonstrated in this workshop, following approaches are used to provide for a more robust end to end solution.
- Lambda functions triggered by document upload to specific S3 bucket to submit document analysis and text detection jobs to Textract
- API Gateway methods to trigger Textract job submission on-demand
- Asynchronous API calls to start [Document analysis](https://docs.aws.amazon.com/textract/latest/dg/API_StartDocumentAnalysis.html) and [Text detection](https://docs.aws.amazon.com/textract/latest/dg/API_StartDocumentTextDetection.html), with unique request token to prevent duplicate submissions
- Use of SNS topics to get notified on completion of Textract jobs
- Automatically triggered post processing Lambda functions to extract actual tables, forms and lines of text, stored in S3 for future querying
- Job status and metdata tracked in DynamoDB table, allowing for troubleshooting and easy querying of results
- API Gateway methods to retrieve results anytime without having to use Textract

## License Summary

This sample code is made available under a modified MIT license. See the [LICENSE](LICENSE) file.

## Prerequisites

In order to complete this workshop you'll need an AWS Account with access to create:
- AWS IAM Roles
- S3 Bucket
- S3 bucket policies
- SNS topics
- DynamoDB tables
- Lambda functions
- API Gateway endpoints and deployments
    
## 1. Launch stack

Textract Enhancer solution components can each be built by hand, either using [AWS Console](https://console.aws.amazon.com/) or using AWS CLI. [AWS Cloudformation](https://aws.amazon.com/cloudformation/) on the other hand provides mechanism to script the hard work of launching the whole stack. 

You can use the button below to launch the solution stack, the component details of which you can find in the following section.

Region| Launch
------|-----
US East (N. Virginia) | [![Launch Textract Enhancer in us-east-1](http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/images/cloudformation-launch-stack-button.png)](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new?stackName=textract-enhancer&templateURL=https://s3.amazonaws.com/my-python-packages/textract-api-stack.json)


## 2. Architecture
The solution architecture is based solely upon serverless Lambda functions, invoking Textract API endpoints. The architecture uses Textract in asynchronous mode, and uses a DynamoDB table to keep track of job status and response location.
    ![Job submission architecture](images/job-submission-architecture.png)

The solution also uses Rest API backed by anotyher set of Lambda functions and the DynamoDB table to provide for fast querying of the resulting documents from S3 bucket.
    ![Result retrieval architecture](images/result-retrieval-architecture.png)


## 3. Solution components

### 3.1. DyanmoDB Table
- When a Textract job is submitted in asynchronous mode, using a request token, it creates a unique job-id is created. For any subsequent submissions with same document, it prevents Textract from running the same job over again. Since in this solution, two different types of jobs are submitted, one for `DocumentAnalysis` and one for `TextDetection`, a DynamoDB table is used with `JobId` as HASH key and `JobType` as RANGE key, to track the status of the job.
- In order to facilitate table scan with the document location, the table also use a global secondary index, with `DocumentBucket` as HASH key and `DocumentPath` as RANGE key. This information is used by the retrieval functions later when an API request is sent to obtain the tables, forms and lines of texts.
- Upon completion of a job, post processing Lambda functions update the corresponding records in this DynamoDB table with location of the extracted files, as stored in S3 bucket, and other metadata such as completion time, number of pages, lines, tables and form fields.

<details>
<summary><strong>Following snippet shows the schema definition used in defining the table (expand for details)</strong></summary><p>

    ```
    "AttributeDefinitions": [
        {
            "AttributeName": "JobId",
            "AttributeType": "S"
        },       
        {
            "AttributeName": "JobType",
            "AttributeType": "S"
        },                                
        {
            "AttributeName": "DocumentBucket",
            "AttributeType": "S"
        },
        {
            "AttributeName": "DocumentPath",
            "AttributeType": "S"
        }                    
    ],
    "KeySchema": [
        {
            "AttributeName": "JobId",
            "KeyType": "HASH"
        },
        {
            "AttributeName": "JobType",
            "KeyType": "RANGE"
        }                    
    ],
    "GlobalSecondaryIndexes": [
        {
            "IndexName": "DocumentIndex",
            "KeySchema": [
                    {
                        "AttributeName": "DocumentBucket",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "DocumentPath",
                        "KeyType": "RANGE"
                    }
            ],
            "Projection": {
                "ProjectionType": "KEYS_ONLY"
            },
            "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5
            }
        }
    ],
    ```            
</p></details>
        
### 3.2. Lambda execution role

### 3.3. DyanmoDB Table

### 3.4. SNS Topic

### 3.5. Textract service role

### 3.6. Job submission - Lambda function

### 3.7. Post Processing - Lambda functions

### 3.8. S3 Bucket

### 3.9. Rest API



