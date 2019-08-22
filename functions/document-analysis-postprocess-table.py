from textract_util import *
import io
import os
import json
import time
import boto3

def lambda_handler(event, context):
    
    #Initialize Boto Resource	
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.client('dynamodb')
    table_name=os.environ['table_name']    
    file_list = []

    if "Records" in event:        
        records = event['Records']
        numRecords = len(records)
        
        print("{} messages recieved".format(numRecords))
        for record in records:
            documentBlocks = None
            num_pages = 0
            num_tables = 0    
            bucket = ""
            upload_prefix = ""            
            textractJobId = ""
            textractStatus = ""
            textractAPI = ""
            textractJobTag = ""
            textractS3ObjectName = ""
            textractS3Bucket = ""            
            if 'Sns' in record.keys():
                sns = record['Sns']                               
                if 'Message' in sns.keys():
                    message = json.loads(sns['Message'])
                    textractJobId = message['JobId']
                    print("{} = {}".format("JobId", textractJobId))
                    textractStatus = message['Status']
                    print("{} = {}".format("Status",textractStatus))   
                    textractTimestamp =  str(int(float(message['Timestamp'])/1000)) 
                    print("{} = {}".format("Timestamp",textractTimestamp))     
                    textractAPI = message['API']
                    print("{} = {}".format("API", textractAPI))                    
                    textractJobTag = message['JobTag']
                    print("{} = {}".format("JobTag", textractJobTag))    
                    documentLocation = message['DocumentLocation']
                    textractS3ObjectName = documentLocation['S3ObjectName']
                    print("{} = {}".format("S3ObjectName", textractS3ObjectName))    
                    textractS3Bucket = documentLocation['S3Bucket']
                    print("{} = {}".format("S3Bucket", textractS3Bucket))                          
                    
                    bucket = textractS3Bucket
                    document_path = textractS3ObjectName[:textractS3ObjectName.rfind("/")] if textractS3ObjectName.find("/") >= 0 else ""
                    document_name = textractS3ObjectName[textractS3ObjectName.rfind("/")+1:textractS3ObjectName.rfind(".")] if textractS3ObjectName.find("/") >= 0 else textractS3ObjectName[:textractS3ObjectName.rfind(".")]
                    document_type = textractS3ObjectName[textractS3ObjectName.rfind(".")+1:].upper()

                    if document_path == "":
                        upload_prefix = textractJobId
                    else:
                        upload_prefix = "{}/{}".format(document_path, textractJobId)

                    print("upload_prefix = " + upload_prefix)  

                    num_pages, documentBlocks = GetDocumentAnalysisResult(textract, textractJobId) 

            if documentBlocks is not None and len(documentBlocks) > 0:
                print("{} Blocks retrieved".format(len(documentBlocks)))

                #Extract table information  into a Python dictionary by parsing the raw JSON from Textract
                tabledict = extractTableBlocks(documentBlocks)
                
                #Generate XML document using table information    
                num_tables, tables = generateTableXML(tabledict)
                
                for page in tables:
                    for table in page:
                        html_document = "{}-page-{}-table-{}.html".format(document_name, table.attrib['ContainingPage'], table.attrib['TableNumber'])
                        html_file = open("/tmp/"+html_document,'w+')
                        html_file.write(prettify(table))
                        html_file.close()
                        s3.meta.client.upload_file("/tmp/"+html_document, bucket, "{}/{}".format(upload_prefix,html_document))  
                        try:
                            response = dynamodb.update_item(
                                TableName=table_name,
                                Key={
                                    'JobId':{'S':textractJobId},
                                    'JobType':{'S':'DocumentAnalysis'}
                                },
                                ExpressionAttributeNames={"#tf": "TableFiles", "#jst": "JobStatus", "#jct": "JobCompleteTimeStamp", "#nt": "NumTables", "#np": "NumPages"},
                                UpdateExpression='SET #tf = list_append(#tf, :table_files), #jst = :job_status, #jct = :job_complete, #nt = :num_tables, #np = :num_pages',
                                ExpressionAttributeValues={
                                    ":table_files": {"L": [{"S": "{}/{}".format(upload_prefix,html_document)}]},
                                    ":job_status": {"S": textractStatus},
                                    ":job_complete": {"N": str(textractTimestamp)},
                                    ":num_tables": {"N": str(num_tables)},
                                    ":num_pages": {"N": str(num_pages)}
                                }
                            )
                        except Exception as e:
                            print('DynamoDB Insertion Error is: {0}'.format(e))
            else:
                try:
                    response = dynamodb.update_item(
                        TableName=table_name,
                        Key={
                            'JobId':{'S':textractJobId},
                            'JobType':{'S':'DocumentAnalysis'}
                        },
                        ExpressionAttributeNames={"#jst": "JobStatus", "#jct": "JobCompleteTimeStamp"},
                        UpdateExpression='SET #jst = :job_status, #jct = :job_complete',
                        ExpressionAttributeValues={
                            ":job_status": {"S": textractStatus},
                            ":job_complete": {"N": str(textractTimestamp)}
                        }
                    )
                except Exception as e:
                    print('DynamoDB Insertion Error is: {0}'.format(e))       
                    
            s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter = "/")
            if 'Contents' in s3_result:
                
                for key in s3_result['Contents']:
                    if key['Key'].endswith("html"):
                        file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))
                
                while s3_result['IsTruncated']:
                    continuation_key = s3_result['NextContinuationToken']
                    s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter="/", ContinuationToken=continuation_key)
                    for key in s3_result['Contents']:
                        if key['Key'].endswith("html"):
                            file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))            
            print(file_list)   
        
    return file_list