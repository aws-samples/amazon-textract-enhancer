import os
import time
import boto3
from datetime import datetime


def attachExternalBucketPolicy(externalBucketName):
    iam = boto3.client('iam')
    roleName = os.environ['role_name']
    policyName = externalBucketName+'-bucketaccesspolicy'
    
    policyExists = False
    policyAttached = False
    
    targetPolicy = None
    policies = iam.list_policies(    
        MaxItems=1000
    )['Policies']
    for policy in policies:
        if policy['PolicyName'] == policyName:
            policyExists = True
            targetPolicy = policy['Arn']
            print("Bucket Access Policy for {} already exists".format(externalBucketName))
            break
            
    if policyExists:
        attached_policies = iam.list_attached_role_policies(
            RoleName=roleName,
            MaxItems=100
        )['AttachedPolicies']   
        for policy in attached_policies:
            if policy['PolicyName'] == policyName:
                policyAttached = True
                print("Bucket Access Policy for {} already attached to Role {}".format(externalBucketName, roleName))
                targetPolicy = policy['PolicyArn']
                break;     
            
    if not policyExists:            
        newPolicy = iam.create_policy(
            PolicyName=externalBucketName+'-bucketaccesspolicy',
            PolicyDocument='{\
                                "Version": "2012-10-17",\
                                "Statement": [\
                                    {\
                                        "Effect": "Allow",\
                                        "Action": [\
                                            "s3:ListBucket",\
                                            "s3:ListBucketVersions"\
                                        ],\
                                        "Resource": [\
                                            "arn:aws:s3:::'+externalBucketName+'"\
                                        ]\
                                    },\
                                    {\
                                        "Effect": "Allow",\
                                        "Action": [\
                                            "s3:GetObject",\
                                            "s3:GetGetObjectVersionObject",\
                                            "s3:PutObject",\
                                            "s3:PutObjectAcl"\
                                        ],\
                                        "Resource": [\
                                            "arn:aws:s3:::'+externalBucketName+'/*"\
                                        ]\
                                    }\
                                ]\
                            }',
            Description='Grant access to an external S3 bucket'
        )
        targetPolicy = newPolicy['Policy']['Arn']
        print("Policy - {} created\Policy ARN: {}".format(newPolicy['Policy']['PolicyName'], 
                                                       newPolicy['Policy']['Arn']))

    if not policyAttached:
        response = iam.attach_role_policy(
            RoleName='LambdaTextractRole',
            PolicyArn=targetPolicy
        )
        
    policies = iam.list_attached_role_policies(
        RoleName=roleName,
        MaxItems=100
    )
    print(policies) 
    return targetPolicy
    
def detachExternalBucketPolicy(bucketAccessPolicyArn, event):
    
    iam = boto3.client('iam')
    roleName = os.environ['role_name']
    
    cleanUpAction = ""
    if 'ExternalPolicyCleanup' in event:
        cleanUpAction = event['ExternalPolicyCleanup'].lower()
    
    if cleanUpAction == "detach" or cleanUpAction == "delete":
        response = iam.detach_role_policy(
            RoleName=roleName,
            PolicyArn=bucketAccessPolicyArn
        )
        policies = iam.list_attached_role_policies(
            RoleName=roleName,
            MaxItems=10
        )
        print(policies)
    
    if cleanUpAction == "delete":    
        iam.delete_policy(
            PolicyArn=bucketAccessPolicyArn
        )
        print("Policy - {} deleted".format(bucketAccessPolicyArn))    

def updateResponse(givenjson, updatejson, override = False):
    for key in updatejson.keys():
        if key not in givenjson or override == True:
            givenjson[key] = updatejson[key]
    return givenjson

def submitDocumentAnalysisJob(bucket, document, tokenPrefix, retryInterval, maxRetryAttempt, topicArn, roleArn, table_name):

    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.client('dynamodb')    
    retryCount = 0
    jsonresponse = {}
    jobId = ""
    jobStartTimeStamp = 0
    jobCompleteTimeStamp = 0    
    document_path = document[:document.rfind("/")] if document.find("/") >= 0 else ""
    document_name = document[document.rfind("/")+1:document.rfind(".")] if document.find("/") >= 0 else document[:document.rfind(".")]
    document_type = document[document.rfind(".")+1:].upper()

    print("DocumentAnalysisJob: ClientRequestToken = {}-{}".format(tokenPrefix, document.replace("/","_").replace(".","-")))
    print("DocumentAnalysisJob: DocumentLocation = 'S3Object': 'Bucket': {}, 'Name': {}".format(bucket, document))
    print("DocumentAnalysisJob: NotificationChannel = 'SNSTopicArn': {},'RoleArn': {}".format(topicArn, roleArn))
    print("DocumentAnalysisJob: JobTag = {}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]))
            
    #Submit Document Anlysis job to Textract to extract text features    
    while retryCount >= 0 and retryCount < maxRetryAttempt:
        try:
            response = textract.start_document_analysis(
                                    ClientRequestToken = "{}-{}".format(tokenPrefix, document.replace("/","_").replace(".","-")),
                                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}},
                                    FeatureTypes=["TABLES", "FORMS"],
                                    NotificationChannel={'SNSTopicArn': topicArn,'RoleArn': roleArn},
                                    JobTag = "{}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]))
            jobId = response['JobId']
            jobStartTimeStamp = datetime.strptime(response['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S %Z').timestamp()
            print("Textract Request: {} submitted at {} with JobId - {}".format(
                response['ResponseMetadata']['RequestId'], jobStartTimeStamp,jobId))    
            
            print("Starting Document Analysis Job: {}".format(jobId))        
        except Exception as e:
            print(e.response['Error'])
            if e.response['Error']['Code'] == 'InvalidParameterException':
                return {'Operation': 'DocumentAnalysis', 'Error': e.response['Error']['Code']}
            elif retryCount < maxRetryAttempt - 1:
                retryCount = retryCount + 1
                print("Job submission failed, retrying after {} seconds".format(retryInterval))            
                time.sleep(retryInterval)
            else:
                print("Job submission failed, after {} retry, aborting".format(maxRetryAttempt))
                retryCount = -1
                return jsonresponse
        else:
            retryCount = -1   

    if document_path == "":
        upload_prefix = jobId
    else:
        upload_prefix = "{}/{}".format(document_path, jobId)

    jsonresponse['DocumentAnalysisJobId'] = jobId
    jsonresponse['DocumentBucket'] = bucket
    jsonresponse['DocumentKey'] = document
    jsonresponse['DocumentAnalysisUploadPrefix'] = upload_prefix
    jsonresponse['DocumentName'] = document_name
    jsonresponse['DocumentType'] = document_type
    jsonresponse['DocumentAnalysisJobStartTimeStamp'] = str(jobStartTimeStamp)
    jsonresponse['DocumentAnalysisJobCompleteTimeStamp'] = '0'
    jsonresponse['NumPages'] = '0'
    jsonresponse['NumTables'] = '0'
    jsonresponse['NumFields'] = '0'      
    jsonresponse['TableFiles'] = []
    jsonresponse['FormFiles'] = []        

    
    recordExists = False
    
    try:
        response = dynamodb.scan(
            TableName=table_name,
            ExpressionAttributeNames={'#ID': 'JobId', '#Type': 'JobType'},
            ExpressionAttributeValues={':jobId' : {'S': jobId}, ':jobType' : {'S': 'DocumentAnalysis'}},
            FilterExpression='#ID = :jobId and #Type = :jobType'
        )      
        if response['Count'] > 0:
            recordExists = True
            item = response['Items'][-1]
            jsonresponse['JobStartTimeStamp'] = int(item['JobStartTimeStamp']['N'])
            jsonresponse['JobCompleteTimeStamp'] = int(item['JobCompleteTimeStamp']['N'])
            jsonresponse['NumPages'] = int(item['NumPages']['N'])
            jsonresponse['NumTables'] = int(item['NumTables']['N'])
            jsonresponse['NumFields'] = int(item['NumFields']['N'])            
            tableFiles = []            
            for tableFile in item['TableFiles']['L']:
                tableFiles.append(tableFile['S'])
            jsonresponse['TableFiles'] = tableFiles
            formFiles = []
            for formFile in item['FormFiles']['L']:
                formFiles.append(formFile['S'])            
            jsonresponse['FormFiles'] = formFiles                 
            
    except Exception as e:
        print('DynamoDB Read Error is: {0}'.format(e))  
    
    if not recordExists:
        try:
            response = dynamodb.update_item(
                TableName=table_name,
                Key={
                    'JobId':{'S':jobId},
                    'JobType':{'S':'DocumentAnalysis'}
                },
                AttributeUpdates={
                    'DocumentBucket':{'Value': {'S':bucket}},
                    'DocumentKey':{'Value': {'S':document}},
                    'UploadPrefix':{'Value': {'S':upload_prefix}},
                    'DocumentName':{'Value': {'S':document_name}},
                    'DocumentType':{'Value': {'S':document_type}},                        
                    'JobStartTimeStamp':{'Value': {'N':str(jobStartTimeStamp)}},
                    'JobCompleteTimeStamp':{'Value': {'N':'0'}},
                    'NumPages':{'Value': {'N':'0'}},
                    'NumTables':{'Value': {'N':'0'}},
                    'NumFields':{'Value': {'N':'0'}},                        
                    'TableFiles':{'Value': {'L':[]}},
                    'FormFiles':{'Value': {'L':[]}}
                }
            )
        except Exception as e:
            print('DynamoDB Insertion Error is: {0}'.format(e))

    return jsonresponse
        
def submitTextDetectionJob(bucket, document, tokenPrefix, retryInterval, maxRetryAttempt, topicArn, roleArn, table_name):

    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.client('dynamodb')    
    retryCount = 0
    jsonresponse = {}
    jobId = ""
    jobStartTimeStamp = 0
    jobCompleteTimeStamp = 0    
    document_path = document[:document.rfind("/")] if document.find("/") >= 0 else ""
    document_name = document[document.rfind("/")+1:document.rfind(".")] if document.find("/") >= 0 else document[:document.rfind(".")]
    document_type = document[document.rfind(".")+1:].upper()

    print("TextDetectionsJob: ClientRequestToken = {}-{}".format(tokenPrefix, document.replace("/","_").replace(".","-")))
    print("TextDetectionJob: DocumentLocation = 'S3Object': 'Bucket': {}, 'Name': {}".format(bucket, document))
    print("TextDetectionJob: NotificationChannel = 'SNSTopicArn': {},'RoleArn': {}".format(topicArn, roleArn))
    print("TextDetectionJob: JobTag = {}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]))
    
    #Submit Text Detection job to Textract to detect lines of text    
    while retryCount >= 0 and retryCount < maxRetryAttempt:
        try:
            response = textract.start_document_text_detection(
                                    ClientRequestToken = "{}-{}".format(tokenPrefix, document.replace("/","_").replace(".","-")),
                                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}},
                                    NotificationChannel={'SNSTopicArn': topicArn,'RoleArn': roleArn},
                                    JobTag = "{}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]))
            jobId = response['JobId']
            jobStartTimeStamp = datetime.strptime(response['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S %Z').timestamp()
            print("Textract Request: {} submitted at {} with JobId - {}".format(
                response['ResponseMetadata']['RequestId'], jobStartTimeStamp,jobId))    
            
            print("Starting Text Detection Job: {}".format(jobId))        

        except Exception as e:
            print(e.response['Error'])
            if e.response['Error']['Code'] == 'InvalidParameterException':
                return {'Operation': 'TextDetection', 'Error': e.response['Error']['Code']}
            elif retryCount < maxRetryAttempt - 1:
                retryCount = retryCount + 1
                print("Job submission failed, retrying after {} seconds".format(retryInterval))            
                time.sleep(retryInterval)
            else:
                print("Job submission failed, after {} retry, aborting".format(maxRetryAttempt))
                retryCount = -1
                return jsonresponse
        else:
            retryCount = -1    

    if document_path == "":
        upload_prefix = jobId
    else:
        upload_prefix = "{}/{}".format(document_path, jobId)

    jsonresponse['TextDetectionJobId'] = jobId
    jsonresponse['DocumentBucket'] = bucket
    jsonresponse['DocumentKey'] = document
    jsonresponse['TextDetectionUploadPrefix'] = upload_prefix
    jsonresponse['DocumentName'] = document_name
    jsonresponse['DocumentType'] = document_type
    jsonresponse['TextDetectionJobStartTimeStamp'] = str(jobStartTimeStamp)
    jsonresponse['TextDetectionJobCompleteTimeStamp'] = '0'
    jsonresponse['NumPages'] = '0'
    jsonresponse['NumLines'] = '0'
    jsonresponse['TextFiles'] = []        

    
    recordExists = False
    
    try:
        response = dynamodb.scan(
            TableName=table_name,
            ExpressionAttributeNames={'#ID': 'JobId', '#Type': 'JobType'},
            ExpressionAttributeValues={':jobId' : {'S': jobId}, ':jobType' : {'S': 'TextDetection'}},
            FilterExpression='#ID = :jobId and #Type = :jobType'
        )                  
        if response['Count'] > 0:
            recordExists = True
            item = response['Items'][-1]
            jsonresponse['JobStartTimeStamp'] = int(item['JobStartTimeStamp']['N'])
            jsonresponse['JobCompleteTimeStamp'] = int(item['JobCompleteTimeStamp']['N'])
            jsonresponse['NumPages'] = int(item['NumPages']['N'])
            jsonresponse['NumLines'] = int(item['NumLines']['N'])
            textFiles = []
            for textFile in item['TextFiles']['L']:
                textFiles.append(textFile['S'])            
            jsonresponse['TextFiles'] = textFiles                  
            
    except Exception as e:
        print('DynamoDB Read Error is: {0}'.format(e))  
    
    if not recordExists:
        try:
            response = dynamodb.update_item(
                TableName=table_name,
                Key={
                    'JobId':{'S':jobId},
                    'JobType':{'S':'TextDetection'}
                },
                AttributeUpdates={
                    'DocumentBucket':{'Value': {'S':bucket}},
                    'DocumentKey':{'Value': {'S':document}},
                    'UploadPrefix':{'Value': {'S':upload_prefix}},
                    'DocumentName':{'Value': {'S':document_name}},
                    'DocumentType':{'Value': {'S':document_type}},                        
                    'JobStartTimeStamp':{'Value': {'N':str(jobStartTimeStamp)}},
                    'JobCompleteTimeStamp':{'Value': {'N':'0'}},
                    'NumPages':{'Value': {'N':'0'}},
                    'NumLines':{'Value': {'N':'0'}},
                    'TextFiles':{'Value': {'L':[]}}
                }
            )
        except Exception as e:
            print('DynamoDB Insertion Error is: {0}'.format(e))

    return jsonresponse
        
def lambda_handler(event, context): 
    print(event)
    
    #Initialize Boto Resource	
    table_name=os.environ['table_name']
    documentAnalysisTokenPrefix = os.environ['document_analysis_token_prefix']  
    textDetectionTokenPrefix = os.environ['text_detection_token_prefix']  
    roleArn = os.environ['role_arn']
    documentAnalysisTopicArn = os.environ['document_analysis_topic_arn'] 
    textDetectionTopicArn = os.environ['text_detection_topic_arn']

    
    retryInterval = int(os.environ['retry_interval']) #30
    maxRetryAttempt = int(os.environ['max_retry_attempt']) #5
    
    external_bucket = ""
    bucket = ""
    document = ""
    bucketAccessPolicyArn = None
    
    if 'ExternalBucketName' in event:
        bucketAccessPolicyArn = attachExternalBucketPolicy(event['ExternalBucketName'])
        external_bucket = event['ExternalBucketName']
        
    if "Records" in event:        
        record, = event["Records"]        
        print(record)
        bucket = record['s3']['bucket']['name']
        document = record['s3']['object']['key']
    else:
        bucket = event['ExternalBucketName']
        document = event['ExternalDocumentPrefix']   
        
    if bucket == "" or  document == "":
        print("Bucket and/or Document not specified, nothing to do.")
        return {}

    documentAnalysisResponse = submitDocumentAnalysisJob(bucket, document, 
                                                        documentAnalysisTokenPrefix, 
                                                        retryInterval, maxRetryAttempt, 
                                                        documentAnalysisTopicArn, 
                                                        roleArn, table_name)
    print("DocumentAnalysisResponse = {}".format(documentAnalysisResponse))

    textDetectionResponse = submitTextDetectionJob(bucket, document, 
                                                    textDetectionTokenPrefix, 
                                                    retryInterval, maxRetryAttempt, 
                                                    textDetectionTopicArn, 
                                                    roleArn, table_name)
    print("TextDetectionResponse = {}".format(textDetectionResponse))
        
    jsonresponse = updateResponse(documentAnalysisResponse, textDetectionResponse, False)

    if 'Error' in jsonresponse:
        return jsonresponse
        
    if bucketAccessPolicyArn is not None:
        detachExternalBucketPolicy(bucketAccessPolicyArn, event)
        
    return jsonresponse
