import os
import time
import boto3

#Function to retrieve result of completed analysis job
def GetDocumentAnalysisResult(textract, jobId):
    maxResults = int(os.environ['max_results']) #1000
    paginationToken = None
    finished = False 
    retryInterval = int(os.environ['retry_interval']) #30
    maxRetryAttempt = int(os.environ['max_retry_attempt']) #5

    result = []

    while finished == False:
        retryCount = 0

        try:
            if paginationToken is None:
                response = textract.get_document_analysis(JobId=jobId,
                                            MaxResults=maxResults)  
            else:
                response = textract.get_document_analysis(JobId=jobId,
                                                MaxResults=maxResults,
                                                NextToken=paginationToken)
        except :
            if retryCount < maxRetryAttempt:
                retryCount = retryCount + 1
                print("Result retrieval failed, retrying after {} seconds".format(retryInterval))            
                time.sleep(retryInterval)
            else:
                print("Result retrieval failed, after {} retry, aborting".format(maxRetryAttempt))             

        #Get the text blocks
        blocks=response['Blocks']
        print ('Retrieved {} Blocks from Document Text'.format(len(blocks)))

        # Display block information
        for block in blocks:
            result.append(block)
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                paginationToken = None
                finished = True  
    
    return result

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
            break;
            
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

def lambda_handler(event, context):
    
    print(event)
    
    #Initialize Boto Resource	
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    sqs = boto3.client('sqs')
    
    tokenPrefix = os.environ['token_prefix']  

    roleArn = os.environ['role_arn']
    topicArn = os.environ['topic_arn'] 

    retryCount = 0
    retryInterval = int(os.environ['retry_interval']) #30
    maxRetryAttempt = int(os.environ['max_retry_attempt']) #5
    
    external_bucket = ""
    bucket = ""
    document = ""

    jsonresponse = {}
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
        return jsonresponse

    document_path = document[:document.rfind("/")] if document.find("/") >= 0 else ""
    document_name = document[document.rfind("/")+1:document.rfind(".")]    
        
    #Submit Document Anlysis job to Textract to extract text features    
    while retryCount >= 0 and retryCount < maxRetryAttempt:
        try:
            response = textract.start_document_analysis(
                                    ClientRequestToken = "{}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]),
                                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}},
                                    FeatureTypes=["TABLES", "FORMS"],
                                    NotificationChannel={'SNSTopicArn': topicArn,'RoleArn': roleArn},
                                    JobTag = "AnalyzeText-{}".format(document[document.rfind("/")+1:document.rfind(".")]))
            print("Textract Request: {} submitted at {} with JobId - {}".format(
                response['ResponseMetadata']['RequestId'],
                response['ResponseMetadata']['HTTPHeaders']['date'],
                response['JobId']
                ))    
            jobId = response['JobId']
            print("Starting Job Id: {}".format(jobId))        
        except Exception as e:
            print(e)
            if retryCount < maxRetryAttempt - 1:
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

    print("upload_prefix = " + upload_prefix)

    jsonresponse = {
        'jobId': jobId,
        'upload_prefix': upload_prefix,
        'bucket': bucket,
        'document': document,
        'external_bucket': external_bucket
    }
        
    return jsonresponse
