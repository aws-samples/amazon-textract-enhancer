from textract_util import *
import io
import os
import json
import time
import boto3
from xml.etree import ElementTree


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
        
    #Initialize Boto Resource	
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.client('dynamodb')
    table_name=os.environ['table_name']

    retryCount = 0
    retryInterval = int(os.environ['retry_interval']) #30
    maxRetryAttempt = int(os.environ['max_retry_attempt']) #5

    num_tables = -1
    file_list = []
    
    bucket = ""
    document = ""

    bucketAccessPolicyArn = None
    
    if 'ExternalBucketName' in event:
        bucketAccessPolicyArn = attachExternalBucketPolicy(event['ExternalBucketName'])
        
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
        return file_list

    document_path = document[:document.rfind("/")] if document.find("/") >= 0 else ""
    document_name = document[document.rfind("/")+1:document.rfind(".")]    
    documentBlocks = None
    jobId = ""
        
    #Analyze Document using a synchronous call to Textract to extract text features    
    while retryCount >= 0 and retryCount < maxRetryAttempt:
        try:
            response = textract.analyze_document(
                                    Document={'S3Object': {'Bucket': bucket, 'Name': documents[2]}},
                                    FeatureTypes=["TABLES", "FORMS"])

            jobId = response['ResponseMetadata']['RequestId']  
            documentBlocks = response['Blocks']           
            print("RequestId: {}, Status: {}, Pages Processed: {}".format(jobId,
                                                                        response['ResponseMetadata']['HTTPStatusCode'],
                                                                        response['DocumentMetadata']['Pages']))                                                                                                   
        except Exception as e:
            exceptionType = str(type(e))
            if exceptionType.find("AccessDeniedException") > 0:
                retryCount = -1
                print("You aren't authorized to perform textract.analyze_document action.")    
            elif exceptionType.find("BadDocumentException") > 0:
                retryCount = -1
                print("Textract isn't able to read the document.")   
            elif exceptionType.find("DocumentTooLargeException") > 0:
                retryCount = -1
                print("The document can't be processed because it's too large. The maximum document size for synchronous operations 5 MB.")           
            elif exceptionType.find("InvalidParameterException") > 0:
                retryCount = -1
                print("An input parameter violated a constraint.")        
            elif exceptionType.find("InvalidS3ObjectException") > 0:
                retryCount = -1
                print("S3 object doesn't exist")  
            elif exceptionType.find("UnsupportedDocumentException") > 0:
                retryCount = -1
                print("The format of the input document isn't supported.")   
            else:
                retryCount = retryCount + 1
                if exceptionType.find("InternalServerError") > 0:
                    print("Amazon Textract experienced a service issue. Trying in {} seconds.".format(retryInterval))   
                    time.sleep(retryInterval)
                elif exceptionType.find("ProvisionedThroughputExceededException") > 0:
                    print("The number of requests exceeded your throughput limit. Trying in {} seconds.".format(retryInterval*3))
                    time.sleep(retryInterval*3)
                elif exceptionType.find("ThrottlingException") > 0:
                    print("Amazon Textract is temporarily unable to process the request. Trying in {} seconds.".format(retryInterval*6))
                    time.sleep(retryInterval*6)
        else:
            retryCount = -1   
    if document_path == "":
        upload_prefix = jobId
    else:
        upload_prefix = "{}/{}".format(document_path, jobId)

    print("upload_prefix = " + upload_prefix)

    if documentBlocks is not None:
        print("{} Blocks retrieved".format(len(documentBlocks)))

        #Extract form fields into a Python dictionary by parsing the raw JSON from Textract
        blocks = textract_util.groupBlocksByType(documentBlocks)
        formKeys, formValues = textract_util.extractKeyValuePairs(blocks)
        pageWords = textract_util.extractWords(blocks)

        #Generate JSON document using form fields information  
        formEntries = textract_util.generateFormEntries(formKeys, formValues, pageWords)   

        json_document = "{}.json".format(document_name)
        json_file = open("/tmp/"+json_document,'w+')
        json_file.write(json.dumps(formEntries, indent=4, sort_keys=True))
        json_file.close()
        s3.meta.client.upload_file("/tmp/"+json_document, bucket, "{}/{}".format(upload_prefix,json_document)) 

        #Extract table information  into a Python dictionary by parsing the raw JSON from Textract
        tabledict = textract_util.extractTableBlocks(documentBlocks)
        
        #Generate XML document using table information    
        num_tables, tables = textract_util.generateTableXML(tabledict)        

        for page in tables:
            for table in page:
                html_document = "{}-page-{}-table-{}.html".format(document_name, table.attrib['ContainingPage'], table.attrib['TableNumber'])
                html_file = open("/tmp/"+html_document,'w+')
                html_file.write(textract_util.prettify(table))
                html_file.close()
                s3.meta.client.upload_file("/tmp/"+html_document, bucket, "{}/{}".format(upload_prefix,html_document))            
           
    s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter = "/")
    if 'Contents' in s3_result:
        
        for key in s3_result['Contents']:
            file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))
        print("Tables in the document: {}".format(len(file_list)))
        
        while s3_result['IsTruncated']:
            continuation_key = s3_result['NextContinuationToken']
            s3_result = s3_conn.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/", ContinuationToken=continuation_key)
            for key in s3_result['Contents']:
                file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))            
            print("List count = {len(file_list)}")
    print(file_list)   
    
    jsonresponse = {}
    jsonresponse['tables'] = []
    for s3file in file_list:
        if s3file.endswith("html"):
            file_handle = s3file[s3file.find(bucket)+len(bucket)+1:]
            s3_object = s3.Object(bucket,file_handle)
            s3_response = s3_object.get()
            xmlstring = s3_response['Body'].read()
            
            tablexml = ElementTree.fromstring(xmlstring)
            jsonresponse['tables'].append(textract_util.etree_to_dict(tablexml))

        if s3file.endswith("json"):
            file_handle = s3file[s3file.find(bucket)+len(bucket)+1:]
            s3_object = s3.Object(bucket,file_handle)
            s3_response = s3_object.get()
            jsonstring = s3_response['Body'].read()

            formjson = json.loads(jsonstring)
            jsonresponse['formfields'] = formjson

    if bucketAccessPolicyArn is not None:
        detachExternalBucketPolicy(bucketAccessPolicyArn, event)
        
    return jsonresponse

 