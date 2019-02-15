import io
import os
import json
import time
import boto3
from io import BytesIO
from xml.dom import minidom
from xml.etree import ElementTree
from collections import defaultdict
from xml.etree.ElementTree import Element, SubElement, Comment, tostring

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

#Function to extarct table information from the raw JSON returned by Textract
def extractTableBlocks(json):
    blocks = {}
    for block in json:
        
        blocks[block['Id']] = {}
        blocks[block['Id']]['Type'] = block['BlockType']
        blocks[block['Id']]['BoundingBox'] = block['Geometry']['BoundingBox']
        blocks[block['Id']]['Polygon'] = block['Geometry']['Polygon']
        
        if block['BlockType'] == "PAGE": 
            if 'Page' in block.keys():
                blocks[block['Id']]['Page'] = block['Page']
            else:
                blocks[block['Id']]['Page'] = 1
            blocks[block['Id']]['Items'] = {}
            if 'Relationships' in block.keys():
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for rid in relationship['Ids']:
                            blocks[block['Id']]['Items'][rid] = {}  
                            
        if 'Text' in block.keys():
            blocks[block['Id']]['Text'] = block['Text']
            blocks[block['Id']]['Confidence'] = block['Confidence']
            
        if block['BlockType'] == "TABLE": 
            
            for key in blocks.keys():
                if blocks[key]['Type'] == 'PAGE' and block['Id'] in blocks[key]['Items'].keys():
                    blocks[block['Id']]['ContainingPage'] = blocks[key]['Page']
                    break
            
            blocks[block['Id']]['Cells'] = {}
            blocks[block['Id']]['Grid'] = []
            blocks[block['Id']]['NumRows'] = 0
            blocks[block['Id']]['NumColumns'] = 0
            if 'Relationships' in block.keys():
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        for rid in relationship['Ids']:
                            blocks[block['Id']]['Cells'][rid] = {}  
                            
        if block['BlockType'] == "CELL":
            blocks[block['Id']]['RowIndex'] = block['RowIndex']
            blocks[block['Id']]['ColumnIndex'] = block['ColumnIndex']
            blocks[block['Id']]['RowSpan'] = block['RowSpan']
            blocks[block['Id']]['ColumnSpan'] = block['ColumnSpan']

            for key in blocks.keys():
                if blocks[key]['Type'] == 'TABLE' and block['Id'] in blocks[key]['Cells'].keys():
                    tableblock = blocks[key]
                    grid = tableblock['Grid']
                    childblock = tableblock['Cells'][block['Id']]
                    childblock['Type'] = "CELL"
                    
                    childblock['RowIndex'] = block['RowIndex']
                    if childblock['RowIndex'] > tableblock['NumRows']:
                        tableblock['NumRows'] = childblock['RowIndex']
                    while len(grid) < tableblock['NumRows']:
                        grid.append([]) 
                        
                    childblock['ColumnIndex'] = block['ColumnIndex']
                    if childblock['ColumnIndex'] > tableblock['NumColumns']:
                        tableblock['NumColumns'] = childblock['ColumnIndex']
                    while len(grid[tableblock['NumRows']-1]) < tableblock['NumColumns']:
                        grid[tableblock['NumRows']-1].append(None)   
                        
                    childblock['RowSpan'] = block['RowSpan']
                    childblock['ColumnSpan'] = block['ColumnSpan']
                    childblock['Confidence'] = block['Confidence']
                    childblock['BoundingBox'] = block['Geometry']['BoundingBox']
                    childblock['Polygon'] = block['Geometry']['Polygon']
                    childblock['WORD'] = []
                    if 'Relationships' in block.keys():
                        for relationship in block['Relationships']:                            
                            if relationship['Type'] == 'CHILD':
                                for rid in relationship['Ids']:
                                    if rid in blocks.keys() and blocks[rid]['Type'] == "WORD":
                                        word = {}
                                        word['Text'] = blocks[rid]['Text']
                                        word['BoundingBox'] = blocks[rid]['BoundingBox']
                                        childblock['WORD'].append(word)
                    gridtext = []
                    for word in childblock['WORD']:
                        gridtext.append(word['Text'])
                    grid[childblock['RowIndex'] - 1][childblock['ColumnIndex'] - 1] = ' '.join(gridtext)
                    break
                    
    for key in list(blocks.keys()):
        if blocks[key]['Type'] != "TABLE":
            blocks.pop(key, None)    
        
    return blocks
    
#Function to genrate table structure in XML, that can be rendered as HTML table
def generateTableXML(tabledict):
    tables = []
    for tkey in tabledict.keys():
        containingPage = tabledict[tkey]['ContainingPage']
        table = Element('table')
        table.set('Id', tkey)
        table.set('ContainingPage', str(containingPage))
        table.set('border', "1")
        NumRows = tabledict[tkey]['NumRows']
        NumColumns = tabledict[tkey]['NumColumns']
        Grid = tabledict[tkey]['Grid']
        for i in range(NumRows):
            row = SubElement(table, 'tr')
            for j in range(NumColumns):
                col = SubElement(row, 'td')
                col.text = Grid[i][j]
        while len(tables) < containingPage:
            tables.append([])
        table.set('TableNumber', str(len(tables[containingPage - 1]) + 1))
        tables[containingPage - 1].append(table)
    return tables

#Convert XML Tables to JSON    
def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v
                     for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v)
                        for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d
    
#Function to prettify XML    
def prettify(elem):
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")    

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
                                            "s3:ListBucketVersions",\
                                            "s3:GetObject",\
                                            "s3:GetObjectVersion"\
                                        ],\
                                        "Resource": [\
                                            "arn:aws:s3:::'+externalBucketName+'",\
                                            "arn:aws:s3:::'+externalBucketName+'/*"\
                                        ]\
                                    },\
                                    {\
                                        "Effect": "Allow",\
                                        "Action": [\
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

    queueUrl = os.environ['queue_url'] 
    roleArn = os.environ['role_arn']
    topicArn = os.environ['topic_arn'] 

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
                return file_list
        else:
            retryCount = -1   

    if document_path == "":
        upload_prefix = jobId
    else:
        upload_prefix = "{}/{}".format(document_path, jobId)

    print("upload_prefix = " + upload_prefix)
    #Poll queue to retrieve the Job result
    jobFound = False
    retryCount = 0
    documentBlocks = None

    print("Retrieving result for Job Id: {}".format(jobId))

    while jobFound == False:
        if retryCount >= maxRetryAttempt:
            print("No results found")
            break
        sqsResponse = sqs.receive_message(QueueUrl=queueUrl, MessageAttributeNames=['ALL'], MaxNumberOfMessages=10)

        if sqsResponse:

            if 'Messages' not in sqsResponse:
                retryCount = retryCount + 1
                print("Retry - {}".format(retryCount))  
                continue

            for message in sqsResponse['Messages']:
                notification = json.loads(message['Body'])
                textMessage = json.loads(notification['Message'])
                print("JobId = {} ({})".format(textMessage['JobId'], textMessage['Status']))
                print("JobTag = {}".format(textMessage['JobTag']))
                print("DocumentLocation = {}/{}".format(
                    textMessage['DocumentLocation']['S3Bucket'],
                    textMessage['DocumentLocation']['S3ObjectName']))
                
                if str(textMessage['JobId']) == jobId:
                    print("Matching Job Found:{}".format(textMessage['JobId']))
                    jobFound = True
                    #=============================================
                    documentBlocks = GetDocumentAnalysisResult(textract, textMessage['JobId'])                        
                    #=============================================

                    if documentBlocks is not None and len(documentBlocks) > 0:
                        sqs.delete_message(QueueUrl=queueUrl,
                                    ReceiptHandle=message['ReceiptHandle'])
                else:
                    print("Job didn't match: {}:{}".format(textMessage['JobId'], jobId))
                    
                    documentBlocks = []

                sqs.delete_message(QueueUrl=queueUrl,
                            ReceiptHandle=message['ReceiptHandle'])
                
    if documentBlocks is not None:
        print("{} Blocks retrieved".format(len(documentBlocks)))

        #Extract table information  into a Python dictionary by parsing the raw JSON from Textract
        tabledict = extractTableBlocks(documentBlocks)
        
        #Generate XML document using table information    
        tables = generateTableXML(tabledict)        
        num_tables = len(tables)

        for page in tables:
            for table in page:
                html_document = "{}-page-{}-table-{}.html".format(document_name, table.attrib['ContainingPage'], table.attrib['TableNumber'])
                html_file = open("/tmp/"+html_document,'w+')
                html_file.write(prettify(table))
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
    
    jsontables = {}
    jsontables['tables'] = []
    for s3file in file_list:
        file_handle = s3file[s3file.find(bucket)+len(bucket)+1:]
        s3_object = s3.Object(bucket,file_handle)
        s3_response = s3_object.get()
        xmlstring = s3_response['Body'].read()
        
        tablexml = ElementTree.fromstring(xmlstring)
        jsontables['tables'].append(etree_to_dict(tablexml))
    
    if bucketAccessPolicyArn is not None:
        detachExternalBucketPolicy(bucketAccessPolicyArn, event)
        
    return jsontables
