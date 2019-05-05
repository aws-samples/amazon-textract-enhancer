import io
import os
import json
import time
import boto3
from datetime import datetime
from io import BytesIO
from xml.dom import minidom
from xml.etree import ElementTree
from collections import defaultdict
from collections import OrderedDict 
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
        except Exception as e:
            exceptionType = str(type(e))
            if exceptionType.find("AccessDeniedException") > 0:
                finished = True
                print("You aren't authorized to perform textract.analyze_document action.")    
            elif exceptionType.find("InvalidJobIdException") > 0:
                finished = True
                print("An invalid job identifier was passed.")   
            elif exceptionType.find("InvalidParameterException") > 0:
                finished = True
                print("An input parameter violated a constraint.")        
            else:
                if retryCount < maxRetryAttempt:
                    retryCount = retryCount + 1
                else:
                    print(e)
                    print("Result retrieval failed, after {} retry, aborting".format(maxRetryAttempt))                       
                if exceptionType.find("InternalServerError") > 0:
                    print("Amazon Textract experienced a service issue. Trying in {} seconds.".format(retryInterval))   
                    time.sleep(retryInterval)
                elif exceptionType.find("ProvisionedThroughputExceededException") > 0:
                    print("The number of requests exceeded your throughput limit. Trying in {} seconds.".format(retryInterval*3))
                    time.sleep(retryInterval*3)
                elif exceptionType.find("ThrottlingException") > 0:
                    print("Amazon Textract is temporarily unable to process the request. Trying in {} seconds.".format(retryInterval*6))
                    time.sleep(retryInterval*6)
          

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

#Function to extract table information from the raw JSON returned by Textract
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

#Function to group all block elements from textract response by type
def groupBlocksByType(responseBlocks):
    blocks = {}

    for block in responseBlocks:
        blocktype = block['BlockType']
        if blocktype not in blocks.keys():
            blocks[blocktype] = [block]
        else:
            blocks[blocktype].append(block)
    print("Extracted Block Types:")
    for blocktype in blocks.keys():
        print("                       {} = {}".format(blocktype, len(blocks[blocktype])))
    return blocks

#Function to extract all key value pair blocks from textract response
def extractKeyValuePairs(blocks):

    keyValuePairs = blocks['KEY_VALUE_SET']
    formKeys = {}
    formValues = {}
    for pair in keyValuePairs:
                                        
        if pair['EntityTypes'][0] == 'KEY':
            
            if pair["Id"] not in formKeys.keys():
                formKeys[pair["Id"]] = {
                                            "BoundingBox": pair["Geometry"]["BoundingBox"],
                                            "Polygon": pair["Geometry"]["Polygon"]
                                        }
            else:
                formKeys[pair["Id"]]["BoundingBox"] = pair["Geometry"]["BoundingBox"]               
                formKeys[pair["Id"]]["Polygon"] = pair["Geometry"]["Polygon"]
                
            for relationShip in pair['Relationships']:
                if relationShip['Type'] == "CHILD":
                    if pair["Id"] not in formKeys.keys():
                        formKeys[pair["Id"]] = {"CHILD": relationShip["Ids"]}
                    else:
                        formKeys[pair["Id"]]["CHILD"] = relationShip["Ids"]
                elif relationShip['Type'] == "VALUE":
                    if pair["Id"] not in formKeys.keys():
                        formKeys[pair["Id"]] = {"VALUE": relationShip["Ids"][0]}
                    else:
                        formKeys[pair["Id"]]["VALUE"] = relationShip["Ids"][0]                    
        elif pair['EntityTypes'][0] == 'VALUE':
            
            if pair["Id"] not in formKeys.keys():
                formValues[pair["Id"]] = {
                                            "BoundingBox": pair["Geometry"]["BoundingBox"],
                                            "Polygon": pair["Geometry"]["Polygon"]
                                        }
            else:
                formValues[pair["Id"]]["BoundingBox"] = pair["Geometry"]["BoundingBox"]               
                formValues[pair["Id"]]["Polygon"] = pair["Geometry"]["Polygon"]
                
            if pair["Id"] not in formValues.keys():
                formValues[pair["Id"]] = {}
            if "Relationships" in pair.keys():
                for relationShip in pair['Relationships']:
                    if relationShip['Type'] == "CHILD":
                        if pair["Id"] not in formValues.keys():
                            formValues[pair["Id"]] = {"CHILD": relationShip["Ids"]}
                        else:
                            formValues[pair["Id"]]["CHILD"] = relationShip["Ids"]                    

    return formKeys, formValues

#Function to extract all words from textract response
def extractWords(blocks):
    wordBlocks = blocks['WORD']
    pageWords = {}
    for wordBlock in wordBlocks:   
        
        if wordBlock["Id"] not in pageWords.keys():
            pageWords[wordBlock["Id"]] = {
                                            "Text": wordBlock["Text"], 
                                            "BoundingBox": wordBlock["Geometry"]["BoundingBox"],
                                            "Polygon": wordBlock["Geometry"]["Polygon"]
                                        }
        else:
            pageWords[wordBlock["Id"]]["Text"] = wordBlock["Text"]        
            pageWords[wordBlock["Id"]]["BoundingBox"] = wordBlock["Geometry"]["BoundingBox"]
            pageWords[wordBlock["Id"]]["Polygon"] = wordBlock["Geometry"]["Polygon"]
    return pageWords

#Function to create a dictionary JSON containing the key value pairs as identified by parsing the textract response
def generateFormEntries(formKeys, formValues, pageWords):
    
    formEntries = {}
    count = 0
    for formKey in formKeys.keys():        
            
        keyText = ""
        if "CHILD" in formKeys[formKey].keys():
            keyTextKeys = formKeys[formKey]['CHILD']
            for textKey in keyTextKeys:
                keyText = keyText + " " + pageWords[textKey]["Text"]
        key = formKeys[formKey]['VALUE']

        valueText = ""
        if "CHILD" in formValues[key].keys():
            valueTextKeys = formValues[key]["CHILD"]
            for textKey in valueTextKeys:
                if textKey in pageWords.keys():
                    valueText = valueText + " " + pageWords[textKey]["Text"]

        if keyText != "":
            count = count + 1
            if keyText not in formEntries.keys(): 
                formEntries[keyText] = [valueText]
            else:
                formEntries[keyText].append(valueText)
    return OrderedDict(sorted(formEntries.items()))

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

def sync_call_handler(event, context): 
        
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
        blocks = groupBlocksByType(documentBlocks)
        formKeys, formValues = extractKeyValuePairs(blocks)
        pageWords = extractWords(blocks)

        #Generate JSON document using form fields information  
        formEntries = generateFormEntries(formKeys, formValues, pageWords)   

        json_document = "{}.json".format(document_name)
        json_file = open("/tmp/"+json_document,'w+')
        json_file.write(json.dumps(formEntries, indent=4, sort_keys=True))
        json_file.close()
        s3.meta.client.upload_file("/tmp/"+json_document, bucket, "{}/{}".format(upload_prefix,json_document)) 

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
    
    jsonresponse = {}
    jsonresponse['tables'] = []
    for s3file in file_list:
        if s3file.endswith("html"):
            file_handle = s3file[s3file.find(bucket)+len(bucket)+1:]
            s3_object = s3.Object(bucket,file_handle)
            s3_response = s3_object.get()
            xmlstring = s3_response['Body'].read()
            
            tablexml = ElementTree.fromstring(xmlstring)
            jsonresponse['tables'].append(etree_to_dict(tablexml))

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

def async_submit_handler(event, context):
    
    print(event)
    
    #Initialize Boto Resource	
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.client('dynamodb')
    table_name=os.environ['table_name']
    tokenPrefix = os.environ['token_prefix']  

    roleArn = os.environ['role_arn']
    topicArn = os.environ['topic_arn'] 

    retryCount = 0
    retryInterval = int(os.environ['retry_interval']) #30
    maxRetryAttempt = int(os.environ['max_retry_attempt']) #5
    
    external_bucket = ""
    bucket = ""
    document = ""
    jobId = ""
    jobStartTimeStamp = 0
    jobCompleteTimeStamp = 0
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
    document_name = document[document.rfind("/")+1:document.rfind(".")] if document.find("/") >= 0 else document[:document.rfind(".")]
    document_type = document[document.rfind(".")+1:].upper()
        
    #Submit Document Anlysis job to Textract to extract text features    
    while retryCount >= 0 and retryCount < maxRetryAttempt:
        try:
            response = textract.start_document_analysis(
                                    ClientRequestToken = "{}-{}".format(tokenPrefix, document[document.rfind("/")+1:document.rfind(".")]),
                                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': document}},
                                    FeatureTypes=["TABLES", "FORMS"],
                                    NotificationChannel={'SNSTopicArn': topicArn,'RoleArn': roleArn},
                                    JobTag = "AnalyzeText-{}".format(document[document.rfind("/")+1:document.rfind(".")]))
            jobId = response['JobId']
            jobStartTimeStamp = datetime.strptime(response['ResponseMetadata']['HTTPHeaders']['date'], '%a, %d %b %Y %H:%M:%S %Z').timestamp()
            print("Textract Request: {} submitted at {} with JobId - {}".format(
                response['ResponseMetadata']['RequestId'], jobStartTimeStamp,jobId))    
            
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


    jsonresponse = {
        'JobId': jobId,
        'DocumentBucket': bucket,
        'DocumentKey': document,
        'UploadPrefix': upload_prefix,        
        'DocumentName':document_name,
        'DocumentType':document_type,
        'JobStartTimeStamp': str(jobStartTimeStamp)
    }
    
    recordExists = False
    
    try:
        response = dynamodb.scan(
            TableName=table_name,
            ExpressionAttributeNames={'#ID': 'JobId'},
            ExpressionAttributeValues={':jobId' : {'S': jobId}},
            FilterExpression='#ID = :jobId'
        )      
        if response['Count'] > 0:
            recordExists = True
            jsonresponse['JobStartTimeStamp'] = int(response['Items'][0]['JobStartTimeStamp']['N'])
            jsonresponse['JobCompleteTimeStamp'] = int(response['Items'][0]['JobCompleteTimeStamp']['N'])
            
    except Exception as e:
        print('DynamoDB Read Error is: {0}'.format(e))  
    
    if not recordExists:
        try:
            response = dynamodb.update_item(
                TableName=table_name,
                Key={
                    'JobId':{'S':jobId},
                },
                AttributeUpdates={
                    'DocumentBucket':{'Value': {'S':bucket}},
                    'DocumentKey':{'Value': {'S':document}},
                    'UploadPrefix':{'Value': {'S':upload_prefix}},
                    'DocumentName':{'Value': {'S':document_name}},
                    'DocumentType':{'Value': {'S':document_type}},
                    'JobStartTimeStamp':{'Value': {'N':str(jobStartTimeStamp)}},
                    'JobCompleteTimeStamp':{'Value': {'N':'0'}},
                    'TableFiles':{'Value': {'L':[]}},
                    'FormFiles':{'Value': {'L':[]}}
                }
            )
        except Exception as e:
            print('DynamoDB Insertion Error is: {0}'.format(e))

    if bucketAccessPolicyArn is not None:
        detachExternalBucketPolicy(bucketAccessPolicyArn, event)
        
    return jsonresponse

def postprocess_table_handler(event, context):
    
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
            num_tables = -1      
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
                print("{} = {}".format("Timestamp", sns['Timestamp']))                
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

                    documentBlocks = GetDocumentAnalysisResult(textract, textractJobId) 

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
                        try:
                            response = dynamodb.update_item(
                                TableName=table_name,
                                Key={
                                    'JobId':{'S':textractJobId}
                                },
                                ExpressionAttributeNames={"#tf": "TableFiles", "#jct": "JobCompleteTimeStamp"},
                                UpdateExpression='SET #tf = list_append(#tf, :table_files), #jct = :job_complete',
                                ExpressionAttributeValues={
                                    ":table_files": {"L": [{"S": "{}/{}".format(upload_prefix,html_document)}]},
                                    ":job_complete": {"N": str(textractTimestamp)}
                                }
                            )
                        except Exception as e:
                            print('DynamoDB Insertion Error is: {0}'.format(e))
           
            s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter = "/")
            if 'Contents' in s3_result:
                
                for key in s3_result['Contents']:
                    file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))
                
                while s3_result['IsTruncated']:
                    continuation_key = s3_result['NextContinuationToken']
                    s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter="/", ContinuationToken=continuation_key)
                    for key in s3_result['Contents']:
                        file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))            
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
            jsonresponse['tables'].append(etree_to_dict(tablexml))
        
    return jsonresponse

def postprocess_form_handler(event, context):
    
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
            num_tables = -1      
            bucket = ""
            upload_prefix = ""            
            textractJobId = ""
            textractStatus = ""
            textractAPI = ""
            textractJobTag = ""
            textractS3ObjectName = ""
            textractS3Bucket = ""  
            textractTimestamp = ""            
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

                    documentBlocks = GetDocumentAnalysisResult(textract, textractJobId) 

            if documentBlocks is not None:
                print("{} Blocks retrieved".format(len(documentBlocks)))
        
                #Extract form fields into a Python dictionary by parsing the raw JSON from Textract
                blocks = groupBlocksByType(documentBlocks)
                formKeys, formValues = extractKeyValuePairs(blocks)
                pageWords = extractWords(blocks)
        
                #Generate JSON document using form fields information  
                formEntries = generateFormEntries(formKeys, formValues, pageWords)   
        
                json_document = "{}.json".format(document_name)
                json_file = open("/tmp/"+json_document,'w+')
                json_file.write(json.dumps(formEntries, indent=4, sort_keys=True))
                json_file.close()
                s3.meta.client.upload_file("/tmp/"+json_document, bucket, "{}/{}".format(upload_prefix,json_document))         

                try:
                    response = dynamodb.update_item(
                        TableName=table_name,
                        Key={
                            'JobId':{'S':textractJobId}
                        },
                        ExpressionAttributeNames={"#ff": "FormFiles", "#jct": "JobCompleteTimeStamp"},
                        UpdateExpression='SET #ff = list_append(#ff, :form_files), #jct = :job_complete',
                        ExpressionAttributeValues={
                            ":form_files": {"L": [{"S": "{}/{}".format(upload_prefix,json_document)}]},
                            ":job_complete": {"N": str(textractTimestamp)}
                        }
                    )
                except Exception as e:
                    print('DynamoDB Insertion Error is: {0}'.format(e))                            
                            
           
            s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter = "/")
            if 'Contents' in s3_result:
                
                for key in s3_result['Contents']:
                    file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))
                
                while s3_result['IsTruncated']:
                    continuation_key = s3_result['NextContinuationToken']
                    s3_result = s3.meta.client.list_objects_v2(Bucket=bucket, Prefix="{}/".format(upload_prefix), Delimiter="/", ContinuationToken=continuation_key)
                    for key in s3_result['Contents']:
                        file_list.append("https://s3.amazonaws.com/{}/{}".format(bucket, key['Key']))            
        
            print(file_list)   
    
    jsonresponse = {}
    for s3file in file_list:
        if s3file.endswith("json"):
            file_handle = s3file[s3file.find(bucket)+len(bucket)+1:]
            s3_object = s3.Object(bucket,file_handle)
            s3_response = s3_object.get()
            jsonstring = s3_response['Body'].read()

            formjson = json.loads(jsonstring)
            jsonresponse['formfields'] = formjson
        
    return jsonresponse