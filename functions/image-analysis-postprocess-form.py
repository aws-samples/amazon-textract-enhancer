import io
import os
import json
import time
import boto3
from io import BytesIO
from xml.dom import minidom
from xml.etree import ElementTree
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

def lambda_handler(event, context):
    
    #Initialize Boto Resource	
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    documentBlocks = None
    bucket = ""
    upload_prefix = ""
    num_tables = -1
    file_list = []

    if "Records" in event:        
        records = event['Records']
        numRecords = len(records)
        textractJobId = ""
        textractStatus = ""
        textractAPI = ""
        textractJobTag = ""
        textractS3ObjectName = ""
        textractS3Bucket = ""
        print("{} messages recieved".format(numRecords))
        for record in records:
            if 'Sns' in record.keys():
                sns = record['Sns']
                textractTimestamp =  sns['Timestamp']                  
                print("{} = {}".format("Timestamp", sns['Timestamp']))                
                if 'Message' in sns.keys():
                    message = json.loads(sns['Message'])
                    textractJobId = message['JobId']
                    print("{} = {}".format("JobId", textractJobId))
                    textractStatus = message['Status']
                    print("{} = {}".format("Status",textractStatus))        
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
                    document_name = textractS3ObjectName[textractS3ObjectName.rfind("/")+1:textractS3ObjectName.rfind(".")]     

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
