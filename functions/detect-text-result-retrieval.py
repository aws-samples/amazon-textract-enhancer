import io
import os
import json
import time
import boto3
from datetime import datetime
from xml.etree import ElementTree
from textract_util import *

def lambda_handler(event, context):    
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.resource('dynamodb')
    table_name=os.environ['table_name']
    table = dynamodb.Table(table_name)    
   
    documentBucket = event['DocumentBucket']
    documentKey = event['DocumentKey']

    print("Invoking retrieval function for text detection result")

    jsonresponse = {}
 
    item = None
    jobStartTimeStamp = None
    jobCompleteTimeStamp = None  

    try:
        response = table.scan(
            FilterExpression = "DocumentBucket = :bucket and DocumentKey = :key and JobType =:jobType",
            ExpressionAttributeValues = {
                ":bucket": documentBucket,
                ":key": documentKey,
                ":jobType": 'TextDetection'

            }
        )
        recordsMatched = len(response['Items'])
        print("{} matching records found for {}/{}".format(recordsMatched, documentBucket, documentKey))
        if recordsMatched > 0:
            item = response['Items'][-1]
    except Exception as e:
        print('Actual error is: {0}'.format(e))

    if item is not None:
        jsonresponse['JobId'] = item['JobId']
        jobStartTimeStamp = item['JobStartTimeStamp']
        jsonresponse['JobStartTimeStamp'] = str(jobStartTimeStamp)
        jobCompleteTimeStamp = item['JobCompleteTimeStamp']
        jsonresponse['JobCompleteTimeStamp'] = str(jobCompleteTimeStamp)
        if jobCompleteTimeStamp <= jobStartTimeStamp:
            jsonresponse['JobStatus'] = "IN PROGRESS"
        else:
            jsonresponse['JobStatus'] = "COMPLETED"
        documentBucket = item['DocumentBucket']
        jsonresponse['DocumentBucket'] = documentBucket
        documentKey = item['DocumentKey']
        jsonresponse['DocumentKey'] = documentKey
        jsonresponse['DocumentName'] = item['DocumentName']
        jsonresponse['DocumentType'] = item['DocumentType']
        jsonresponse['UploadPrefix'] = item['UploadPrefix']
        jsonresponse['NumPages'] = str(item['NumPages'])
        jsonresponse['NumLines'] = str(item['NumLines'])            
    
        textFiles = item['TextFiles']
        print("Document Text stored in {} files".format(len(textFiles)))
        for textFile in textFiles:
            s3_object = s3.Object(documentBucket,textFile)
            print("Reading Document text from {}".format(textFile))
            s3_response = s3_object.get()
            jsonstring = s3_response['Body'].read()

            documentjson = json.loads(jsonstring)

            jsonresponse = {}
            for page in documentjson.keys():
                jsonresponse[page] = []
                for line in documentjson[page].keys():
                    jsonresponse[page].append(documentjson[page][line]['Text'])


    return jsonresponse