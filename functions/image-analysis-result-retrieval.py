import os
import boto3
import json
from xml.etree import ElementTree
from collections import defaultdict

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

def lambda_handler(event, context):    
    s3 = boto3.resource('s3')
    textract = boto3.client('textract')
    dynamodb = boto3.resource('dynamodb')
    table_name=os.environ['table_name']
    table = dynamodb.Table(table_name)    
   
    documentBucket = event['DocumentBucket']
    documentKey = event['DocumentKey']
    resultType = "ALL"
    if 'ResultType' in event:
        resultType = event['ResultType'].upper()
    print("Invoking retrieval function for result type {}".format(resultType))
    jsonresponse = {}
    if resultType != "ALL" and resultType != "TABLE" and resultType != "FORM":
        jsonresponse["Error"] = "Invalid Result Type {}".format(resultType)
        return jsonresponse

    item = None
    jobStartTimeStamp = None
    jobCompleteTimeStamp = None  

    try:
        response = table.scan(
            FilterExpression = "DocumentBucket = :bucket and DocumentKey = :key",
            ExpressionAttributeValues = {
                ":bucket": documentBucket,
                ":key": documentKey
            }
        )
        print(len(response['Items']))
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
        jsonresponse['NumTables'] = str(item['NumTables'])
        jsonresponse['NumFields'] = str(item['NumFields'])                
    
    if resultType == "FORM" or resultType == "ALL":
        formFiles = item['FormFiles']
        print("Form Fields stored in {} files".format(len(formFiles)))
        for formFile in formFiles:
            s3_object = s3.Object(documentBucket,formFile)
            print("Reading Form fields from {}".format(formFile))
            s3_response = s3_object.get()
            jsonstring = s3_response['Body'].read()

            formjson = json.loads(jsonstring)
            jsonresponse['formfields'] = formjson    

    if resultType == "TABLE" or resultType == "ALL":
        tableFiles = item['TableFiles']
        jsonresponse['tables'] = []     
        print("Table data stored in {} files".format(len(tableFiles)))
        for tableFile in tableFiles:
            s3_object = s3.Object(documentBucket,tableFile)
            print("Reading Form fields from {}".format(tableFile))
            s3_response = s3_object.get()
            xmlstring = s3_response['Body'].read()

            tablexml = ElementTree.fromstring(xmlstring)
            jsonresponse['tables'].append(etree_to_dict(tablexml))

    return jsonresponse