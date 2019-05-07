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
    
    return response['DocumentMetadata']['Pages'], result

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
    num_tables = len(tabledict.keys())
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
    return num_tables, tables
    
#Function to prettify XML    
def prettify(elem):
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")    

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
            print(record)
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

                    num_pages, documentBlocks = GetDocumentAnalysisResult(textract, textractJobId) 

            if documentBlocks is not None:
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
                                    'JobId':{'S':textractJobId}
                                },
                                ExpressionAttributeNames={"#tf": "TableFiles", "#jct": "JobCompleteTimeStamp", "#nt": "NumTables", "#np": "NumPages"},
                                UpdateExpression='SET #tf = list_append(#tf, :table_files), #jct = :job_complete, #nt = :num_tables, #np = :num_pages',
                                ExpressionAttributeValues={
                                    ":table_files": {"L": [{"S": "{}/{}".format(upload_prefix,html_document)}]},
                                    ":job_complete": {"N": str(textractTimestamp)},
                                    ":num_tables": {"N": str(num_tables)},
                                    ":num_pages": {"N": str(num_pages)}
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