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
    retryCount = 0
    result = []

    while finished == False:

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
                    print("The number of requests exceeded your throughput limit. Trying in {} seconds.".format(retryInterval*6))
                    time.sleep(retryInterval*3)
                elif exceptionType.find("ThrottlingException") > 0:
                    print("Amazon Textract is temporarily unable to process the request. Trying in {} seconds.".format(retryInterval*3))
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
