import os
import time
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
        blocks=[]
        if 'Blocks' in response:
            blocks=response['Blocks']
            print ('Retrieved {} Blocks from Textract Document Analysis response'.format(len(blocks)))
        else:
            print("No blocks found in Textract Document Analysis response, could be a result of unreadable document.")
            finished = True


        # Display block information
        for block in blocks:
            result.append(block)
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                paginationToken = None
                finished = True  
    
    if 'DocumentMetadata' not in response:
        return 0, result    
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

    formKeys = {}
    formValues = {}
    
    if 'KEY_VALUE_SET' in blocks:
        keyValuePairs = blocks['KEY_VALUE_SET']

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
    
    pageWords = {}
    if 'WORD' in blocks:
        wordBlocks = blocks['WORD']
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

#Function to retrieve result of completed analysis job
def GetTextDetectionResult(textract, jobId):
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
                response = textract.get_document_text_detection(JobId=jobId,
                                            MaxResults=maxResults)  
            else:
                response = textract.get_document_text_detection(JobId=jobId,
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

        #Get the text blocks
        blocks=[]
        if 'Blocks' in response:
            blocks=response['Blocks']
            print ('Retrieved {} Blocks from Textract Text Detection response'.format(len(blocks)))      
        else:
            print("No blocks found in Textract Text Detection response, could be a result of unreadable document.")
            finished = True           

        # Display block information
        for block in blocks:
            result.append(block)
            if 'NextToken' in response:
                paginationToken = response['NextToken']
            else:
                paginationToken = None
                finished = True  
    
    if 'DocumentMetadata' not in response:
        return 0, result      
    return response['DocumentMetadata']['Pages'], result

#Function to extract lines of text from all pages from textract response
def extractTextBody(blocks):
    total_line = 0
    document_text = {}
    for page in blocks['PAGE']:
        document_text['Page-{0:02d}'.format(page['Page'])] = {}
        print("Page-{} contains {} Lines".format(page['Page'], len(page['Relationships'][0]['Ids'])))
        total_line += len(page['Relationships'][0]['Ids'])
        for i, line_id in enumerate(page['Relationships'][0]['Ids']):
            page_line = None
            for line in blocks['LINE']:
                if line['Id'] == line_id:
                    page_line = line
                    break
            document_text['Page-{0:02d}'.format(page['Page'])]['Line-{0:04d}'.format(i+1)] = {}
            document_text['Page-{0:02d}'.format(page['Page'])]['Line-{0:04d}'.format(i+1)]['Text'] = page_line['Text']
    print(total_line)
    return document_text, total_line
