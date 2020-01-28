import boto3
import time

def main():
    # Document - (Future - replace with s3 bucket fetch)
    s3BucketName = "XXXXXX"
    documentName = "XXXXXX"
    
    #1 - Start Analyze Request
    jobId = startJob(s3BucketName, documentName)
    print(f'Started job with id: {jobId}')
    print ("-------------------------------")

    #2 Check if job (NextToken) is complete and append to pages
    if(isJobComplete(jobId)):  
        pages = getJobResults(jobId)
    
    #3 Table Reponse from getJobResults to csv
    output_csv = documentName.replace(".pdf", ".csv")
    with open(output_csv, "w+") as fout:
        fout.write('')
    for response in pages:
        table_csv = get_table_csv_results(response)
        # replace content
        with open(output_csv, "a") as fout:
            fout.write(table_csv)
        # show the results
    print('CSV OUTPUT FILE: ', output_csv)

    #4 Write all non tables from getJobResults to Lines
    output_lines = documentName.replace(".pdf", ".txt")
    with open(output_lines, "w+") as fout:
        fout.write('')
    for response in pages:
        to_lines = get_detected_lines(pages)
        # replace content
        with open(output_lines, "a") as fout:
            fout.write(to_lines)
        # show the results
    print('TXT OUTPUT FILE: ', output_lines)

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    },
    FeatureTypes=["FORMS","TABLES"])

    return response['JobId']

def isJobComplete(jobId):
    time.sleep(2)
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(2)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):
    pages = []
    time.sleep(2)
    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
 
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(1)
        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)
        pages.append(response) 
        print("Resultset page recieved with token: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    print(f'Finished job with {jobId}')

    return pages

def get_table_csv_results(response):
    # Get the text blocks
    blocks=response['Blocks']

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        return "<b> NO Table FOUND </b>"

    csv = ''
    for index, table in enumerate(table_blocks):
        csv += generate_table_csv(table, blocks_map, index +1)
        csv += '\n\n'

    return csv

def generate_table_csv(table_result, blocks_map, table_index):
    rows = get_rows_columns_map(table_result, blocks_map)

    table_id = 'Table_' + str(table_index)
    
    # get cells.
    csv = 'Table: {0}\n\n'.format(table_id)

    for row_index, cols in rows.items():
        
        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'
        
    csv += '\n\n\n'
    return csv

def get_rows_columns_map(table_result, blocks_map):

    rows = {}
    for relationship in table_result['Relationships']:
        if relationship['Type'] == 'CHILD':
            for child_id in relationship['Ids']:
                cell = blocks_map[child_id]
                if cell['BlockType'] == 'CELL':
                    row_index = cell['RowIndex']
                    col_index = cell['ColumnIndex']
                    if row_index not in rows:
                        # create new row
                        rows[row_index] = {}
                        
                    # get the text value
                    rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '    
    return text

def get_detected_lines(pages):
    lines=''
    for resultPage in pages:
        
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                lines+=item["Text"] + ' '
    return lines
    
if __name__ == "__main__":
    main()    