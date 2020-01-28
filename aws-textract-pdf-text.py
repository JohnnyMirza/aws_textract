import boto3
import time

def main():
    # Document - (Future - replace with s3 bucket fetch)
    s3BucketName = "XXXXXX"
    documentName = "XXXXXX"

    #1 - Start Text Detection Request
    jobId = startJob(s3BucketName, documentName)
    print("Started job with id: {}".format(jobId))
    print ("-------------------------------")
    if(isJobComplete(jobId)):  
        response = getJobResults(jobId)

    #2 Check if job (NextToken) is complete and append to pages
    if(isJobComplete(jobId)):  
        pages = getJobResults(jobId)
    
    #3 Get Text from multipage document and write to file
    output_text = documentName.replace(".pdf", ".txt")
    with open(output_text, "w+") as fout:
        fout.write('')
    for response in pages:
        to_text=getDetetectedText(pages)
        with open(output_text, "a") as fout:
             fout.write(to_text)
        # show the results
    print('TXT OUTPUT FILE: ', output_text)

def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        }
    })

    return response["JobId"]

def isJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def getDetetectedText(pages):
    lines=''
    for resultPage in pages:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                lines+=item["Text"] + ' '
    return lines

if __name__ == "__main__":
    main()    