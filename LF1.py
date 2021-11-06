import json
import boto3
#from botocore.vendored import requests
import requests
#from botocore.vendored import requests
import urllib3
import time
from requests_aws4auth import AWS4Auth

credentials = boto3.Session().get_credentials()
region = 'us-west-2'
http = urllib3.PoolManager()
service = 'es'
#awsauth = AWS4Auth("", "", region, "es", session_token=None)
awsauth = AWS4Auth("AKIAXBG5QJ2HV6ENLQPG", "o0T3P23hzBBSv9n2XzVEGVnUdU4OHoVQWOeTsO4D",
                       region, service, session_token=None)

def detect_labels(bucket, photo):

    client=boto3.client('rekognition')

    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},MaxLabels=10)

    print('Detected labels for ' + photo) 
    print()   
    for label in response['Labels']:
        print ("Label: " + label['Name'])
        print ("Confidence: " + str(label['Confidence']))
        print ("Instances:")
        for instance in label['Instances']:
            print ("  Bounding box")
            print ("    Top: " + str(instance['BoundingBox']['Top']))
            print ("    Left: " + str(instance['BoundingBox']['Left']))
            print ("    Width: " +  str(instance['BoundingBox']['Width']))
            print ("    Height: " +  str(instance['BoundingBox']['Height']))
            print ("  Confidence: " + str(instance['Confidence']))
            print()

        print ("Parents:")
        for parent in label['Parents']:
            print ("   " + parent['Name'])
        print ("----------")
        print ()
    return len(response['Labels'])

def lambda_handler(event, context):
    # TODO implement
    #print(json.dumps(event, indent=4, sort_keys=True))
    #bucket_name = 'photoboy'
    #key_name = 'download.jpg'
    print("LOG: In function")
    s3_info = event['Records'][0]['s3']
    print("LOG: ", str(s3_info))
    bucket_name = s3_info['bucket']['name']
    print("LOG: bucket: ", str(bucket_name))
    key_name = s3_info['object']['key']
    print("LOG: key: ", str(key_name))
    #print(bucket_name)
    client = boto3.client('rekognition')
    print("LOG: client: ", str(client))
    
    #bucket = '2021awsassignment2'
    #key = '3.jpg'
    
    #label_count = detect_labels(bucket, key)
    #print("LOG: count: ", label_count)
    pass_object = {'S3Object':{'Bucket':bucket_name,'Name':key_name}}
    print("LOG: pass_objectL ", str(pass_object))
    resp = client.detect_labels(Image={'S3Object':{'Bucket':bucket_name,'Name':key_name}},MaxLabels=10)
    resp = client.detect_labels(Image=pass_object)
    print("LOG: resp: ", str(resp))
    #print('<---------Now response object---------->')
    #print(json.dumps(resp, indent=4, sort_keys=True))
    timestamp =time.time()
    #timestamp = event['Records'][0]['eventTime']
    #timestamp = timestamp[:-5]
    labels = []
    #temp = resp['Labels'][0]['Name']
    for i in range(len(resp['Labels'])):
        labels.append(resp['Labels'][i]['Name'])
    print('<------------Now label list----------------->')
    print("LOG: ", labels)
    #print('<------------Now required json-------------->')
    body = {"objectKey":key_name,'bucket':bucket_name,'createdTimestamp':timestamp,'labels':labels}
    format = {'objectKey':key_name,'bucket':bucket_name,'createdTimestamp':timestamp,'labels':labels}
    #required_json = json.dumps(format)
    #print(required_json)
    url = "https://search-photos-ebkwwzrdqfndyfzzo4kpmdcb5y.us-west-2.es.amazonaws.com/photos/_doc"

    headers = {"Content-Type": "application/json"}
    #url2 = "https://vpc-photos-b4al4b3cnk5jcfbvlrgxxu3vhu.us-east-1.es.amazonaws.com/photos/_search?pretty=true&q=*:*"
    #r = requests.post(url, data=json.dumps(format).encode("utf-8"), headers=headers)
    #response = requests.post(url, json=body, auth = awsauth, headers=headers)
    r = requests.post(url, auth = awsauth, data=json.dumps(format).encode("utf-8"), headers=headers)
    #resp_elastic = requests.get(url2,headers={"Content-Type": "application/json"}).json()
    #print('<------------------GET-------------------->')
    print("LOG: requests post: ", r.text)
    #print(json.dumps(resp_elastic, indent=4, sort_keys=True))
    return {
        
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Content-Type': 'application/json'
        },
        'body': json.dumps("Image labels have been successfully detected!")
    }