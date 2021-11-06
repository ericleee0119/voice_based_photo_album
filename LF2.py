import json
import os
import math
import dateutil.parser
import datetime
import time
import logging
import boto3
#from botocore.vendored import requests
import requests
from requests_aws4auth import AWS4Auth
#logger = logging.getLogger()
#logger.setLevel(logging.DEBUG)

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']


'''def close(message):
    response = {
        "sessionState":{
            'dialogAction': {
            'type': 'Close',
            'fulfillmentState':"Fulfilled",
            'message': message
            },
            "intent":{
            "state": "Fulfilled",
            "name": intent_name
        }
      }
    }
    return response'''



'''def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    return response'''

    
def lambda_handler(event, context):
    # TODO implement
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    #client = boto3.client('lex-runtime')
    client = boto3.client('lexv2-runtime')
    #logger.debug("In lambda")
    print("LOG: event: ", str(event))
    print("LOG: context: ", str(context))
    print("LOG: text event: ", str(event["queryStringParameters"]['q']))
    response_lex = client.recognize_text(
    botId='97ZPXWRNWQ',
    botAliasId='XLHJWSMGKR',
    localeId='en_US',
    sessionId='test_session',
    text=event["queryStringParameters"]['q'])
    print("reponse lex: ", str(response_lex))
    if 'slots' in response_lex['sessionState']['intent']:
        #keys = [response_lex['sessionState']['intent']['slots']['keyone']['value']['originalValue'],response_lex['sessionState']['intent']['slots']['keytwo']['value']['originalValue'],response_lex['sessionState']['intent']['slots']['keythree']['value']['originalValue']]
        key1 = ""
        key2 = ""
        key3 = ""
        if response_lex['sessionState']['intent']['slots']['keyone'] is not None:
            key1 = response_lex['sessionState']['intent']['slots']['keyone']['value']['originalValue']
            
        if response_lex['sessionState']['intent']['slots']['keytwo'] is not None:
            key2 = response_lex['sessionState']['intent']['slots']['keytwo']['value']['originalValue']
            
        if response_lex['sessionState']['intent']['slots']['keythree'] is not None:
            key3 = response_lex['sessionState']['intent']['slots']['keythree']['value']['originalValue']
            
            
        url3 = "https://2021awsassignment2.s3.us-west-2.amazonaws.com/"
        keys = [key1, key2, key3]
        print(keys)
        pictures = search_intent(keys) #get images keys from elastic search labels
        returnString = ""
        for i in range(len(pictures)):
            returnString += pictures[i]
            if(i + 1 != (len(pictures))):
                returnString += "_"
        response = {
            "statusCode": 200,
            'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            #'Content-Type': 'application/json'
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
            "body": returnString,
            "isBase64Encoded": False
        }
    else:
        response = {
            "statusCode": 200,
            'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET,OPTIONS'
            },
            "body": "",
            "isBase64Encoded": False}
    #logger.debug('event.bot.name={}'.format(event['bot']['name']))
    print("LOG: RETURN Response: ", str(response))
    return response
    
def dispatch(intent_request):
    #logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']
    return search_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')

def search_intent(labels):

    # key1 = get_slots(intent_request)['keyone']
    # key2 = get_slots(intent_request)['keytwo']
    # key3 = get_slots(intent_request)['keythree']
    url = 'https://search-photos-ebkwwzrdqfndyfzzo4kpmdcb5y.us-west-2.es.amazonaws.com/photos/_search?q='
    
    region = 'us-west-2'  # For example, us-west-1
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth("AKIAXBG5QJ2HV6ENLQPG", "o0T3P23hzBBSv9n2XzVEGVnUdU4OHoVQWOeTsO4D",
                       region, service, session_token=None)
                       
                       
    print("access key: ", credentials.access_key)
    print("secret key: ", credentials.secret_key)
    print("region: ", region)
    print("service: ", service)
                       
                       
    headers = {"Content-Type": "application/json"}
    
    #labels = [key1,key2,key3]
    resp = []
    for label in labels:
        if (label is not None) and label != '':
            url2 = url+str(label)
            response = requests.get(url2)
            query = {
                    "size": 1,
                    "query": {
                        "function_score": {
                            "query": {
                                "bool": {
                                    "must": [{"match": {"labels": label}}]
                                },
                            },
                            "functions": [
                                {
                                    "random_score": {}
                                }
                            ]
                        }
                    }
                }
            
            response = requests.get(url2, auth=awsauth, headers=headers,
                     data=json.dumps(query))
            
            print("LOG: response: ", response)
            print("url: ", url2)
            print("LOG: reponse status: ", response.status_code)
            resp.append(response.json())
    print ("LOG: resp: ", resp)
  
    output = []
    for r in resp:
        if 'hits' in r:
             for val in r['hits']['hits']:
                key = val['_source']['objectKey']
                if key not in output:
                    output.append(key)
    #url = "https://vpc-photos-b4al4b3cnk5jcfbvlrgxxu3vhu.us-east-1.es.amazonaws.com/photos/_search?pretty=true&q=*:*"
    #print(url)
    #resp = requests.get(url,headers={"Content-Type": "application/json"}).json()
    #resp = requests.get(url)
    print(output)

    return output
    '''return close(
              {'contentType': 'PlainText',
               'content': ''.join(output)})'''