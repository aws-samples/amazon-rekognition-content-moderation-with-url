import json
import boto3
import botocore.vendored.requests.packages.urllib3 as urllib3
import io


def getModerationForUrl(url):
    try:
        extensions = ['jpg', 'jpeg', 'png']
        if not any(url.lower().endswith(ext) for ext in extensions):
            return 400, "Amazon Rekognition supports only the following image formats: jpg, jpeg, png"
        
        manager = urllib3.PoolManager()
        response = manager.request('GET', url, preload_content=False)
        if response.status == 404:
            return 404, "Image not found"
        reader = io.BufferedReader(response, 8)
        readBytes = reader.read()
        reader.close()
    
        if(len(readBytes) > 5242880):
            return 400, "Amazon Rekognition does not support images more than 5MB in this implementation. Use images stored on Amazon S3. See here: https://docs.aws.amazon.com/rekognition/latest/dg/limits.html"
        
        client = boto3.client('rekognition')
        response = client.detect_moderation_labels(Image={'Bytes': readBytes}, MinConfidence=60)
        return 200, response['ModerationLabels']
    
    except:
        return 503, "Unexpected error"

def lambda_handler(event, context):
    body = event.get('body')
    if body is None:
        raise KeyError("payload is missing")
        
    url = json.loads(body)['url']
    if url is None:
        raise KeyError("url is missing from the payload")
        
    moderationResponse = getModerationForUrl(url)
   
    return {
        'statusCode': moderationResponse[0],
        'body': json.dumps(moderationResponse[1])
    }
