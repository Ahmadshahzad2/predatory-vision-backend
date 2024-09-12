import boto3
import json
import base64
import cv2
import numpy as np

def image_to_base64(img_bytes):
    """
    Converts an OpenCV image to a base64 string.
    """
    image_base64 = base64.b64encode(img_bytes).decode('utf-8')
    return image_base64

def create_payload(img_bytes):
    """
    Creates a JSON payload for AWS Lambda function with a base64-encoded image.
    """
    base64_string = image_to_base64(img_bytes)
    payload = {
        "body": base64_string
    }
    # Ensure the payload is a JSON string
    return json.dumps(payload)


def send_image_to_lambda(image):
    """
    Sends an image (frame) to the AWS Lambda function and retrieves the processed image.
    """
    # Encode the frame to jpg format
    _, img_bytes = cv2.imencode('.jpg', image)
    payload = create_payload(img_bytes.tobytes())

    lambda_client = boto3.client('lambda', region_name='us-west-2',)
    
    response = lambda_client.invoke(
        FunctionName='ultralytics',
        InvocationType='RequestResponse',
        Payload=payload  # Make sure Payload is a JSON string
    )
    
    result = json.load(response['Payload'])
    processed_image_base64 = json.loads(result['body'])['result']
    processed_image_data = base64.b64decode(processed_image_base64)
    nparr = np.frombuffer(processed_image_data, np.uint8)
    processed_image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    return processed_image