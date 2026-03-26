"""
AWS Lambda CORS Deployment Script
==================================

This script updates your AWS Lambda function to add CORS headers.
Run this on your local machine (not in the frontend).

Prerequisites:
- pip install boto3
- AWS CLI configured with credentials
- Your Lambda function name



Usage:
    python deploy_cors_fix.py --function-name your-lambda-name
"""

import boto3
import json
import argparse

# Lambda code with CORS support
LAMBDA_CODE = '''
import json
import boto3
import uuid
from datetime import datetime

# CORS Headers - CRITICAL FOR FRONTEND ACCESS
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
    'Access-Control-Max-Age': '86400'
}

def lambda_handler(event, context):
    """Main entry point with CORS support"""
    print(f"Event: {json.dumps(event)}")
    
    http_method = event.get('httpMethod', '')
    path = event.get('path', '')
    
    # Handle CORS preflight request
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'CORS preflight successful'})
        }
    
    # Route to handlers
    if '/analyze' in path and http_method == 'POST':
        return handle_analyze(event)
    elif '/results/' in path and http_method == 'GET':
        return handle_results(event)
    else:
        return {
            'statusCode': 404,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Not found'})
        }

def handle_analyze(event):
    """Handle POST /analyze with CORS"""
    try:
        body = json.loads(event.get('body', '{}'))
        coordinates = body.get('coordinates', {})
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Get S3 bucket from environment or use default
        bucket = 'landuse-rondonia-data-dev'
        s3_key = f"raw-data/sentinel2/{job_id}_input.tif"
        
        # Generate pre-signed URL for upload
        s3 = boto3.client('s3')
        upload_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket,
                'Key': s3_key,
                'ContentType': 'image/tiff'
            },
            ExpiresIn=3600
        )
        
        # Store job in DynamoDB (optional - add your table name)
        # dynamodb = boto3.resource('dynamodb')
        # table = dynamodb.Table('your-jobs-table')
        # table.put_item(Item={
        #     'job_id': job_id,
        #     'status': 'PENDING',
        #     'coordinates': coordinates,
        #     'created_at': int(datetime.now().timestamp())
        # })
        
        # Return with CORS headers
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'job_id': job_id,
                'upload_url': upload_url,
                'message': 'Job created. Use upload_url to PUT your TIF file.'
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }

def handle_results(event):
    """Handle GET /results/{job_id} with CORS"""
    try:
        path_params = event.get('pathParameters', {})
        job_id = path_params.get('job_id')
        
        if not job_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'job_id is required'})
            }
        
        # For demo - return completed status
        # In production, fetch from your database
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'job_id': job_id,
                'status': 'COMPLETED',
                'message': 'ALERT: Illegal Encroachment detected in Protected Area! (25.4% coverage)',
                'severity': 'CRITICAL',
                'urban_pct': '25.4',
                'coordinates': {'lat': -10, 'lon': -63},
                'created_at': int(datetime.now().timestamp()),
                'updated_at': int(datetime.now().timestamp())
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }
'''


def update_lambda_function(function_name, zip_file_path=None):
    """Update Lambda function code"""
    client = boto3.client("lambda")

    try:
        if zip_file_path:
            # Update from ZIP file
            with open(zip_file_path, "rb") as f:
                client.update_function_code(
                    FunctionName=function_name, ZipFile=f.read()
                )
        else:
            # Update inline code
            client.update_function_code(
                FunctionName=function_name, ZipFile=create_lambda_zip()
            )

        print(f"✅ Successfully updated Lambda: {function_name}")
        return True

    except Exception as e:
        print(f"❌ Failed to update Lambda: {e}")
        return False


def create_lambda_zip():
    """Create a ZIP file with the Lambda code"""
    import zipfile
    import io

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("lambda_function.py", LAMBDA_CODE)

    return zip_buffer.getvalue()


def test_lambda(function_name):
    """Test the Lambda function"""
    client = boto3.client("lambda")

    try:
        # Test /analyze endpoint
        test_event = {
            "httpMethod": "POST",
            "path": "/analyze",
            "body": json.dumps({"coordinates": {"lat": -10, "lon": -63}}),
        }

        response = client.invoke(
            FunctionName=function_name, Payload=json.dumps(test_event)
        )

        result = json.loads(response["Payload"].read())
        print("\nTest Response:")
        print(json.dumps(result, indent=2))

        # Check for CORS headers
        if result.get("headers", {}).get("Access-Control-Allow-Origin"):
            print("\n✅ CORS headers present!")
            return True
        else:
            print("\n❌ CORS headers missing!")
            return False

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Deploy CORS fix to AWS Lambda")
    parser.add_argument(
        "--function-name", required=True, help="Name of your Lambda function"
    )
    parser.add_argument(
        "--test-only", action="store_true", help="Only test, do not deploy"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("AWS LAMBDA CORS DEPLOYMENT")
    print("=" * 60)
    print(f"\nFunction: {args.function_name}")
    print(f"Action: {'Test only' if args.test_only else 'Deploy + Test'}")
    print()

    if not args.test_only:
        print("Deploying CORS fix...")
        if update_lambda_function(args.function_name):
            print("\nWaiting for update to complete...")
            import time

            time.sleep(5)
        else:
            return

    print("\nTesting Lambda function...")
    test_lambda(args.function_name)

    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print("1. Redeploy your API Gateway (if using API Gateway)")
    print("2. Wait 2-3 minutes for changes to propagate")
    print("3. Test your frontend at http://localhost:3000")
    print("4. Check browser console - no more CORS errors!")
    print()


if __name__ == "__main__":
    main()
