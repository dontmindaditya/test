# ROOT FIX - Add CORS to AWS Backend


## The Problem
Your AWS Lambda doesn't send CORS headers, causing browser to block requests with:
```
Access-Control-Allow-Origin header missing
```

## The Solution
Add CORS headers to **every response** from your Lambda function.

## Method 1: Update Lambda Code (RECOMMENDED)

### Step 1: Add CORS Headers to Your Lambda

Find your Lambda function in AWS Console and add this to the top:

```python
import json

# Add this at the top of your lambda_function.py
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',  # Or use 'http://localhost:3000' for specific origin
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
    'Access-Control-Max-Age': '86400'
}
```

### Step 2: Modify Your Handler Functions

Find your `/analyze` handler and add headers:

**BEFORE:**
```python
def handle_analyze(event, context):
    # ... your logic ...
    return {
        'statusCode': 200,
        'body': json.dumps({
            'job_id': job_id,
            'upload_url': upload_url
        })
    }
```

**AFTER:**
```python
def handle_analyze(event, context):
    # ... your logic ...
    return {
        'statusCode': 200,
        'headers': CORS_HEADERS,  # <-- ADD THIS LINE
        'body': json.dumps({
            'job_id': job_id,
            'upload_url': upload_url
        })
    }
```

### Step 3: Modify Your Results Handler

Find your `/results/{job_id}` handler and add headers:

**BEFORE:**
```python
def handle_results(event, context):
    # ... your logic ...
    return {
        'statusCode': 200,
        'body': json.dumps({
            'job_id': job_id,
            'status': status
        })
    }
```

**AFTER:**
```python
def handle_results(event, context):
    # ... your logic ...
    return {
        'statusCode': 200,
        'headers': CORS_HEADERS,  # <-- ADD THIS LINE
        'body': json.dumps({
            'job_id': job_id,
            'status': status
        })
    }
```

### Step 4: Add OPTIONS Handler

Add this to handle CORS preflight requests:

```python
def lambda_handler(event, context):
    http_method = event.get('httpMethod', '')
    
    # Handle CORS preflight
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'OK'})
        }
    
    # ... rest of your routing logic ...
```

### Step 5: Deploy

1. Save the updated code
2. Click **Deploy** in AWS Lambda console
3. Test your frontend again

---

## Method 2: API Gateway CORS (Easier Alternative)

If you don't want to modify Lambda code, enable CORS in API Gateway:

### Step 1: Open API Gateway Console
1. Go to AWS Console → API Gateway
2. Find your API: `48ih4pysre`
3. Click on it

### Step 2: Enable CORS for /analyze
1. Click on **/analyze** resource
2. Click **Actions** dropdown (top right)
3. Select **Enable CORS**
4. Check these options:
   - ✅ Access-Control-Allow-Headers: `Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token`
   - ✅ Access-Control-Allow-Methods: `POST, OPTIONS`
   - ✅ Access-Control-Allow-Origin: `*` (or your domain)
5. Click **Enable CORS and replace existing CORS headers**
6. Click **Yes, replace existing values**

### Step 3: Enable CORS for /results/{job_id}
1. Click on **/results/{job_id}** resource
2. Click **Actions** → **Enable CORS**
3. Check:
   - ✅ Access-Control-Allow-Headers: `Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token`
   - ✅ Access-Control-Allow-Methods: `GET, OPTIONS`
   - ✅ Access-Control-Allow-Origin: `*`
4. Click **Enable CORS and replace existing CORS headers**

### Step 4: Deploy API
1. Click **Actions** → **Deploy API**
2. Deployment stage: Select your stage (probably `dev`)
3. Click **Deploy**
4. Wait 2-3 minutes for changes to propagate

---

## Method 3: Complete Lambda Code (Copy-Paste Ready)

If you want to replace your entire Lambda function, use this:

```python
import json
import boto3
import uuid
from datetime import datetime

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
    'Access-Control-Max-Age': '86400'
}

def lambda_handler(event, context):
    print(f"Event: {json.dumps(event)}")
    
    http_method = event.get('httpMethod', '')
    path = event.get('path', '')
    
    # Handle CORS preflight
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'OK'})
        }
    
    # Route requests
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
    try:
        body = json.loads(event.get('body', '{}'))
        coordinates = body.get('coordinates', {})
        
        job_id = str(uuid.uuid4())
        
        # Generate pre-signed URL
        s3 = boto3.client('s3')
        bucket = 'landuse-rondonia-data-dev'
        s3_key = f"raw-data/sentinel2/{job_id}_input.tif"
        
        upload_url = s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': bucket, 'Key': s3_key, 'ContentType': 'image/tiff'},
            ExpiresIn=3600
        )
        
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
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }

def handle_results(event):
    try:
        path_params = event.get('pathParameters', {})
        job_id = path_params.get('job_id')
        
        # For demo, return a completed job
        # In production, fetch from DynamoDB
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'job_id': job_id,
                'status': 'COMPLETED',
                'message': 'ALERT: Illegal Encroachment detected in Protected Area! (25.4% coverage)',
                'severity': 'CRITICAL',
                'urban_pct': '25.4',
                'created_at': int(datetime.now().timestamp()),
                'updated_at': int(datetime.now().timestamp())
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)})
        }
```

---

## Verification

### Test with curl:
```bash
curl -v -X POST https://48ih4pysre.execute-api.us-west-2.amazonaws.com/dev/analyze \
  -H "Content-Type: application/json" \
  -d '{"coordinates":{"lat":-10,"lon":-63}}' 2>&1 | grep -i "access-control"
```

You should see:
```
< access-control-allow-origin: *
< access-control-allow-headers: Content-Type,X-Amz-Date...
```

### Test from browser:
1. Open your frontend at `http://localhost:3000/analysis`
2. Open browser DevTools → Network tab
3. Click "Analyze"
4. Look at the response headers - you should see `access-control-allow-origin: *`

---

## After Fix

Once you add CORS headers to the backend, you can remove the proxy from frontend:

**Current (with proxy):**
```typescript
fetch("/api/analyze")  // Proxy route
```

**After CORS fix (direct):**
```typescript
fetch("https://48ih4pysre.execute-api.us-west-2.amazonaws.com/dev/analyze")
```

---

## Which Method Should You Use?

| Method | Difficulty | Maintenance | Recommendation |
|--------|-----------|-------------|----------------|
| Lambda Code | Medium | Low | ✅ Best for production |
| API Gateway | Easy | Medium | ⚡ Quick fix |
| Replace Lambda | Easy | Low | 🔧 If current code is simple |

**My recommendation:** Use **Method 1** (Lambda Code) for production, or **Method 2** (API Gateway) for quick fix.
