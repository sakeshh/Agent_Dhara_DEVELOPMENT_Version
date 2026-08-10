import os
import urllib.request
import json
from dotenv import load_dotenv

load_dotenv()

token = os.environ.get("BACKEND_AUTH_TOKEN", "change-me-dev")
url = "http://127.0.0.1:8000/assess"

payload = {
    "session_id": "1952cb8f-1b1f-47fc-a647-9769b8742b11",
    "sources": ["dbo.courses_raw", "dbo.students_raw"],
    "user_request": "assess selected tables"
}

headers = {
    "Content-Type": "application/json",
    "X-Backend-Token": token
}

req = urllib.request.Request(
    url,
    data=json.dumps(payload).encode('utf-8'),
    headers=headers,
    method='POST'
)

try:
    with urllib.request.urlopen(req) as response:
        body = response.read().decode('utf-8')
        result_data = json.loads(body)
        
        if result_data.get("ok"):
            res = result_data.get("result", {})
            print("Extraction errors:", res.get("extraction_errors"))
            print("Extractions count:", len(res.get("extractions") or []))
            for idx, ex in enumerate(res.get("extractions") or []):
                print(f"Extraction {idx}: source={ex.get('source')}, location_type={ex.get('location_type')}")
                val = ex.get("result", {})
                print(f"  Result keys: {list(val.keys())}")
                if 'datasets' in val:
                    print(f"  Result['datasets']: {list(val['datasets'].keys())}")
                    
            dq = res.get("data_quality", {})
            print("Data Quality keys:", list(dq.keys()))
            if 'datasets' in dq:
                print("Data Quality ['datasets'] keys:", list(dq['datasets'].keys()))
        else:
            print("Failed:", result_data)
            
except Exception as e:
    print("API call failed:", e)
