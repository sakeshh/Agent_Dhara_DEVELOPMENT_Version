import requests
import json
import time

BASE_URL = "http://127.0.0.1:8000"
headers = {
    "X-Backend-Token": "change-me-dev",
    "Content-Type": "application/json"
}

SESSION_ID = "a3447b6b-1763-4282-82c5-73b5a32119fc"

def run_step(name, endpoint, payload):
    print(f"\n--- Running Step: {name} ---")
    url = f"{BASE_URL}{endpoint}"
    print(f"Request: POST {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    
    t0 = time.time()
    try:
        res = requests.post(url, headers=headers, json=payload, timeout=240)
        duration = time.time() - t0
        print(f"Response Status: {res.status_code} (took {duration:.2f}s)")
        
        try:
            data = res.json()
            # Truncate large code outputs for readability
            if "code" in data:
                print("Code generated successfully. Length:", len(data["code"]))
                # Print first 5 and last 5 lines of generated code to verify
                lines = data["code"].splitlines()
                if len(lines) > 20:
                    print("\nFirst 10 lines of code:")
                    print("\n".join(lines[:10]))
                    print("...\nLast 10 lines of code:")
                    print("\n".join(lines[-10:]))
                else:
                    print("\nGenerated Code:")
                    print(data["code"])
            else:
                print(json.dumps(data, indent=2))
            return data
        except Exception:
            print("Response (not JSON):")
            print(res.text[:1000])
            return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None

def main():
    print(f"Starting pipeline regeneration & execution for session {SESSION_ID}")
    
    # 1. Generate cleanse_only code
    payload_cleanse = {
        "session_id": SESSION_ID,
        "engine": "sql",
        "sql_dialect": "tsql",
        "codegen_mode": "template",
        "generation_mode": "cleanse_only"
    }
    run_step("1. Cleanse Generation", "/etl/generate", payload_cleanse)
    
    # 2. Generate transform_only code (this will trigger our merge logic)
    payload_transform = {
        "session_id": SESSION_ID,
        "engine": "sql",
        "sql_dialect": "tsql",
        "codegen_mode": "template",
        "generation_mode": "transform_only"
    }
    run_step("2. Transform Generation", "/etl/generate", payload_transform)
    
    # 3. Execute the pipeline (which runs the combined code and performs Fabric mirroring)
    payload_execute = {
        "session_id": SESSION_ID,
        "approved": True,
        "dry_run": False
    }
    exec_res = run_step("3. Execute Pipeline", "/etl/execute", payload_execute)
    
    print("\n--- Pipeline Verification Summary ---")
    if exec_res:
        print("Execution status:", "Success" if exec_res.get("ok") else "Failed")
        if not exec_res.get("ok"):
            print("Error message:", exec_res.get("message"))
        else:
            print("Post Execution Summary:")
            print(json.dumps(exec_res.get("post_execution_summary"), indent=2))
            
            # Print Fabric Mirror Results if available
            # Let's load the session details to get the fabric mirror status saved under ctx['etl_flow']
            print("\nFetching session details from backend...")
            sess_res = requests.get(f"{BASE_URL}/sessions/{SESSION_ID}", headers=headers)
            if sess_res.status_code == 200:
                sess_data = sess_res.json()
                flow = sess_data.get("context", {}).get("etl_flow", {})
                fabric_result = flow.get("fabric_mirror_result")
                print("\nFabric Mirror Result in session state:")
                print(json.dumps(fabric_result, indent=2))
            else:
                print("Failed to fetch session details:", sess_res.status_code)
    else:
        print("Execution result could not be retrieved.")

if __name__ == "__main__":
    main()
