import subprocess

def run():
    out = subprocess.check_output('git show "HEAD:Agent Dhara Backend/agent/intelligent_data_assessment.py"', shell=True, text=True, encoding='utf-8')
    lines = out.splitlines()
    
    in_rec = False
    in_fix = False
    
    print("--- DQ_ISSUE_RECOMMENDATIONS ---")
    for line in lines:
        if "DQ_ISSUE_RECOMMENDATIONS: Dict[str, str] = {" in line:
            in_rec = True
        if in_rec:
            print(line)
            if line.strip() == "}":
                in_rec = False
                
    print("\n--- FIXABILITY_BY_ISSUE_TYPE ---")
    for line in lines:
        if "FIXABILITY_BY_ISSUE_TYPE: Dict[str, str] = {" in line:
            in_fix = True
        if in_fix:
            print(line)
            if line.strip() == "}":
                in_fix = False

if __name__ == '__main__':
    run()
