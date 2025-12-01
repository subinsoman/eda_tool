import openpyxl
import glob
import os

# Find the latest generated report
list_of_files = glob.glob('data_1_metrics_report_*.xlsx')
latest_file = max(list_of_files, key=os.path.getctime)
print(f"Checking file: {latest_file}")

wb = openpyxl.load_workbook(latest_file)
ws_metrics = wb['Column Metrics']

# Get all headers
header_row = ws_metrics[1]
headers = [cell.value for cell in header_row]

print("Headers found:", headers)

# Expected present
expected_present = ['P10 (10%)', 'P70 (70%)', 'P90 (90%)']
# Expected absent
expected_absent = ['P1 (1%)', 'P5 (5%)', 'P20 (20%)', 'P30 (30%)', 'P40 (40%)', 'P60 (60%)', 'P80 (80%)', 'P95 (95%)', 'P99 (99%)', 'P99.9 (99.9%)', 'Tail Ratio (P95/P5)']

all_passed = True

for col in expected_present:
    if col in headers:
        print(f"PASS: {col} is present.")
    else:
        print(f"FAIL: {col} is MISSING.")
        all_passed = False

for col in expected_absent:
    if col not in headers:
        print(f"PASS: {col} is absent.")
    else:
        print(f"FAIL: {col} is PRESENT.")
        all_passed = False

if all_passed:
    print("\nVerification SUCCESS: Percentile metrics are correct.")
else:
    print("\nVerification FAILED: Percentile metrics are incorrect.")
