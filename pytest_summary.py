import json
import pytest 

pytest.main(["--report-log=test_log.json", "--tb=no",
             "-k not _116[ and not _120[ and not _133["]) 

report_log = []
with open("./logs/test_log.json", 'r') as f:
    for l in f.readlines():
        report_log.append(json.loads(l))

tests = {}
reports = {}
for log in report_log:
    if 'nodeid' in log.keys():
        if "column" in log["nodeid"] and log["when"] == "call":
            test, report = log["nodeid"].strip("]").split("[")
            if test not in tests:
                tests[test] = {"passed" : 0,
                               "total"  : 1,
                               "fails": []}
                if log["outcome"] == "passed":
                    tests[test]["passed"] += 1
                else:
                    tests[test]["fails"].append(report)
            else:
                tests[test]["total"] += 1
                if log["outcome"] == "passed":
                    tests[test]["passed"] += 1
                else:
                    tests[test]["fails"].append(report)
#print(json.dumps(tests, sort_keys=True, indent=4))

for test, results in tests.items():
    if results['passed'] != results['total']:
        print(f"{test} : {results['passed']/results['total']*100} %")
        failures = " ".join(report for report in results['fails'])
        print(f"fails: {failures}\n")
    else:
        print(f"{test} : {results['passed']/results['total']*100} %")