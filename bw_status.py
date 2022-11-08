#! /usr/local/bin/python3

import subprocess
import sys
import time

jobid = sys.argv[-1]

# the dashboard data has some delay, so if dashboard_cli returns
# a non-zero exit code retry 4 times with 11s delays
attempts = 0
state = None
while attempts < 4:
    attempts += 1
    res = subprocess.run(
        [
            "dashboard_cli",
            "jobs",
            "-j",
            jobid,
            "--fields",
            "state",
            "--since",
            "-10d",
            "--noheader",
        ],
        capture_output=True,
        encoding="ascii",
    )
    if res.returncode == 0:
        state = res.stdout.strip()
        break
    if res.returncode == 4:
        # the job is not yet known to dashboard_cli. Assume 'running'
        state = "RUNNING"
        break
    if attempts == 4:
        print(res.stderr)
        sys.exit(res.returncode)
    else:
        time.sleep(11)

running_status = [
    "PENDING",
    "CONFIGURING",
    "COMPLETING",
    "RUNNING",
    "SUSPENDED",
    "PREEMPTED",
]

if state == "COMPLETED":
    print("success")
elif state in running_status:
    print("running")
else:
    print("failed")
