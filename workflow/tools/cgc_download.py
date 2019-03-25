#!/usr/bin/python3

import os
import sys
import json
import subprocess

task_dict = json.loads(sys.argv[1])
inputs = task_dict['input']
cwd = os.getcwd()

download_dckr = ""

subprocess.check_output(['docker','pull',download_dckr])

stdout, stderr, p, success = '', '', None, True

for f in inputs.get('cgc_task_outputs'):
    command = ['docker run',
               '-v', cwd + ':/data',
               '-e SB_API_ENDPOINT',
               '-e SB_AUTH_TOKEN',
               download_dckr, 'download',
               '--id', f.get('path'),
               '--output-dir', cwd]
    try:
        p = subprocess.Popen(command,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             shell=True)
        stdout, stderr = p.communicate()
    except Exception as e:
        print(e, file=sys.stderr)
        success = False

    if p and p.returncode != 0:
        success = False

    print(stdout.decode("utf-8"))
    print(stderr.decode("utf-8"), file=sys.stderr)

with open('output.json', 'w') as j:
    j.write(json.dumps({'output_dir':cwd}))

if not success:
    exit(p.returncode if p.returncode else 1)
