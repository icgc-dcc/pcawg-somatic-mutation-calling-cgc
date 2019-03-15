#!/usr/bin/python3

import os
import sys
import json
import yaml
import subprocess

task_dict = json.loads(sys.argv[1])
inputs = task_dict['input']
cwd = os.getcwd()

# common input args for all PCAWG cgc apps (varinat callers/tools)
cgc_project = inputs['cgc_project']
study = inputs['study']
donor_id = inputs['donor_id']
app = inputs['app']
instance_type = inputs['instance_type']
use_spot = inputs['use_spot']

app_name, app_rev = app.split('/')

# app specific args
app_input = {
    'pcawg-delly-caller': {
        'tumor-bam': {
            'class': 'File',
            'path': inputs.get('tumor-bam', '').replace('cgc://', ''),
        },
        'normal-bam': {
            'class': 'File',
            'path': inputs.get('normal-bam', '').replace('cgc://', ''),
        },
        'reference-gz': {
            'class': 'File',
            'path': inputs.get('reference-gz', '').replace('cgc://', ''),
        },
        'exclude-reg': {
            'class': 'File',
            'path': inputs.get('exclude-reg', '').replace('cgc://', ''),
        },
        'gencode-gz': {
            'class': 'File',
            'path': inputs.get('gencode-gz', '').replace('cgc://', ''),
        }
    },
    'pcawg-dkfz-caller': {
        'tumor-bam': {
            'class': 'File',
            'path': inputs.get('tumor-bam', '').replace('cgc://', ''),
        },
        'normal-bam': {
            'class': 'File',
            'path': inputs.get('normal-bam', '').replace('cgc://', ''),
        },
        'reference-gz': {
            'class': 'File',
            'path': inputs.get('reference-gz', '').replace('cgc://', ''),
        },
        'delly_task_id': {
            'class': 'File',
            'path': inputs.get('delly_task_id', '').replace('cgc://', ''),
        }
    },
    'pcawg-sanger-caller': {
        'tumor': {
            'class': 'File',
            'path': inputs.get('tumor', '').replace('cgc://', ''),
        },
        'normal': {
            'class': 'File',
            'path': inputs.get('normal', '').replace('cgc://', ''),
        },
        'refFrom': {
            'class': 'File',
            'path': inputs.get('refFrom', '').replace('cgc://', ''),
        },
        'bbFrom': {
            'class': 'File',
            'path': inputs.get('bbFrom', '').replace('cgc://', ''),
        }
    }
}

# TODO: for pcawg-dkfz-caller we need to do a bit extra work to get the 'somatic_bedpe' output file from DELLY step

# prepare syncr sbg task file
sbg_task = {
    'meta': [
        {'study': study},
        {'donor_id': donor_id},
        {'app_name': app_name},
        {'app_rev': app_rev}
    ],
    'task': {
        'project': cgc_project,
        'app': '%s/%s' % (cgc_project, app),
        'execution_settings': {
            'instance_type': instance_type
        },
        'use_interruptible_instances': use_spot,
        'inputs': app_input[app_name]
    }
}

# write sbg task config to an YAML
with open('task.yaml', 'w') as t:
    yaml.safe_dump(sbg_task, t, default_flow_style=False)

# now launch the sbg task
os.environ['SB_AUTH_TOKEN'] = os.environ['CGC_ACCESS_TOKEN']

command = 'docker run --rm ' \
          '-e SB_AUTH_TOKEN ' \
          '-v `pwd`:/workdir ' \
          '--workdir /workdir ' \
          'quay.io/pancancer/syncr:0.0.2 ' \
          'sbg_task'

success = True
stdout, stderr = '', ''
try:
    p = subprocess.Popen([command],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    stdout, stderr = p.communicate()
except Exception as e:
    success = False

if p.returncode != 0:
    success = False

with open('stdout.txt', 'a') as o:
    o.write(stdout.decode("utf-8"))
with open('stderr.txt', 'a') as e:
    e.write(stderr.decode("utf-8"))

if not success:
    exit(1)

# upon completion, collect task id
with open('_task_info') as t:
    task_info = yaml.safe_load(t)

# write to output.json
with open('output.json') as j:
    json.dumps({
        'task_id': task_info['id']
    })
