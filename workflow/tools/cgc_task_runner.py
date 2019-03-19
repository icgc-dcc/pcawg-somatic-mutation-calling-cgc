#!/usr/bin/python3

import os
import sys
import json
import yaml
import hashlib
import copy
import subprocess
import sevenbridges as sbg

task_dict = json.loads(sys.argv[1])
inputs = task_dict['input']
cwd = os.getcwd()

os.environ['SB_AUTH_TOKEN'] = os.environ['CGC_ACCESS_TOKEN']
os.environ['SB_API_ENDPOINT'] = 'https://cgc-api.sbgenomics.com/v2'

# common input args for all PCAWG cgc apps (varinat callers/tools)
cgc_project = inputs['cgc_project']
study = inputs['study']
donor_id = inputs['donor_id']
app = inputs['app']
instance_type = inputs['instance_type']
use_spot = inputs['use_spot']

app_name, app_rev = app.split('/')
api = sbg.Api()


def get_input_md5(cgc_input):
    my_input = copy.deepcopy(cgc_input)
    filtered_input_dict = {
        k: my_input[k] for k in my_input if isinstance(my_input[k], dict) and \
                                            my_input[k].get('class') == 'File' and \
                                            my_input[k].pop('name', 1)
    }

    return hashlib.md5(json.dumps(filtered_input_dict,
                                  sort_keys=True).encode('utf-8')).hexdigest()


# get one of the delly's output files as dkfz's input
delly_bedpe = {}
if app_name == 'pcawg-dkfz-caller':
    delly_task_id = inputs['delly_task_id']
    files = api.files.query(project=cgc_project, origin={'task': delly_task_id})
    bedpe_file = [(f.id, f.name) for f in files if f.name.endswith('.somatic.sv.bedpe.txt')][0]
    delly_bedpe['path'] = bedpe_file[0]
    delly_bedpe['name'] = bedpe_file[1]

# app specific args
app_input = {
    'pcawg-delly-caller': {
        'run-id': donor_id,
        'tumor-bam': {
            'class': 'File',
            'path': inputs.get('tumor-bam', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('tumor-bam', '').split('|')[-1],
        },
        'normal-bam': {
            'class': 'File',
            'path': inputs.get('normal-bam', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('normal-bam', '').split('|')[-1],
        },
        'reference-gz': {
            'class': 'File',
            'path': inputs.get('reference-gz', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('reference-gz', '').split('|')[-1],
        },
        'reference-gc': {
            'class': 'File',
            'path': inputs.get('reference-gc', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('reference-gc', '').split('|')[-1],
        },
    },
    'pcawg-dkfz-caller': {
        'run-id': donor_id,
        'tumor-bam': {
            'class': 'File',
            'path': inputs.get('tumor-bam', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('tumor-bam', '').split('|')[-1],
        },
        'normal-bam': {
            'class': 'File',
            'path': inputs.get('normal-bam', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('normal-bam', '').split('|')[-1],
        },
        'reference-gz': {
            'class': 'File',
            'path': inputs.get('reference-gz', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('reference-gz', '').split('|')[-1],
        },
        'delly-bedpe': {
            'class': 'File',
            'path': delly_bedpe.get('path', ''),
            'name': delly_bedpe.get('name', ''),
        },
    },
    'pcawg-sanger-caller': {
        'tumor': {
            'class': 'File',
            'path': inputs.get('tumor', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('tumor', '').split('|')[-1],
        },
        'normal': {
            'class': 'File',
            'path': inputs.get('normal', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('normal', '').split('|')[-1],
        },
        'refFrom': {
            'class': 'File',
            'path': inputs.get('refFrom', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('refFrom', '').split('|')[-1],
        },
        'bbFrom': {
            'class': 'File',
            'path': inputs.get('bbFrom', '').split('|')[0].replace('cgc://', ''),
            'name': inputs.get('bbFrom', '').split('|')[-1],
        }
    }
}

input_hash = get_input_md5(app_input[app_name])

# prepare syncr sbg task file
sbg_task = {
    'probing_interval': 300,  # probing task status every 5 min on CGC
    'meta': [
        {'study': study},
        {'donor_id': donor_id},
        {'app_name': app_name},
        {'app_rev': app_rev},
        {'input_hash': input_hash}
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

command = 'docker run --rm ' \
          '-e SB_AUTH_TOKEN ' \
          '-v `pwd`:/workdir ' \
          '--workdir /workdir ' \
          'quay.io/pancancer/syncr:0.0.2 ' \
          'sbg_task'

stdout, stderr, p, success = '', '', None, True
try:
    p = subprocess.Popen([command],
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

# collect cgc task id
task_info = {}
try:  # _task_info file may not exist
    with open('_task_info') as t:
        task_info = yaml.safe_load(t)
except FileNotFoundError:
    success = False

# write to output.json
output = {
    'cgc_task_id': task_info.get('id'),
    'cgc_task_outputs': {},
    'cgc_task_details': {}
}

if output['cgc_task_id']:
    cgc_task = api.tasks.get(output['cgc_task_id'])

    out_dict = dict(cgc_task.outputs)
    for o in out_dict:
        if isinstance(out_dict[o], dict) and out_dict[o].get('class') != 'File':
            continue
        output['cgc_task_outputs'][o] = {
            'name': out_dict[o]['name'],
            'path': out_dict[o]['path'],
            'size': out_dict[o]['size'],
        }

    output['cgc_task_details'] = {
        'start_time': str(cgc_task.start_time),
        'executed_by': cgc_task.executed_by,
        'instance_type': cgc_task.execution_settings['instance_type'],
        'execution_duration': cgc_task.execution_status.execution_duration,
        'price': cgc_task.price.amount,
        'spot_instance': cgc_task.use_interruptible_instances,
    }

with open('output.json', 'w') as j:
    j.write(json.dumps(output))

if not success:
    exit(p.returncode if p.returncode else 1)
