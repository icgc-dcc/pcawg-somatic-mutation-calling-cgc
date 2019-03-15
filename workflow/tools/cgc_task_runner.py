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
        'tumor-bam': inputs.get('tumor-bam', '').replace('', 'cgc://'),
        'normal-bam': inputs.get('normal-bam', '').replace('', 'cgc://'),
        'reference-gz': inputs.get('reference-gz', '').replace('', 'cgc://'),
        'exclude-reg': inputs.get('exclude-reg', '').replace('', 'cgc://'),
        'gencode-gz': inputs.get('gencode-gz', '').replace('', 'cgc://'),
    },
    'pcawg-dkfz-caller': {
        'tumor-bam': inputs.get('tumor-bam', '').replace('', 'cgc://'),
        'normal-bam': inputs.get('normal-bam', '').replace('', 'cgc://'),
        'reference-gz': inputs.get('reference-gz', '').replace('', 'cgc://'),
        'delly_task_id': inputs.get('delly_task_id', '').replace('', 'cgc://'),
    },
    'pcawg-sanger-caller': {
        'tumor': inputs.get('tumor', '').replace('', 'cgc://'),
        'normal': inputs.get('normal', '').replace('', 'cgc://'),
        'refFrom': inputs.get('refFrom', '').replace('', 'cgc://'),
        'bbFrom': inputs.get('bbFrom', '').replace('', 'cgc://'),
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
        'inputs': app_input['app_name']
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

stdout, stderr = '', ''
try:
    p = subprocess.Popen([command],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         shell=True)
    stdout, stderr = p.communicate()
except Exception as e:
    exit(stderr)

# upon completion, collect task id
with open('_task_info') as t:
    task_info = yaml.safe_load(t)

# write to output.json
with open('output.json') as j:
    json.dumps({
        'task_id': task_info['id']
    })
