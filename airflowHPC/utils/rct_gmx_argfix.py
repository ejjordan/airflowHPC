#!/usr/bin/env python3

import os
import sys
import json

# ensure we handle an RP task
if 'RP_TASK_ID' not in os.environ:
    sys.exit(0)


args = ['-pin on']
tid  = str(os.environ['RP_TASK_ID'])
rank = int(os.environ['RP_RANK'])

# load the task slots
with open('./%s.sl' % tid) as fin:
    slot = json.load(fin)[rank]

# import pprint
# pprint.pprint(slot)

# get assigned resources
core_ids = [core['index'] for core in slot['cores']]
gpu_ids  = [gpu['index']  for gpu  in slot['gpus'] ]

cmin = min(core_ids)
cmax = max(core_ids)

# ensure that core IDs are sequential without any gaps
if core_ids != list(range(cmin, cmax + 1)):
    sys.exit(0)

# we can only handle one gpu id
if len(gpu_ids) > 1:
    sys.exit(0)

# assume all core IDs are sequential
args.append('-pinoffset=%d' % cmin)
args.append('-ntomp=%d' % len(core_ids))
args.append('-gputasks=%d' % gpu_ids[0])

print(' '.join(args))

