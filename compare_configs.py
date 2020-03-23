#!/usr/bin/env python

import sys
import yaml
from pathlib import Path
from deepdiff import DeepDiff
from pprint import pprint

yaml1Path = Path(sys.argv[1])
yaml2Path = Path(sys.argv[2])

with yaml1Path.open('r', encoding='utf-8') as f:
    yaml1Content = f.read()
    f.close()

with yaml2Path.open('r', encoding='utf-8') as f:
    yaml2Content = f.read()
    f.close()

yaml1 = yaml.safe_load(yaml1Content)
yaml2 = yaml.safe_load(yaml2Content)

result = DeepDiff(yaml1, yaml2)

if 'dictionary_item_added' in result or 'dictionary_item_removed' in result:
    if 'values_changed' in result:
        result.pop('values_changed')
    if 'type_changes' in result:
        result.pop('type_changes')
    print('Structure of {} and {} does NOT match!'.format(yaml1Path, yaml2Path))
    pprint(result, indent=2)
    exit(1)
else:
    print('Structure of {} and {} matches'.format(yaml1Path, yaml2Path))
