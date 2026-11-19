#!/usr/bin/env python3
"""
Run the BIDS validator that Loris-MRI uses (the `bids_validator` package that
comes with pybids) on every file of a BIDS dataset, and print the files it
rejects as JSON. Read-only.

Why: Loris-MRI's BIDS import loads the dataset with pybids and validation on.
A file the validator rejects (a name that does not follow BIDS, a label that
does not fit) is not indexed - it is silently left out of the import.

Usage: python bids_validator_check.py <bids_dir>
Output: {"validator": "bids_validator 1.14.7", "checked": N, "invalid": ["/sub-01/..."]}
        or {"error": "..."} (exit 2) when the validator is not installed.
"""

import json
import os
import sys

# Folders pybids does not index for the import (not validated here either).
SKIP_TOP = {'derivatives', 'sourcedata', 'code', '.git', '.datalad'}


def main() -> int:
    if len(sys.argv) != 2 or not os.path.isdir(sys.argv[1]):
        print(json.dumps({'error': 'usage: bids_validator_check.py <bids_dir>'}))
        return 2
    try:
        import bids_validator
        from bids_validator import BIDSValidator
    except ImportError as e:
        print(json.dumps({'error': f'bids_validator is not installed for {sys.executable} ({e})'}))
        return 2

    root = os.path.abspath(sys.argv[1])
    validator = BIDSValidator()
    checked = 0
    invalid = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        if rel_dir == '.':
            dirnames[:] = [d for d in dirnames if d not in SKIP_TOP]
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        for name in filenames:
            if name.startswith('.'):
                continue
            rel = '/' + os.path.relpath(os.path.join(dirpath, name), root)
            checked += 1
            if not validator.is_bids(rel):
                invalid.append(rel)

    version = getattr(bids_validator, '__version__', '')
    print(json.dumps({'validator': f'bids_validator {version}'.strip(), 'checked': checked,
                      'invalid': sorted(invalid)}))
    return 0


if __name__ == '__main__':
    sys.exit(main())
