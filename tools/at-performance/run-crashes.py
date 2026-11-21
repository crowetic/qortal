#!/usr/bin/env python3
"""Run abrupt JVM exits and exact recovery checks in new disposable directories."""
import argparse
import json
import shutil
import subprocess
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('directory', type=Path, help='New output directory; must not exist')
p.add_argument('--classpath', required=True)
p.add_argument('--log-config', type=Path, required=True)
a = p.parse_args()
a.directory.mkdir(parents=True, exist_ok=False)
root = a.directory.resolve()
command = ['java', '-Xmx1g', '-Djava.awt.headless=true',
           '-Dlog4j.configurationFile=' + str(a.log_config.resolve()),
           '-cp', a.classpath, 'org.qortal.repository.hsqldb.ATCrashProbe']
results = []

def run(directory, stage, expected):
    log = root / (directory.name + '-' + stage + '.log')
    with log.open('w') as output:
        code = subprocess.run(command + [str(directory), stage], stdout=output,
                              stderr=subprocess.STDOUT, timeout=180).returncode
    if code != expected:
        raise RuntimeError(f'{stage}: exit {code}, expected {expected}; see {log}')
    return code

fresh = root / 'fresh'
fresh.mkdir()
(fresh / 'DISPOSABLE_AT_REPLAY').touch()
run(fresh, 'init_halt', 83)
run(fresh, 'check', 0)
results.append({'fixture': 'fresh', 'stage': 'init_halt', 'recovery': 'exact ledger match'})

for fixture, stages in [('active', ['payload', 'history', 'head', 'account', 'recipient', 'marker', 'commit_before', 'commit_after']),
                        ('frozen', ['history', 'head', 'account', 'recipient', 'marker', 'commit_before', 'commit_after']),
                        ('orphan', ['history', 'head', 'marker', 'commit_before', 'commit_after'])]:
    base = root / (fixture + '-base')
    base.mkdir()
    (base / 'DISPOSABLE_AT_REPLAY').touch()
    run(base, {'active': 'init', 'frozen': 'init_frozen', 'orphan': 'init_orphan'}[fixture], 0)
    for stage in stages:
        target = root / (fixture + '-' + stage)
        shutil.copytree(base, target)
        run(target, 'orphan_' + stage if fixture == 'orphan' else stage, 83)
        run(target, 'check', 0)
        results.append({'fixture': fixture, 'stage': stage, 'recovery': 'exact ledger match'})
        print(json.dumps(results[-1]), flush=True)
        (root / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
print(f'PASS: {len(results)} abrupt-exit recovery cases', flush=True)
