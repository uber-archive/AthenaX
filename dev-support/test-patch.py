#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import fnmatch
import logging
import os
from subprocess import Popen
import sys
import xml.etree.ElementTree as ET

TARGET_DIR = "./target/"
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
LOG = logging.getLogger('testpatch')
LOG.setLevel(logging.INFO)


def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                yield os.path.join(root, basename)


def tee(proc, f):
    for line in iter(proc.stdout.readline, ''):
        sys.stdout.write(line)
        f.write(line)


class TestPatchRunner:
    def __init__(self, out):
        self.compilationWarnings = 0
        self.failedTests = []
        self.findbugsWarnings = 0
        self.out = out

    def validate(self):
        LOG.info('Validating dependency and licenses')
        proc = Popen([
            '/bin/bash', '-c',
            'mvn clean enforcer:enforce apache-rat:check --batch-mode --fail-at-end|tee ' + TARGET_DIR + '/validation.txt;'
            + '[ $PIPESTATUS -eq 0 ] || exit $PIPESTATUS'], stdout=sys.stdout, stderr=sys.stderr)
        return proc.wait()

    def checkstyle(self):
        LOG.info('Validating checkstyles')
        proc = Popen([
            '/bin/bash', '-c',
            'mvn clean checkstyle:check --batch-mode --fail-at-end|tee -a ' + TARGET_DIR + '/validation.txt;'
            + '[ $PIPESTATUS -eq 0 ] || exit $PIPESTATUS'], stdout=sys.stdout, stderr=sys.stderr)
        return proc.wait()

    def compile(self):
        LOG.info('Compiling the project')
        proc = Popen([
            '/bin/bash', '-c',
            'mvn compile test-compile install'
            + ' -DskipTests=true --update-snapshots --batch-mode --fail-at-end|tee ' + TARGET_DIR + '/buildWarnings.txt;'
            + '[ $PIPESTATUS -eq 0 ] || exit $PIPESTATUS'], stdout=sys.stdout, stderr=sys.stderr)

        ret = proc.wait()
        if ret != 0:
            LOG.warning('The compilation has failed.')
        else:
            with open(TARGET_DIR + '/buildWarnings.txt') as f:
                for line in f:
                    if line.startswith('[WARNING]') and line.find('/target/generated-sources/') == -1:
                        self.compilationWarnings += 1
        return ret

    def run_test(self):
        LOG.info('Running tests')
        proc = Popen([
            '/bin/bash', '-c',
            'mvn test --batch-mode --fail-at-end|tee ' + TARGET_DIR + '/testResults.txt;'
            + '[ $PIPESTATUS -eq 0 ] || exit $PIPESTATUS'], stdout=sys.stdout, stderr=sys.stderr)
        return proc.wait()

    def run_findbugs(self):
        LOG.info('Running findbugs')
        proc = Popen([
            '/bin/bash', '-c',
            'mvn findbugs:findbugs --batch-mode --fail-at-end|tee ' + TARGET_DIR + '/findbugsResults.txt;'
            + '[ $PIPESTATUS -eq 0 ] || exit $PIPESTATUS'], stdout=sys.stdout, stderr=sys.stderr)
        return proc.wait()

    def parse_findbugs(self):
        for f in find_files('.', 'findbugsXml.xml'):
            try:
                tree = ET.parse(f)
                root = tree.getroot()
                bugs = len(root.findall('.//BugInstance'))
                if bugs > 0:
                    name = root.find('./Project').attrib['projectName']
                    yield (name, bugs)
            except Exception as e:
                    LOG.warning('[test-patch] Failed to parse the findbugs results from %s, reason: %s' % (f, e))

    def parse_failed_test(self):
        for f in find_files('.', 'TEST*.xml'):
            try:
                tree = ET.parse(f)
                root = tree.getroot()
                failures = int(root.attrib['failures'])
                errors = int(root.attrib['errors'])
                name = root.attrib['name']
                if failures > 0 or errors > 0:
                    yield name
            except Exception as e:
                LOG.warning('[test-patch] Failed to parse the result %s, reason: %s' % (f, e))
                continue

    def run(self):
        ret = self.validate()
        if ret != 0:
            self.out.write('The validation of dependency or licenses have failed.\n')
            return ret

        ret = self.compile()
        if ret != 0:
            self.out.write('The compilation has failed.\n')
            return ret

        if self.compilationWarnings > 0:
            self.out.write('The compiler has generated %d warnings.\n\n' % self.compilationWarnings)
            return -1

        ret = self.checkstyle()
        if ret != 0:
            self.out.write('The validation of checkstyles have failed.\n')
            return ret

        if self.run_test() != 0:
            self.out.write('There are failures when running the tests.\n')

        self.failedTests = list(self.parse_failed_test())
        if len(self.failedTests) == 0:
            self.out.write('There are no test failures.\n')
        else:
            self.out.write('The following tests have failed:\n')
            for test in self.failedTests:
                self.out.write('  %s\n' % test)

        if self.run_findbugs() != 0:
            self.out.write('There are failures when running findbugs.\n')
        else:
            self.findbugsWarnings = list(self.parse_findbugs())
            if len(self.findbugsWarnings) == 0:
                self.out.write('No findbugs warnings has been found.\n')
            else:
                self.out.write('Findbugs has failed.\n')
                for name, bugs in self.findbugsWarnings:
                    self.out.write('  Project %s has %s findbugs warnings.\n' % (name, bugs))

        if self.compilationWarnings > 0 or len(self.failedTests) > 0 or len(self.findbugsWarnings) > 0:
            return -1
        else:
            return 0

try:
  os.mkdir(TARGET_DIR)
except:
  pass

runner = TestPatchRunner(sys.stdout)
ret = runner.run()
sys.exit(ret)
