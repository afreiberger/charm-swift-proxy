#!/usr/bin/env python3
#
# Copyright 2018 Canonical Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

_path = os.path.dirname(os.path.realpath(__file__))
_parent = os.path.abspath(os.path.join(_path, '..'))


def _add_path(path):
    if path not in sys.path:
        sys.path.insert(1, path)


_add_path(_parent)

from subprocess import (
    check_call,
    CalledProcessError
)

from charmhelpers.core.hookenv import (
    action_get,
    action_set,
    action_fail,
)

from lib.swift_utils import (
    sync_builders_and_rings_if_changed,
    SWIFT_HA_RES,

)

from charmhelpers.contrib.hahelpers.cluster import (
    is_elected_leader,
)

from yaml import load as YAMLload

ALL_RINGS = ['object', 'container', 'account']


def validate_descriptors(ring, spec):
    valid = True
    check_rings = [ring]
    if ring == 'all':
        check_rings = ALL_RINGS
    for r in check_rings:
        for action, ops in spec.iteritems():
            for opdata in ops:
                cmd = ['swift-object-builder',
                       "/etc/swift/{}.builder".format(r),
                       'search', opdata['descriptor']]
                try:
                    check_call(cmd)
                except CalledProcessError:
                    valid = False
                    action_set({'message': "Spec descriptor {} was not found"
                                "in ring {}".format(opdata['descriptor'],
                                                    ring)})
    return valid


def validate_spec(spec):
    """ Validate that change-spec for the action matches expected format
        Example:
        remove:
          - descriptor: 'd99'
          - descriptor: 'd100'
          - descriptor: '<any search device supported by swift-ring-builder>'
        set_weight:
          - descriptor: 'd101'
            weight: 1.0
          - descriptor: 'd102'
            weight: 0.5
          - descriptor: '<any search device>'
            weight: 0.0
    """
    valid_actions = ['remove', 'set_weight']
    required_args = {'remove': ['descriptor'],
                     'set_weight': ['descriptor', 'weight']}
    argtypes = {'descriptor': 'string', 'weight': 'float'}
    valid = True
    # Check that all hooks
    for action, ops in spec.iteritems():
        if action in valid_actions:
            for op in ops:
                if cmp(op.items(), required_args[action]) != 0:
                    valid = False
                    action_set({'message': "Spec for action {}, operation {} "
                                "does not match required fields {}"
                                "".format(action, op, required_args[action])})
                for arg in required_args[action]:
                    try:
                        eval("{}(op[arg])".format(argtypes[arg]))
                    except Exception:
                        valid = False
                        action_set({'message': "Spec for action {}, operation "
                                    "{} has arg {} which is not of type {}"
                                    "".format(action, op, arg, argtypes[arg])})
    return valid


def update_ring(ring, action, **kwargs):
    success = True
    cmd = ['swift-ring-builder', "/etc/swift/{}.builder".format(ring), action]
    if action == 'remove':
        cmd.append(kwargs['descriptor'])
    elif action == 'set_weight':
        cmd.append(kwargs['descriptor'], kwargs['weight'])
    try:
        check_call(cmd)
    except CalledProcessError as e:
        success = False
        action_set({'message': "Failed to update ring with cmd: {}\nError: {}"
                    "".format(cmd, e)})
    return success


@sync_builders_and_rings_if_changed
def manage_rings():
    """Parse YAML of actions to perform on rings, then
    trigger a ring rebalance if successful."""

    if not is_elected_leader(SWIFT_HA_RES):
        action_fail('Must run action on leader unit')
        return

    change_spec = YAMLload(action_get('change-spec'))
    ring = action_get('ring')

    # Ordered validation section to fail fast and perform deeper checks later
    # all validation must be done before the rings are changed or partial
    # changes would trigger a ring sync
    valid_rings = ALL_RINGS
    valid_rings.append('all')

    if ring not in valid_rings:
            action_fail('Invalid ring specified')
    if not validate_spec(change_spec):
            action_fail('Invalid change-spec')
    if not validate_descriptors(ring, change_spec):
            action_fail('Some descriptor(s) in change-spec not found')

    rings = [ring]
    if ring == 'all':
        rings = ALL_RINGS
    for r in rings:
        for action, ops in change_spec.iteritems():
            for opdata in ops:
                update_ring(ring, action, opdata)


if __name__ == '__main__':
    manage_rings()
