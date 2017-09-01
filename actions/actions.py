#!/usr/bin/python
#
# Copyright 2016 Canonical Ltd
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

import argparse
import os
import sys
import yaml

sys.path.append('hooks/')

from charmhelpers.core.host import service_pause, service_resume
from charmhelpers.core.hookenv import (
    action_fail,
    action_get,
    action_set,
    WARNING
)
from charmhelpers.contrib.openstack.utils import (
    set_unit_paused,
    clear_unit_paused,
)
from charmhelpers.contrib.hahelpers.cluster import is_elected_leader
from lib.swift_utils import (
    assess_status,
    services,
    get_replicas,
    set_replicas,
    balance_rings,
    SWIFT_RINGS,
    SWIFT_HA_RES,
    sync_builders_and_rings_if_changed,
    SwiftProxyCharmException
)
from swift_hooks import CONFIGS


def get_action_parser(actions_yaml_path, action_name,
                      get_services=services):
    """Make an argparse.ArgumentParser seeded from actions.yaml definitions."""
    with open(actions_yaml_path) as fh:
        doc = yaml.load(fh)[action_name]["description"]
    parser = argparse.ArgumentParser(description=doc)
    parser.add_argument("--services", default=get_services())
    # TODO: Add arguments for params defined in the actions.yaml
    return parser


# NOTE(ajkavangh) - swift-proxy has been written with a pause that predates the
# enhanced pause-resume, and allowsa --services argument to be passed to
# control the services that are stopped/started.  Thus, not knowing if changing
# this will break other code, the bulk of this custom code has been retained.

def pause(args):
    """Pause all the swift services.

    @raises Exception if any services fail to stop
    """
    for service in args.services:
        stopped = service_pause(service)
        if not stopped:
            raise Exception("{} didn't stop cleanly.".format(service))
    set_unit_paused()
    assess_status(CONFIGS, args.services)


def resume(args):
    """Resume all the swift services.

    @raises Exception if any services fail to start
    """
    for service in args.services:
        started = service_resume(service)
        if not started:
            raise Exception("{} didn't start cleanly.".format(service))
    clear_unit_paused()
    assess_status(CONFIGS, args.services)


def _update_replicas(ring, replicas):
    balance_required = False
    path = SWIFT_RINGS[ring]
    if not os.path.exists(path):
        action_fail("Swift ring file {}"
                    "missing from unit.".format(ring))

    try:
        current_replicas = float(get_replicas(path))
    except:
        action_fail("Current replicas not able to be retrieved"
                    "from {}".format(path))
        return

    try:
        requested_replicas = float(replicas)
    except:
        action_fail("Requested replicas is not a floating point"
                    "number: {}".format(replicas))

    if current_replicas == requested_replicas:
        action_set({ring: "Replicas already set to "
                          "{}".format(replicas)})
    else:
        try:
            set_replicas(path, replicas)
            action_set({ring: "Replicas updated to "
                              "{}".format(replicas)})
        except SwiftProxyCharmException as exc:
            action_fail("Failed replica update on {}\n"
                        "{}".format(ring, str(exc)),
                        level=WARNING)
        else:
            balance_required = True

    return balance_required


@sync_builders_and_rings_if_changed
def update_replicas(args):
    """Sets number of replicas in the builder file(s) as specified
    and triggers rebalance and sync of builder/ring files
    """
    if not is_elected_leader(SWIFT_HA_RES):
        action_fail("This Unit is not the leader. "
                    "Must be run on the leader.\n"
                    "Suggest using <juju run 'is-leader'>"
                    "to determine proper unit")
        return

    replicas = action_get("replicas")
    ring = (action_get("ring")).lower()

    if replicas < 1:
        action_fail("Failing for data safety."
                    " Must specify minimum of 1 replica!")
        return

    if ring == 'all':
        if all([os.path.exists(p) for p in SWIFT_RINGS.itervalues()]):
            for curr_ring in SWIFT_RINGS.iterkeys():
                balance_required = _update_replicas(curr_ring, replicas)
        else:
            action_fail("One or more swift ring files missing from unit.")
    else:
        if ring in SWIFT_RINGS:
            balance_required = _update_replicas(ring, replicas)
        else:
            action_fail("Ring {} unknown to swift-proxy".format(ring))

    if balance_required:
        balance_rings()

    return


# A dictionary of all the defined actions to callables (which take
# parsed arguments).
ACTIONS = {"pause": pause, "resume": resume,
           "update-replicas": update_replicas}


def main(argv):
    action_name = _get_action_name()
    actions_yaml_path = _get_actions_yaml_path()
    parser = get_action_parser(actions_yaml_path, action_name)
    args = parser.parse_args(argv)
    try:
        action = ACTIONS[action_name]
    except KeyError:
        return "Action %s undefined" % action_name
    else:
        try:
            action(args)
        except Exception as e:
            action_fail(str(e))


def _get_action_name():
    """Return the name of the action."""
    return os.path.basename(__file__)


def _get_actions_yaml_path():
    """Return the path to actions.yaml"""
    cwd = os.path.dirname(__file__)
    return os.path.join(cwd, "..", "actions.yaml")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
