# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2017 CERN.
#
# REANA is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# REANA is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# REANA; if not, write to the Free Software Foundation, Inc., 59 Temple Place,
# Suite 330, Boston, MA 02111-1307, USA.
#
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization or
# submit itself to any jurisdiction.

import datetime
import json
import logging

import jq
import networkx as nx
import adage.nodestate as nodestate
import adage.dagstate as dagstate
from yadage.utils import WithJsonRefEncoder

from .utils import publish_workflow_status

# def publish_workflow_status(*args, **kwargs):
#     pass


log = logging.getLogger(__name__)

def analyze_progress(adageobj):
    dag, rules, applied = adageobj.dag, adageobj.rules, adageobj.applied_rules
    successful, failed, running, unsubmittable = 0, 0, 0, 0

    nodestates = []
    for node in nx.topological_sort(dag):
        nodeobj = dag.getNode(node)
        if nodeobj.state == nodestate.RUNNING:
            nodestates.append(
                {'state': 'running', 'job_id': nodeobj.proxy.job_id}
            )
        elif dagstate.node_status(nodeobj):
            nodestates.append(
                {'state': 'succeeded', 'job_id': nodeobj.proxy.job_id}
            )
        elif dagstate.node_ran_and_failed(nodeobj):
            nodestates.append(
                {'state': 'failed', 'job_id': nodeobj.proxy.job_id}
            )
        elif dagstate.upstream_failure(dag,nodeobj):
            nodestates.append(
                {'state': 'unsubmittable', 'job_id': None}
            )
        else:
            nodestates.append(
                {'state': 'scheduled', 'job_id': None}
            )
    return nodestates

class REANATracker(object):

    def __init__(self, identifier = None):
        self.workflow_id = identifier
        log.info('initializing REANA workflow tracker for id {}'.format(self.workflow_id))

    def initialize(self, adageobj):
        self.track(adageobj)

    def track(self, adageobj):
        log.info('sending progress information')
        serialized = json.dumps(adageobj.json(), cls=WithJsonRefEncoder,
                                sort_keys=True)
        purejson = json.loads(serialized)

        progress = {
            "engine_specific": None,
            "failed": {"total": 0, "job_ids": []},
            "planned": {"total": 0, "job_ids": []},
            "submitted": {"total": 0, "job_ids": []},
            "succeeded": {"total": 0, "job_ids": []}
        }


        progress['engine_specific'] = jq.jq('{dag: {edges: .dag.edges, nodes: [.dag.nodes[]|{metadata: {name: .task.metadata.name}, id: .id, jobid: .proxy.proxydetails.job_id}]}}').transform(
            purejson
        )

        for node in analyze_progress(adageobj):
            key = {
                'running': 'submitted',
                'succeeded': 'succeeded',
                'failed': 'failed',
                'unsubmittable': 'planned',
                'scheduled': 'planned',
            }[node['state']]
            progress[key]['total'] += 1
            if key in ['submitted','succeeded','failed']:
                progress[key]['job_ids'].append(node['job_id'])

        log_message = 'this is a tracking log at {}'.format(
            datetime.datetime.now().isoformat()
        )

        log.info('''sending to REANA
uuid: {}
json:
{}
message:
{}
'''.format(self.workflow_id, json.dumps(progress, indent=4), log_message))
        publish_workflow_status(self.workflow_id, status = 2, message = progress, logs = log_message)

    def finalize(self, adageobj):
        self.track(adageobj)
