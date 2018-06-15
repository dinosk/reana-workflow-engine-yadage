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

import json
import logging
import datetime
import pika
from .utils import publish_workflow_status

from yadage.utils import WithJsonRefEncoder

log = logging.getLogger(__name__)


def publish_workflow_status(channel, workflow_uuid, status,
                            logs='',
                            message=None):
    """Update database workflow status.
    :param workflow_uuid: UUID which represents the workflow.
    :param status: String that represents the analysis status.
    :param status_message: String that represents the message related with the
       status, if there is any.
    """
    log.info('Publishing Workflow: {0} Status: {1}'.
             format(workflow_uuid, status))
    channel.basic_publish(exchange='',
                          routing_key='jobs-status',
                          body=json.dumps({"workflow_uuid": workflow_uuid,
                                           "logs": logs,
                                           "status": status,
                                           "message": message}),
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # msg persistent
                          ))

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
        json_message = {
            'progress': {
                'planned': 3,
                'submitted': 2,
                'succeeded': 1,
                'failed': 0
            },
            'structure': {
                'type': 'yadage',
                'graph': {
                    'nodes': [
                        {'nodeid': '1234', 'metadata': {'name': 'selection'}, 'jobid': None},
                        {'nodeid': '9876', 'metadata': {'name': 'fitting'}, 'jobid': 'job-12345'}
                    ],
                    'egdes': [
                        {'from': '1234', 'to': '9876'}
                    ]
                }
            }
        }
        log_message = 'this is a tracking log at {}'.format(
            datetime.datetime.now().isoformat()
        )

        log.info('''sending to REANA
uuid: {}
json:
{}
message:
{}
'''.format(self.workflow_id, json.dumps(json_message, indent=4), log_message))
        publish_workflow_status(self.workflow_id, status = 2, message = log_message)

    def finalize(self, adageobj):
        self.track(adageobj)
