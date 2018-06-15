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
from yadage.utils import WithJsonRefEncoder

log = logging.getLogger(__name__)

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

    def finalize(self, adageobj):
        self.track(adageobj)
