# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2017, 2018 CERN.
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

from __future__ import absolute_import, print_function

import json
import logging
import os

import zmq
from yadage.steering_api import steering_ctx
from yadage.utils import setupbackend_fromstring

from .celeryapp import app
from .config import (CODE_DIRECTORY_RELATIVE_PATH,
                     INPUTS_DIRECTORY_RELATIVE_PATH,
                     LOGS_DIRECTORY_RELATIVE_PATH,
                     OUTPUTS_DIRECTORY_RELATIVE_PATH, SHARED_VOLUME_PATH,
                     YADAGE_INPUTS_DIRECTORY_RELATIVE_PATH)
from .tracker import REANATracker
from .utils import publish_workflow_status

log = logging.getLogger(__name__)

known_dirs = [
    CODE_DIRECTORY_RELATIVE_PATH,
    INPUTS_DIRECTORY_RELATIVE_PATH,
    LOGS_DIRECTORY_RELATIVE_PATH,
    OUTPUTS_DIRECTORY_RELATIVE_PATH,
    YADAGE_INPUTS_DIRECTORY_RELATIVE_PATH,
]


@app.task(name='tasks.run_yadage_workflow', ignore_result=True)
def run_yadage_workflow(workflow_uuid, workflow_workspace,
                        workflow=None, workflow_json=None,
                        toplevel=os.getcwd(), parameters=None):
    log.info('getting socket..')

    workflow_workspace = '{0}/{1}'.format(SHARED_VOLUME_PATH,
                                          workflow_workspace)
    app.conf.update(WORKFLOW_UUID=workflow_uuid)

    cap_backend = setupbackend_fromstring('fromenv')

    if workflow_json:
        # When `yadage` is launched using an already validated workflow file.
        workflow_kwargs = dict(workflow_json=workflow_json)
    elif workflow:
        # When `yadage` resolves the workflow file from a remote repository:
        # i.e. github:reanahub/reana-demo-root6-roofit/workflow.yaml
        workflow_kwargs = dict(workflow=workflow, toplevel=toplevel)

    # Since we need code and input data accessible from `cmd` section on yadage
    # we will copy `INPUTS_DIRECTORY_RELATIVE_PATH` and
    # `CODE_DIRECTORY_RELATIVE_PATH` inside the configured
    # `YADAGE_INPUTS_DIRECTORY_RELATIVE_PATH`
    # Remove once `yadage` accepts multiple `initdir`.
    absolute_yadage_inputs_directory_path = os.path.join(
        workflow_workspace, '..',
        YADAGE_INPUTS_DIRECTORY_RELATIVE_PATH)
    log.info('Creating {0}'.format(absolute_yadage_inputs_directory_path))
    os.makedirs(absolute_yadage_inputs_directory_path)
    absolute_inputs_directory_path = os.path.join(
        workflow_workspace, '..',
        INPUTS_DIRECTORY_RELATIVE_PATH)
    absolute_inputs_directory_path = os.path.join(
        workflow_workspace, '..',
        INPUTS_DIRECTORY_RELATIVE_PATH)
    absolute_code_directory_path = os.path.join(
        workflow_workspace, '..',
        CODE_DIRECTORY_RELATIVE_PATH)
    log.info('Copying {source} to {dest}.'.format(
        source=absolute_inputs_directory_path,
        dest=absolute_yadage_inputs_directory_path))
    os.system('cp -R {source} {dest}'.format(
        source=absolute_inputs_directory_path,
        dest=absolute_yadage_inputs_directory_path))
    log.info('Copied {source} to {dest}.'.format(
        source=absolute_code_directory_path,
        dest=absolute_yadage_inputs_directory_path))
    os.system('cp -R {source} {dest}'.format(
        source=absolute_code_directory_path,
        dest=absolute_yadage_inputs_directory_path))
    # Set `workflow_workspace/yadage_inputs_directory_relative_path` as the
    # input directory
    dataopts = {'initdir': absolute_yadage_inputs_directory_path}

    try:
        with steering_ctx(dataarg=workflow_workspace,
                          dataopts=dataopts,
                          initdata=parameters if parameters else {},
                          visualize=False,
                          updateinterval=5,
                          loginterval=5,
                          backend=cap_backend,
                          **workflow_kwargs) as ys:

            log.info('running workflow on context: {0}'.format(locals()))
            publish_workflow_status(workflow_uuid, 1)

            ys.adage_argument(additional_trackers=[
                REANATracker(identifier=workflow_uuid)])

        publish_workflow_status(workflow_uuid, 2)

        log.info('workflow done')
    except Exception as e:
        log.info('workflow failed: {0}'.format(e))
        publish_workflow_status(workflow_uuid, 3)

    finally:
        yadage_workflow_workspace_content = \
            os.path.join(workflow_workspace, '*')
        absolute_outputs_directory_path = os.path.join(
            workflow_workspace, '..', OUTPUTS_DIRECTORY_RELATIVE_PATH)
        # Remove outputs directory since `shutil.copytree` needs an empty
        # dst directory.
        log.info('Copying {source} to {dest}.'.format(
            source=yadage_workflow_workspace_content,
            dest=absolute_outputs_directory_path))
        os.system('cp -R {source} {dest}'.format(
            source=yadage_workflow_workspace_content,
            dest=absolute_outputs_directory_path))
        log.info('Workflow outputs copied to `/outputs` directory.')
