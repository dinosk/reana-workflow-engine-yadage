# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2018 CERN.
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

import pika

from .config import BROKER_PASS, BROKER_PORT, BROKER_URL, BROKER_USER


def publish_workflow_status(workflow_uuid, status, message=None, logs=None):
    """Update database workflow status.

    :param workflow_uuid: UUID which represents the workflow.
    :param status: String that represents the analysis status.
    :param status_message: String that represents the message related with the
       status, if there is any.
    """

    broker_credentials = pika.PlainCredentials(BROKER_USER,
                                               BROKER_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(BROKER_URL,
                                  BROKER_PORT,
                                  '/',
                                  broker_credentials))
    channel = connection.channel()
    channel.queue_declare(queue='jobs-status')
    channel.basic_publish(exchange='',
                          routing_key='jobs-status',
                          body=json.dumps({"workflow_uuid": workflow_uuid,
                                           "status": status,
                                           "logs": logs,
                                           "message": message}),
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # msg persistent
                          ))
