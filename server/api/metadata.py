# Copyright 2017 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ------------------------------------------------------------------------------

import logging

from api import utils
from api.auth import authorized

from rbac_transaction_creation import metadata_transaction_creation

from sanic import Blueprint
from sanic.response import json

LOGGER = logging.getLogger(__name__)
METADATA_BP = Blueprint('metadata')


@METADATA_BP.post('api/metadata')
@authorized()
async def create_new_metadata(request):
    required_fields = ['key', 'value']
    utils.validate_fields(required_fields, request.json)

    txn_key = await utils.get_transactor_key(request)
    metadata_key = request.json.get('key')
    batch_list = metadata_transaction_creation.create_metadata(
        txn_key,
        request.app.config.BATCHER_KEY_PAIR,
        metadata_key,
        request.json.get('value')
    )

    await utils.send(
        request.app.config.VAL_CONN, batch_list[0], request.app.config.TIMEOUT
    )
    return json({'key': metadata_key})
