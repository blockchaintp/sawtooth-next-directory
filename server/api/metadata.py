# Copyright 2018 Blockchain Technology Partners
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

from api.auth import authorized
from api.errors import ApiNotFound

from db import metadata_manipulation, metadata_query

from sanic import Blueprint
from sanic.response import json

LOGGER = logging.getLogger(__name__)
METADATA_BP = Blueprint('metadata')


@METADATA_BP.post('api/users/<user_id>/metadata')
@authorized()
async def create_all_metadata(request, user_id):
    await metadata_manipulation.insert_all_metadata(
        request.app.config.DB_CONN,
        user_id,
        request.json
    )

    return await get_all_metadata(request, user_id)


@METADATA_BP.get('api/users/<user_id>/metadata')
@authorized()
async def get_all_metadata(request, user_id):
    all_metadata = await metadata_query.fetch_all_metadata(
        request.app.config.DB_CONN,
        user_id
    )

    if not all_metadata:
        raise ApiNotFound(
            "Not Found: No user with the id '{}' exists".format(user_id)
        )

    return json(all_metadata)


@METADATA_BP.get('api/users/<user_id>/metadata/<key>')
@authorized()
async def get_metadata(request, user_id, key):
    value = await metadata_query.fetch_metadata(
        request.app.config.DB_CONN,
        user_id,
        key
    )

    if not value:
        raise ApiNotFound(
            "Not Found: No key '{}' for a user with the id '{}' exists".format(key, user_id)
        )

    return json(value)


@METADATA_BP.put('api/users/<user_id>/metadata/<key>')
@authorized()
async def add_metadata(request, user_id, key):
    await metadata_manipulation.insert_metadata(
        request.app.config.DB_CONN,
        user_id,
        key,
        request.json
    )

    return json("", status=204)


@METADATA_BP.delete('api/users/<user_id>/metadata/<key>')
@authorized()
async def delete_metadata(request, user_id, key):
    await metadata_manipulation.delete_metadata(
        request.app.config.DB_CONN,
        user_id,
        key
    )

    return json("", status=204)
