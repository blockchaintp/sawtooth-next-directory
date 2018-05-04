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

import rethinkdb as r

from db import metadata_query


LOGGER = logging.getLogger(__name__)


async def insert_all_metadata(conn, user_id, metadata):
    for key, value in metadata.items():
        await insert_metadata(conn, user_id, key, value)


async def insert_metadata(conn, user_id, key, value):
    existing_metadata = await metadata_query.fetch_metadata_raw(conn, user_id, key)

    if not existing_metadata:
        await insert_new_metadata(conn, user_id, key, value)
    else:
        await update_existing_metadata(conn, value, existing_metadata)


async def insert_new_metadata(conn, user_id, key, value):
    metadata_dict = \
        {
            "user_id": user_id,
            "key": key,
            "value": value
        }

    await r.table('metadata')\
        .insert(metadata_dict)\
        .run(conn)


async def update_existing_metadata(conn, value, existing_metadata):
    uuid = existing_metadata["id"]

    await r.table('metadata')\
        .get(uuid)\
        .update({'value': value})\
        .run(conn)


async def delete_metadata(conn, user_id, key):
    existing_metadata = await metadata_query.fetch_metadata_raw(conn, user_id, key)

    if not existing_metadata:
        return {}

    uuid = existing_metadata["id"]

    await r.table('metadata')\
        .get(uuid)\
        .delete()\
        .run(conn)
