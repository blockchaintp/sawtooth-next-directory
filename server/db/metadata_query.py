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


LOGGER = logging.getLogger(__name__)


async def fetch_all_metadata(conn, user_id):
    all_metadata = await r.table('metadata')\
        .get_all(user_id, index='user_id')\
        .coerce_to('array').run(conn)

    all_metadata_dict = {}

    for kv_pair in all_metadata:
        key = kv_pair['key']
        value = kv_pair['value']
        all_metadata_dict[key] = value

    return all_metadata_dict


async def fetch_metadata(conn, user_id, key):
    metadata = await fetch_metadata_raw(conn, user_id, key)

    if not metadata:
        return {}

    return metadata['value']


async def fetch_metadata_raw(conn, user_id, key):
    metadata = await r.table('metadata')\
        .get_all(user_id, index='user_id')\
        .filter({'key': key})\
        .coerce_to('array').run(conn)

    if not metadata:
        return {}

    return metadata[0]
