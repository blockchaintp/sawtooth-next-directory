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
# -----------------------------------------------------------------------------

from sawtooth_sdk.protobuf import state_context_pb2
from sawtooth_sdk.processor.exceptions import InvalidTransaction

from rbac_addressing import addresser
from rbac_processor.common import is_in_metadata_container
from rbac_processor.common import return_metadata_container
from rbac_processor.protobuf import metadata_state_pb2
from rbac_processor.protobuf import metadata_transaction_pb2
from rbac_processor.state import get_state
from rbac_processor.state import set_state


def apply_create_metadata(header, payload, state):
    create_metadata = metadata_transaction_pb2.CreateMetadata()
    create_metadata.ParseFromString(payload.content)

    if len(create_metadata.key) < 8:
        raise InvalidTransaction(
            "CreateMetadata txn with key '{}' is invalid. Metadata "
            "must have keys longer than 7 characters.".format(
                create_metadata.key))

    if not create_metadata.value:
        raise InvalidTransaction(
            "CreateMetadata txn with value '{}' is invalid. Metadata "
            "must have non-empty values.".format(
                create_metadata.value))

    validate_metadata_state(header, create_metadata, state)

    handle_metadata_state_set(header, create_metadata, state)


def validate_metadata_state(header, create_metadata, state):
    metadata_entries = get_state(
        state,
        [addresser.make_metadata_address(create_metadata.key)])
    if metadata_entries:
        # this is necessary for state collisions.
        try:
            metadata_container = return_metadata_container(metadata_entries[0])

            if is_in_metadata_container(metadata_container, create_metadata.key):
                raise InvalidTransaction(
                    "Metadata with key '{}' already exists.".format(
                        create_metadata.key))
        except KeyError:
            # The metadata does not exist yet in state and so the transaction
            # is valid.
            pass


def handle_metadata_state_set(header, create_metadata, state):
    metadata_container = metadata_state_pb2.MetadataContainer()
    metadata = metadata_state_pb2.Metadata(
        key=create_metadata.key,
        value=create_metadata.value)

    metadata_container.metadata.extend([metadata])

    set_state(state, {
        addresser.make_metadata_address(create_metadata.key):
            metadata_container.SerializeToString()
    })
