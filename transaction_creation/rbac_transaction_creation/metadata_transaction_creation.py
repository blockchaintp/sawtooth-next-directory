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

from rbac_addressing import addresser

from rbac_transaction_creation.common import make_header_and_batch
from rbac_transaction_creation.protobuf import rbac_payload_pb2
from rbac_transaction_creation.protobuf import metadata_transaction_pb2


def create_metadata(txn_key,
                    batch_key,
                    metadata_key,
                    value):
    """Create a BatchList with a CreateMetadata RBAC transaction.

    Args:
        txn_key (Key): The transaction signer's public/private key pair.
        batch_key (Key): The batch signer's public/private key pair.
        metadata_key (str): The key of the metadata.
        value (str): The value of the metadata.

    Returns:
        tuple
            BatchList, batch header_signature tuple
    """

    create_metadata_payload = metadata_transaction_pb2.CreateMetadata(
        key=metadata_key,
        value=value)
    inputs = [addresser.make_metadata_address(metadata_key=metadata_key)]
    outputs = [addresser.make_metadata_address(metadata_key=metadata_key)]

    rbac_payload = rbac_payload_pb2.RBACPayload(
        content=create_metadata_payload.SerializeToString(),
        message_type=rbac_payload_pb2.RBACPayload.CREATE_METADATA)

    return make_header_and_batch(
        rbac_payload,
        inputs,
        outputs,
        txn_key,
        batch_key)
