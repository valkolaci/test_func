#
# oci-list-compartments-python version 1.0.
#
# Copyright (c) 2020 Oracle, Inc.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl.
#

import sys
import io
import json
from fdk import response
import oci.identity

from cfg import Config

# Read and parse configuration
c = Config.read_config()
print(c.dump())

sys.exit(1)

# Handle REST calls
def handler(ctx, data: io.BytesIO = None):
    try:
      body = json.loads(data.getvalue())
      nodepool_id = body.get("nodepool_id")
      if nodepool_id is None or nodepool_id == "":
        raise "Missing nodepool_id parameter"
      size = body.get("size")
      if size is None or size == "":
        raise "Missing size parameter"
      try:
        size = int(size)
      except (ValueError) as ex:
        raise "Invalid size parameter (not an integer)"
    except (Exception, ValueError) as ex:
      print(str(ex), flush=True)

    print("Requested node pool '%s' change size: %d" % (nodepool_id, size))

    signer = oci.auth.signers.get_resource_principals_signer()
    resp = get_oke_node_pool(nodepool_id, signer=signer)
    np = resp["nodepool"]
    current = np["size"]
    print("Requested node pool current size: %d" % (current))

    if current == size:
      print("No change needed")
    else:
      print("Updating node pool '%s' to size: %d" % (nodepool_id, size))
      resp = set_oke_node_pool(nodepool_id, size, signer=signer)

    return response.Response(
        ctx,
        response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

# Calculate compartment path through parent links
def get_compartment_path(compartments_by_id, tenancy_id, c):
    name = c["name"]
    while True:
      parent_id = c["parent_id"]
      if parent_id == tenancy_id:
        break
      c = compartments_by_id[parent_id]
      name = "%s/%s" % (c["name"], name)
    return name

# List compartments
def list_compartments(config = {}, **kwargs):
    client = oci.identity.IdentityClient(config=config, **kwargs)
    # OCI API for managing users, groups, compartments, and policies
    try:
        signer = kwargs.get('signer', None)
        tenancy_id = signer.tenancy_id if signer is not None else config['tenancy'] if config is not None and 'tenancy' in config else None
        # Returns a list of all compartments and subcompartments in the tenancy (root compartment)
        compartments = client.list_compartments(
            tenancy_id,
            compartment_id_in_subtree=True,
            access_level='ANY'
        )
        compartments_by_id = dict()
        compartments_by_path = dict()
        for c in compartments.data:
          compartments_by_id[c.id] = {
            "id": c.id,
            "name": c.name,
            "parent_id": c.compartment_id
          }
        for c in compartments_by_id.values():
          path = get_compartment_path(compartments_by_id, tenancy_id, c)
          c["path"] = path
          compartments_by_path[path] = c
        compartments = [compartments_by_id[c.id] for c in compartments.data]
    except Exception as ex:
        print("ERROR: Cannot access compartments", ex, flush=True)
        raise
    resp = {"compartments": compartments}
    return resp

# List OKE clusters
def list_oke_clusters(compartment_id, config = {}, **kwargs):
    client = oci.container_engine.ContainerEngineClient(config=config, **kwargs)
    try:

        clusters = client.list_clusters(
            compartment_id
        )
        # Create a list that holds a list of the clusters id and name next to each other
        clusters = [{ "id": c.id, "name": c.name, "object": c } for c in clusters.data]
    except Exception as ex:
        print("ERROR: Cannot access clusters", ex, flush=True)
        raise
    resp = {"clusters": clusters}
    return resp

# List OKE node pools
def list_oke_node_pools(compartment_id, cluster_id, config = {}, **kwargs):
    client = oci.container_engine.ContainerEngineClient(config=config, **kwargs)
    try:

        nodepools = client.list_node_pools(
            compartment_id,
            cluster_id=cluster_id
        )
        # Create a list that holds a list of the node pool id and name next to each other
        nodepools = [{ "id": n.id, "name": n.name, "size": n.node_config_details.size, "object": n } for n in nodepools.data]
    except Exception as ex:
        print("ERROR: Cannot access node pools", ex, flush=True)
        raise
    resp = {"nodepools": nodepools}
    return resp

# Get OKE node pool
def get_oke_node_pool(nodepool_id, config = {}, **kwargs):
    client = oci.container_engine.ContainerEngineClient(config=config, **kwargs)
    try:

        n = client.get_node_pool(
            nodepool_id
        )
        # Create a list that holds a list of the node pool id and name next to each other
        nodepool = { "id": n.data.id, "name": n.data.name, "size": n.data.node_config_details.size }
    except Exception as ex:
        print("ERROR: Cannot access node pool", ex, flush=True)
        raise
    resp = {"nodepool": nodepool}
    return resp

# Set OKE node pool
def set_oke_node_pool(nodepool_id, size, config = {}, **kwargs):
    client = oci.container_engine.ContainerEngineClient(config=config, **kwargs)
    try:

        client.update_node_pool(
            nodepool_id,
            update_node_pool_details=oci.container_engine.models.UpdateNodePoolDetails(
              node_config_details=oci.container_engine.models.UpdateNodePoolNodeConfigDetails(
                size=size
              )
            )
        )
        n = client.get_node_pool(
            nodepool_id
        )
        nodepool = { "id": n.data.id, "name": n.data.name, "size": n.data.node_config_details.size }
    except Exception as ex:
        print("ERROR: Cannot access node pool", ex, flush=True)
        raise
    resp = {"nodepool": nodepool}
    return resp
