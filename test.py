#!/usr/bin/env python

import func
import oci
import json

config = oci.config.from_file()
oci.config.validate_config(config)
compartments = func.list_compartments(config=config)  # function defined below
for compartment in compartments["compartments"]:
  print("%s: %s" % (compartment["path"], compartment["id"]))

found_compartment = None
for compartment in compartments["compartments"]:
  if compartment["path"] == "enap/cmp-tst":
    found_compartment = compartment
    break

clusters = func.list_oke_clusters(compartment["id"], config=config)
for cluster in clusters["clusters"]:
  print("%s: %s" % (cluster["name"], cluster["id"]))

for cluster in clusters["clusters"]:
  nodepools = func.list_oke_node_pools(compartment["id"], cluster["id"], config=config)
  for nodepool in nodepools["nodepools"]:
    print("%s: %s %d" % (nodepool["name"], nodepool["id"], nodepool["size"]))

n = func.get_oke_node_pool("ocid1.nodepool.oc19.eu-frankfurt-2.aaaaaaaarjzfwwc3qunzfuuvltvoogdpdv7begsmoebhusywxnclft6eut5q", config=config)
nodepool = n["nodepool"]
print("%s: %s %d" % (nodepool["name"], nodepool["id"], nodepool["size"]))
