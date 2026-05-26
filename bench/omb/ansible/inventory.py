#!/usr/bin/env python3
"""Dynamic Ansible inventory generated from Terraform outputs.

Calls `terraform output -json` in `bench/omb/terraform/gcp/` and emits
an inventory with three groups: `kafka`, `crabka`, `client`. Hosts
expose `public_ip`, `private_ip`, and an `ansible_host` set to the
public IP so we can SSH from the operator's laptop.

Ansible spec for dynamic inventories:
  https://docs.ansible.com/ansible/latest/dev_guide/developing_inventory.html
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
TF_DIR = HERE.parent / "terraform" / "gcp"


def tf_outputs() -> dict:
    res = subprocess.run(
        ["terraform", "-chdir", str(TF_DIR), "output", "-json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(res.stdout)


def host_entry(vm: dict, ssh_user: str) -> tuple[str, dict]:
    return vm["name"], {
        "ansible_host": vm["public_ip"],
        "ansible_user": ssh_user,
        "ansible_python_interpreter": "/usr/bin/python3",
        "public_ip": vm["public_ip"],
        "private_ip": vm["private_ip"],
    }


def build_inventory() -> dict:
    outs = tf_outputs()
    ssh_user = outs["ssh_user"]["value"]

    inv: dict = {
        "_meta": {"hostvars": {}},
        "all": {"children": ["kafka", "crabka", "client"]},
        "kafka": {"hosts": [], "vars": {}},
        "crabka": {"hosts": [], "vars": {}},
        "client": {"hosts": [], "vars": {}},
    }

    for group_key, tf_key in [
        ("kafka", "kafka_brokers"),
        ("crabka", "crabka_brokers"),
        ("client", "clients"),
    ]:
        for vm in outs[tf_key]["value"]:
            name, hv = host_entry(vm, ssh_user)
            inv["_meta"]["hostvars"][name] = hv
            inv[group_key]["hosts"].append(name)

    inv["kafka"]["vars"]["bootstrap_servers"] = outs["kafka_bootstrap_servers"]["value"]
    inv["crabka"]["vars"]["bootstrap_servers"] = outs["crabka_bootstrap_servers"]["value"]

    return inv


def main() -> None:
    if "--host" in sys.argv:
        print(json.dumps({}))
        return
    print(json.dumps(build_inventory(), indent=2))


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        sys.stderr.write(
            f"terraform output failed in {TF_DIR}: {e.stderr or e}\n"
            f"Have you run `bench/omb/scripts/tf-apply.sh` yet?\n"
        )
        sys.exit(2)
