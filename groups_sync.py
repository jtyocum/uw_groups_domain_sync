"""
Tool to sync users from a local system to UW Groups.
"""

import sys
import os
import yaml
import requests
import subprocess
import re
import time


def get_uw_group_members(
    gws_base_url: str,
    gws_ca_cert: str,
    gws_client_cert: str,
    gws_client_key: str,
    uw_group: str,
) -> list:
    """
    Get UW group membership list from Groups Web Service.
    """

    r = requests.get(
        gws_base_url + "/group/" + uw_group + "/member",
        verify=gws_ca_cert,
        cert=(gws_client_cert, gws_client_key),
    )

    group_members = []

    for member in r.json()["data"]:
        if member["type"] == "uwnetid":
            # Verify personal NetID
            # https://wiki.cac.washington.edu/pages/viewpage.action?spaceKey=infra&title=UW+NetID+Namespace
            if re.match("^[a-z][a-z0-9]{0,7}$", member["id"]):
                group_members.append(member["id"])

    return group_members


def get_local_group_members(local_group: str) -> list:
    """
    Get local group membership via NSS.
    """

    r = subprocess.run(["getent", "group", local_group], capture_output=True, text=True)
    members = r.stdout.strip().split(":")[3].split(",")
    return members


def add_uw_group_members(
    gws_base_url: str,
    gws_ca_cert: str,
    gws_client_cert: str,
    gws_client_key: str,
    uw_group: str,
    members: list,
) -> int:

    group_members = ",".join(members)

    r = requests.put(
        gws_base_url + "/group/" + uw_group + "/member/" + group_members,
        verify=gws_ca_cert,
        cert=(gws_client_cert, gws_client_key),
    )

    return r.status_code


def remove_uw_group_members(
    gws_base_url: str,
    gws_ca_cert: str,
    gws_client_cert: str,
    gws_client_key: str,
    uw_group: str,
    members: list,
) -> bool:

    group_members = ",".join(members)

    r = requests.delete(
        gws_base_url + "/group/" + uw_group + "/member/" + group_members,
        verify=gws_ca_cert,
        cert=(gws_client_cert, gws_client_key),
    )

    return r.status_code


def main():
    conf_path = os.path.dirname(os.path.abspath(__file__)) + r"/conf/groups_sync.yml"
    config = yaml.load(open(conf_path, "r"), Loader=yaml.SafeLoader)

    # Group Web Service base URL
    # Use API v3, https://wiki.cac.washington.edu/display/infra/Groups+Service+API+v3
    gws_base_url = config["gws_base_url"]
    # GWS requires certificate based auth
    gws_ca_cert = config["gws_ca_cert"]
    gws_client_cert = config["gws_client_cert"]
    gws_client_key = config["gws_client_key"]
    # key (Uw group) = value (local group)
    group_map = config["group_map"]

    for uw_group, local_group in group_map.items():
        add_members = []
        remove_members = []

        try:
            uw_group_member_list = get_uw_group_members(
                gws_base_url, gws_ca_cert, gws_client_cert, gws_client_key, uw_group
            )
            local_group_member_list = get_local_group_members(local_group)
        except Exception:
            print("FATAL: Error retrieving group members?", sys.exc_info())
            sys.exit(1)

        if set(local_group_member_list) != set(uw_group_member_list):
            for member in local_group_member_list:
                if member not in uw_group_member_list:
                    add_members.append(member)

            for member in uw_group_member_list:
                if member not in local_group_member_list:
                    remove_members.append(member)

            # Split up changes into batches per API docs
            chunk_size = 50
            for i in range(0, len(add_members), chunk_size):
                try:
                    status_code = add_uw_group_members(
                        gws_base_url,
                        gws_ca_cert,
                        gws_client_cert,
                        gws_client_key,
                        uw_group,
                        add_members[i : i + chunk_size],
                    )
                    print(
                        "STATUS: ADD ({0}, {1}) CHUNK ({2}, {3}) {4}".format(
                            uw_group, local_group, i, i + chunk_size, status_code
                        )
                    )
                except Exception:
                    print("FATAL: Error adding members?", sys.exc_info())
                    sys.exit(1)

                # Small delay to reduce load during large sync operations
                time.sleep(1)

            for i in range(0, len(remove_members), chunk_size):
                try:
                    status_code = remove_uw_group_members(
                        gws_base_url,
                        gws_ca_cert,
                        gws_client_cert,
                        gws_client_key,
                        uw_group,
                        remove_members[i : i + chunk_size],
                    )
                    print(
                        "STATUS: REMOVE ({0}, {1}) CHUNK ({2}, {3}) {4}".format(
                            uw_group, local_group, i, i + chunk_size, status_code
                        )
                    )
                except Exception:
                    print("FATAL: Error removing members?", sys.exc_info())
                    sys.exit(1)

                # Small delay to reduce load during large sync operations
                time.sleep(1)

        print(
            "UWGROUP: {} LGROUP: {} ADD: {} REM: {}".format(
                uw_group, local_group, len(add_members), len(remove_members)
            )
        )

    return


if __name__ == "__main__":
    main()
