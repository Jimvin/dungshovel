"""Seed a scratch ZooKeeper with znode layouts mimicking real applications.

Usage: python tests/seed_zk.py HOST:PORT [--hold SECONDS]

Ephemeral znodes disappear when the seeding session closes, so --hold keeps the
sessions open (as "live clients") while you run dungshovel against the server.
NEVER run this against a real cluster.
"""

import argparse
import json
import time
import urllib.parse

from kazoo.client import KazooClient
from kazoo.security import make_acl, make_digest_acl


def pb_field(num, value):
    if isinstance(value, int):
        out, v = bytes([num << 3 | 0]), value
        enc = bytearray()
        while True:
            b = v & 0x7F
            v >>= 7
            enc.append(b | (0x80 if v else 0))
            if not v:
                break
        return out + bytes(enc)
    value = value.encode()
    return bytes([num << 3 | 2, len(value)]) + value


def seed(hosts, hold):
    # Each "application" gets its own session so the audit sees several clients.
    kafka = KazooClient(hosts=hosts); kafka.start()
    nifi = KazooClient(hosts=hosts); nifi.start()
    dubbo = KazooClient(hosts=hosts); dubbo.start()
    admin = KazooClient(hosts=hosts, auth_data=[("digest", "hdfs:secret")]); admin.start()

    # --- Apache Kafka under a Stackable-style chroot ------------------------
    root = "/znode-0b6f3c2e-kafka"
    for p in ("brokers/topics/orders", "brokers/topics/payments", "config/topics", "admin/delete_topics",
              "isr_change_notification", "latest_producer_id_block", "consumers/legacy-group/ids"):
        kafka.ensure_path("%s/%s" % (root, p))
    kafka.create(root + "/cluster/id", json.dumps({"version": "1", "id": "q8zXuR2jQ7mUcKc1Vf3w1A"}).encode(),
                 makepath=True)
    kafka.create(root + "/controller_epoch", b"3")
    kafka.create(root + "/controller", json.dumps({"version": 2, "brokerid": 1, "timestamp": "1763300000000"}).encode(),
                 ephemeral=True)
    for bid, host in ((1, "kafka-broker-0.kafka.svc"), (2, "kafka-broker-1.kafka.svc")):
        kafka.create("%s/brokers/ids/%d" % (root, bid), json.dumps({
            "listener_security_protocol_map": {"CLIENT": "SSL", "INTERNAL": "SSL"},
            "endpoints": ["CLIENT://%s:9093" % host, "INTERNAL://%s:19093" % host],
            "jmx_port": -1, "features": {}, "host": None, "timestamp": "1763300000000",
            "port": -1, "version": 5, "rack": "eu-central-1a"}).encode(), ephemeral=True, makepath=True)
    kafka.create(root + "/config/users/alice", b'{"version":1,"config":{"SCRAM-SHA-512":"salt=...,stored_key=..."}}',
                 makepath=True)
    kafka.create(root + "/kafka-acl/Topic/orders", json.dumps({"version": 1, "acls": [
        {"principal": "User:CN=orders-service", "permissionType": "Allow", "operation": "Write", "host": "10.1.2.3"},
        {"principal": "User:alice", "permissionType": "Allow", "operation": "Read", "host": "*"}]}).encode(),
        makepath=True)
    kafka.create(root + "/consumers/legacy-group/ids/legacy-group_worker7.example.com-1700000000000-1a2b3c4d",
                 b"{}", ephemeral=True)

    # --- Apache NiFi (Curator leader election) -----------------------------
    for role in ("Cluster Coordinator", "Primary Node"):
        for i, node in enumerate(("nifi-0.nifi.svc:11443", "nifi-1.nifi.svc:11443")):
            nifi.create("/nifi/leaders/%s/_c_%08d-aaaa-bbbb-lock-%010d" % (role, i, i + 1), node.encode(),
                        ephemeral=True, makepath=True)
    nifi.create("/nifi/components/1e2f3a4b-0000-1000-8000-000000000001", b"\x00\x01state", makepath=True)

    # --- Apache Dubbo ------------------------------------------------------
    svc = "/dubbo/org.example.GreetingService"
    prov = "dubbo://10.20.0.5:20880/org.example.GreetingService?application=greeter&dubbo=2.0.2&pid=42&release=3.2.0&side=provider&timestamp=1763200000000"
    cons = "consumer://10.20.0.9/org.example.GreetingService?application=web-frontend&category=consumers&pid=77&side=consumer&timestamp=1763210000000"
    dubbo.create("%s/providers/%s" % (svc, urllib.parse.quote(prov, safe="")), b"", ephemeral=True, makepath=True)
    # Persistent consumer registration (dynamic=false): survives after the consumer left.
    dubbo.create("%s/consumers/%s" % (svc, urllib.parse.quote(cons, safe="")), b"", makepath=True)
    dubbo.ensure_path(svc + "/configurators")
    dubbo.ensure_path(svc + "/routers")

    # --- HDFS NameNode HA (protobuf breadcrumb, digest ACL) ----------------
    crumb = (pb_field(1, "hdfs-ns1") + pb_field(2, "nn0") + pb_field(3, "namenode-0.hdfs.svc")
             + pb_field(4, 8020) + pb_field(5, 8019))
    acl = [make_digest_acl("hdfs", "secret", all=True), make_acl("ip", "10.30.0.0/16", read=True)]
    admin.create("/hadoop-ha/hdfs-ns1/ActiveBreadCrumb", crumb, acl=acl, makepath=True)

    # --- Apache Solr -------------------------------------------------------
    for p in ("/solr/collections/logs", "/solr/configs/_default", "/solr/overseer_elect", "/solr/live_nodes"):
        kafka.ensure_path(p)
    kafka.create("/solr/security.json", b'{"authentication":{"class":"solr.BasicAuthPlugin"}}')
    kafka.create("/solr/live_nodes/solr-0.solr.svc:8983_solr", b"", ephemeral=True)

    # --- HiveServer2 dynamic service discovery -----------------------------
    kafka.create("/hiveserver2/serverUri=hive-0.hive.svc:10000;version=4.0.1;sequence=0000000003", b"",
                 ephemeral=True, makepath=True)

    # --- something unknown -------------------------------------------------
    kafka.ensure_path("/myapp/settings")
    kafka.set("/myapp/settings", b"db.password=hunter2\n")

    print("seeded; sessions:", [hex(c.client_id[0]) for c in (kafka, nifi, dubbo, admin)])
    if hold:
        time.sleep(hold)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("hosts")
    ap.add_argument("--hold", type=int, default=0)
    a = ap.parse_args()
    seed(a.hosts, a.hold)
