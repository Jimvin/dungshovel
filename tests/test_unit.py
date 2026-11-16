import json

from dungshovel import clients, fingerprint, server
from dungshovel.enumerate import Tree
from dungshovel.util import (decode_protobuf, encode_data, parse_hostport, parse_target,
                             parse_url_endpoint, session_server_id)


# --- helpers ---------------------------------------------------------------

def make_tree(paths, data=None, ephemeral=()):
    """Build a Tree from a list of paths (parents are added automatically)."""
    tree = Tree()
    all_paths = {"/"}
    for p in paths:
        parts = p.strip("/").split("/")
        for i in range(1, len(parts) + 1):
            all_paths.add("/" + "/".join(parts[:i]))
    for p in sorted(all_paths):
        tree.nodes[p] = {"path": p, "stat": {"ephemeral_owner": "0x1" if p in ephemeral else None,
                                             "ctime": None, "num_children": 0}}
        if p != "/":
            parent = p.rsplit("/", 1)[0] or "/"
            tree.children.setdefault(parent, []).append(p.rsplit("/", 1)[1])
    for parent, kids in tree.children.items():
        tree.nodes[parent]["stat"]["num_children"] = len(kids)
    tree.raw_data.update(data or {})
    return tree


def apps_by_key(tree):
    apps, cl, _ = fingerprint.fingerprint(None, tree, allow_reads=False)
    return {(a["key"], a["root"]): a for a in apps}, cl


# --- util ------------------------------------------------------------------

def test_parse_target():
    assert parse_target("zk1", 2181) == ("zk1", 2181)
    assert parse_target("zk1:2281", 2181) == ("zk1", 2281)
    assert parse_target("[::1]:2182", 2181) == ("::1", 2182)
    assert parse_target("fe80::1", 2181) == ("fe80::1", 2181)


def test_parse_hostport():
    assert parse_hostport("solr-0:8983_solr") == ("solr-0", 8983)
    assert parse_hostport("rs1.example.com,16020,1700000000000") == ("rs1.example.com", 16020)
    assert parse_hostport("[fe80::1]:9092") == ("fe80::1", 9092)
    assert parse_hostport("not an endpoint") is None
    assert parse_hostport("host:99999") is None
    assert parse_url_endpoint("SASL_SSL://b-1.kafka:9096") == ("SASL_SSL", "b-1.kafka", 9096)


def test_encode_data():
    assert encode_data(b"hello\n") == ("hello\n", "utf-8", False)
    assert encode_data(b"\x00\xff")[1] == "base64"
    assert encode_data(b"hello", force_base64=True) == ("aGVsbG8=", "base64", False)
    assert encode_data(b"abcdef", max_bytes=3) == ("abc", "utf-8", True)


def test_protobuf_and_session():
    # field 1 = "ns", field 4 = 8020
    fields = decode_protobuf(b"\x0a\x02ns\x20\xd4\x3e")
    assert fields[1] == [b"ns"] and fields[4] == [8020]
    assert decode_protobuf(b"\xff\xff\xff") == {}
    assert session_server_id("0x300000b38810001") == 3


# --- 4lw parsers -----------------------------------------------------------

CONS = """ /10.0.0.7:53120[1](queued=0,recved=52,sent=52,sid=0x100000b38810001,lop=PING,est=1794864396373,to=10000,lcxid=0x15,lzxid=0x57,lresp=864987,llat=0,minlat=0,avglat=0.0,maxlat=1)
 /[0:0:0:0:0:0:0:1]:41000[0](queued=0,recved=1,sent=0)
"""

DUMP = """SessionTracker dump:
Session Sets (2)/(2):
0 expire at Thu Jan 01 00:14:30 UTC 1970:
1 expire at Thu Jan 01 00:14:40 UTC 1970:
\t0x100000b38810001
ephemeral nodes dump:
Sessions with Ephemerals (1):
0x100000b38810001:
\t/brokers/ids/1
\t/controller
Connections dump:
Connections Sets (1)/(1):
"""


def test_parse_cons():
    conns = server.parse_cons(CONS)
    assert conns[0]["address"] == "10.0.0.7" and conns[0]["port"] == 53120
    assert conns[0]["session_id"] == "0x100000b38810001"
    assert conns[0]["established"] == 1794864396373
    assert conns[1]["session_id"] is None and conns[1]["port"] == 41000


def test_parse_dump():
    d = server.parse_dump(DUMP)
    assert d["sessions"] == ["0x100000b38810001"]
    assert d["ephemerals"]["0x100000b38810001"] == ["/brokers/ids/1", "/controller"]


def test_interpret_disabled_and_admin_fallback():
    raw = {"4lw": {"cons": {"status": "disabled", "output": "cons is not executed because..."}},
           "admin": {"cons": {"status": "ok", "output": {"connections": [
               {"remote_socket_address": "10.0.0.8:1234", "session_id": 72057642230546433,
                "established": 1794864396373, "last_response_time": 1000}], "secure_connections": []}}}}
    res = server.interpret(raw)
    assert res["connections_source"] == "admin:connections"
    assert res["connections"][0]["session_id"] == "0x100000b38810001"
    assert res["exposure"]["4lw"]["cons"] == "disabled"


# --- fingerprinting --------------------------------------------------------

KAFKA = ["brokers/ids/1", "brokers/topics/t", "controller_epoch", "cluster/id", "config/topics",
         "consumers/g/ids", "admin/delete_topics"]


def test_kafka_in_chroot_and_at_root():
    chroot = make_tree(["/znode-abc/" + p for p in KAFKA])
    apps, _ = apps_by_key(chroot)
    assert ("kafka", "/znode-abc") in apps
    assert ("dubbo", "/znode-abc") not in apps and ("dubbo", "/") not in apps  # Kafka's /consumers

    root = make_tree(["/" + p for p in KAFKA] + ["/myapp/x"])
    apps, _ = apps_by_key(root)
    assert ("kafka", "/") in apps
    un = fingerprint.unclassified_roots(root, list(apps.values()))
    assert [u["path"] for u in un] == ["/myapp"]


def test_kafka_extractor_uses_data():
    broker = json.dumps({"endpoints": ["SSL://b0.kafka:9093"], "timestamp": "1763300000000"}).encode()
    tree = make_tree(["/kafka/" + p for p in KAFKA],
                     data={"/kafka/brokers/ids/1": broker,
                           "/kafka/cluster/id": b'{"version":"1","id":"CID"}'},
                     ephemeral={"/kafka/brokers/ids/1"})
    apps, cl = apps_by_key(tree)
    assert apps[("kafka", "/kafka")]["details"]["cluster_id"] == "CID"
    b = [c for c in cl if c["role"] == "broker"][0]
    assert (b["address"], b["port"], b["evidence"]) == ("b0.kafka", 9093, "ephemeral")


def test_dubbo_url_children():
    p = "dubbo%3A%2F%2F10.0.0.5%3A20880%2Forg.Svc%3Fapplication%3Dgreeter%26timestamp%3D1763200000000"
    c = "consumer%3A%2F%2F10.0.0.9%2Forg.Svc%3Fapplication%3Dweb"
    tree = make_tree(["/dubbo/org.Svc/providers/" + p, "/dubbo/org.Svc/consumers/" + c,
                      "/dubbo/org.Svc/configurators"])
    apps, cl = apps_by_key(tree)
    assert apps[("dubbo", "/dubbo")]["details"]["dubbo_applications"] == ["greeter", "web"]
    assert {(c["address"], c["port"], c["role"]) for c in cl} == {
        ("10.0.0.5", 20880, "provider"), ("10.0.0.9", None, "consumer")}


def test_nifi_solr_hive_layouts():
    tree = make_tree(["/nifi/leaders/Cluster Coordinator/_c_x-lock-0000000001",
                      "/solr/live_nodes/solr-0:8983_solr", "/solr/collections/c", "/solr/configs/d",
                      "/hiveserver2/serverUri=hs2:10000;version=4.0.1;sequence=0000000001"])
    apps, cl = apps_by_key(tree)
    assert {k for k, _ in apps} == {"nifi", "solr", "hiveserver2"}
    assert ("hs2", 10000) in {(c["address"], c["port"]) for c in cl}


def test_hadoop_ha_breadcrumb():
    crumb = b"\x0a\x03ns1\x12\x03nn0\x1a\x03nn0\x20\xd4\x3e"
    tree = make_tree(["/hadoop-ha/ns1/ActiveBreadCrumb"], data={"/hadoop-ha/ns1/ActiveBreadCrumb": crumb})
    apps, cl = apps_by_key(tree)
    assert cl[0]["address"] == "nn0" and cl[0]["port"] == 8020 and cl[0]["evidence"] == "persistent"


def test_no_match_on_plain_tree():
    apps, _ = apps_by_key(make_tree(["/a/b/c", "/config/x", "/zookeeper/quota"]))
    assert apps == {}


# --- client correlation ----------------------------------------------------

def test_clients_correlate_session_to_connection():
    tree = make_tree(["/svc/live/node1:9000"], ephemeral={"/svc/live/node1:9000"})
    probe = {"connections": [{"address": "10.0.0.7", "port": 5000, "session_id": "0x1"},
                             {"address": "10.0.0.9", "port": 5001, "session_id": "0xff"}],
             "connections_source": "4lw:cons"}
    app_clients = [{"address": "node1", "port": 9000, "application": "X", "role": "r",
                    "evidence": "ephemeral", "session_id": "0x1", "registered": None}]
    res = clients.build(probe, tree, app_clients, own_session="0xff")
    conns = {c["session_id"]: c for c in res["connections"]}
    assert conns["0x1"]["ephemeral_count"] == 1 and conns["0x1"]["applications"] == ["X/r"]
    assert conns["0xff"]["self"]
    assert res["registrations"][0]["connected_from"] == "10.0.0.7:5000"
    assert "10.0.0.9" not in {h["identity"] for h in res["hosts"]}


# --- report rendering --------------------------------------------------------

def test_text_report_renders_plain_and_coloured():
    from dungshovel import report
    tree = make_tree(["/kafka/" + p for p in KAFKA] + ["/myapp/x"], ephemeral={"/kafka/brokers/ids/1"})
    tree.nodes["/kafka"]["acl"] = [{"scheme": "world", "id": "anyone", "perms": "cdrwa"}]
    apps, app_clients, reads = fingerprint.fingerprint(None, tree, allow_reads=False)
    nodes = [dict(tree.nodes[p], stat={**tree.nodes[p]["stat"], "data_length": 0, "mtime": None})
             for p in sorted(tree.nodes)]
    rep = {"generated": "2026-11-16T00:00:00Z", "targets": [{
        "target": "zk:2181", "connection": {"ok": True, "session_id": "0x1", "auth_schemes": []},
        "server": {"info": {"version_short": "3.9.7", "mode": "leader"},
                   "exposure": {"4lw": {"srvr": "ok", "cons": "disabled"}, "admin": {}}},
        "znodes": {"count": len(nodes), "basepath": "/", "truncated": False, "denied": [],
                   "acl_summary": {"world_read": 1, "world_write": 1, "world_admin": 1, "acl_unreadable": 0},
                   "nodes": nodes},
        "fingerprint": {"applications": apps, "reads": reads,
                        "unclassified": fingerprint.unclassified_roots(tree, apps)},
        "clients": clients.build({}, tree, app_clients),
    }]}
    plain = report.to_text(rep)
    assert "Apache Kafka" in plain and "/myapp" in plain and "└── " in plain
    assert "\x1b[" not in plain and not any(line.endswith(" ") for line in plain.splitlines())
    assert "\x1b[" in report.to_text(rep, color="always")


# --- findings -----------------------------------------------------------------

def _ids(fs):
    return [f["id"] for f in fs]


def _result(version="3.9.7", auth=(), tls=False, acl=None, exposure=None, apps=(), idents=()):
    return {"port": 2181,
            "connection": {"ok": True, "tls": tls, "auth_schemes": list(auth)},
            "server": {"info": {"version_short": version}, "exposure": exposure or {}},
            "znodes": {"acl_summary": acl or {}, "denied": []},
            "fingerprint": {"applications": list(apps)},
            "clients": {"acl_identities": list(idents)}}


def test_cve_ranges():
    from dungshovel import cves
    ids = lambda v: {c["id"] for c, disputed in cves.affecting(v) if not disputed}
    assert cves.affecting("3.9.7") == [] and cves.affecting("3.8.8") == []
    assert "CVE-2024-51504" in ids("3.9.2") and "CVE-2024-23944" not in ids("3.9.2")
    assert ids("3.9.6") == {"CVE-2026-115448", "CVE-2026-100302", "CVE-2026-100300",
                            "CVE-2026-100252", "CVE-2026-100235"}
    assert "CVE-2024-23944" in ids("3.7.2") and "CVE-2024-23944" not in ids("3.5.9")
    assert "CVE-2019-0201" in ids("3.5.4-beta") and "CVE-2019-0201" not in ids("3.5.5")
    assert "CVE-2023-44981" in ids("3.4.14") and "CVE-2023-44981" not in ids("3.7.2")
    for c in cves.CVES:  # every fixed version must lie outside the affected ranges
        for f in c["fixed"]:
            assert c["id"] not in ids(f), (c["id"], f)


def test_findings_versions():
    from dungshovel import findings
    fs = {f["id"]: f for f in findings.evaluate(_result("3.6.3", acl={"world_write": 1, "world_read": 1}))}
    assert "ZK-VERSION-EOL" in fs and "CVE-2026-79993" in fs["ZK-VERSION-EOL"]["evidence"][0]
    assert fs["CVE-2024-23944"]["severity"] == "high"          # critical + unauthenticated session
    assert fs["CVE-2023-44981"]["severity"] == "medium"        # needs quorum SASL: conditional
    assert "CVE-2026-100300" in fs                             # quota ACL not read: conditional

    standalone = _result("3.9.2")
    standalone["server"]["info"]["mode"] = "standalone"
    fs = {f["id"]: f for f in findings.evaluate(standalone)}
    assert "CVE-2023-44981" not in fs                          # 3.9.2 is fixed anyway
    assert "ZK-VERSION-EOL" not in fs and "CVE-2024-51504" in fs
    assert fs["CVE-2024-51504"]["severity"] == "medium"        # IP auth on AdminServer unknown

    old = _result("3.8.2")
    old["server"]["info"]["mode"] = "standalone"
    fs = {f["id"]: f for f in findings.evaluate(old)}
    assert "CVE-2023-44981" not in fs and "CVE-2023-44981" in str(fs["ZK-KNOWN-CVES"]["evidence"])

    assert not [f for f in findings.evaluate(_result("3.9.7")) if f["id"].startswith(("CVE", "ZK-KNOWN"))]


def test_quota_precondition():
    from dungshovel import findings
    tree = make_tree(["/zookeeper/quota"])
    tree.nodes["/zookeeper/quota"]["acl"] = [{"scheme": "world", "id": "anyone", "perms": "cdrwa"}]
    fs = {f["id"]: f for f in findings.evaluate(_result("3.9.6"), tree)}
    assert fs["CVE-2026-100300"]["severity"] == "high"
    assert "world:cdrwa" in fs["CVE-2026-100300"]["evidence"][-2]


def test_findings_acl_and_exposure():
    from dungshovel import findings
    tree = make_tree(["/kafka/" + p for p in KAFKA])
    tree.nodes["/"]["acl"] = [{"scheme": "world", "id": "anyone", "perms": "cdrwa"}]
    tree.nodes["/kafka"]["acl"] = [{"scheme": "world", "id": "anyone", "perms": "cdrwa"}]
    tree.raw_data["/kafka/x"] = b"jdbc.password=hunter2"
    tree.nodes["/kafka/x"] = {"path": "/kafka/x", "acl": [{"scheme": "world", "id": "anyone", "perms": "r"}]}
    res = _result(acl={"world_write": 3, "world_admin": 0, "world_read": 3},
                  exposure={"4lw": {"srvr": "ok", "envi": "ok", "cons": "disabled"}},
                  apps=[{"key": "kafka", "application": "Apache Kafka", "root": "/kafka", "details": {}}])
    fs = {f["id"]: f for f in findings.evaluate(res, tree)}
    assert fs["ZK-ACL-WORLD-WRITE"]["severity"] == "high"
    assert any("Apache Kafka" in e for e in fs["ZK-ACL-WORLD-WRITE"]["evidence"])
    assert fs["ZK-SECRETS-IN-DATA"]["severity"] == "high" and "hunter2" not in str(fs["ZK-SECRETS-IN-DATA"])
    assert fs["ZK-4LW-EXPOSED"]["title"].endswith("envi")  # srvr alone is not sensitive
    assert "ZK-PLAINTEXT" in fs and "ZK-UNAUTH-CONNECT" not in fs


def test_findings_digest_severity_depends_on_auth():
    from dungshovel import findings
    ident = {"scheme": "digest", "id": "hdfs:abc=", "znodes": 2, "sample": ["/x"]}
    unauth = {f["id"]: f for f in findings.evaluate(_result(idents=[ident]))}
    auth = {f["id"]: f for f in findings.evaluate(_result(auth=["digest"], tls=True, idents=[ident]))}
    assert unauth["ZK-DIGEST-HASHES"]["severity"] == "high"
    assert auth["ZK-DIGEST-HASHES"]["severity"] == "medium"
    assert "ZK-PLAINTEXT" not in auth and "ZK-UNAUTH-CONNECT" not in auth


def test_findings_clean_server():
    from dungshovel import findings
    res = _result(auth=["x509"], tls=True, acl={"world_write": 0, "world_admin": 0, "world_read": 0},
                  exposure={"4lw": {"srvr": "ok", "conf": "disabled"}})
    assert findings.evaluate(res) == []

