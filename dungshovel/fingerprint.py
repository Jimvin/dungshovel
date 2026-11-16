"""Identify applications using a ZooKeeper ensemble from their znode layout.

A signature is a set of relative path patterns (fnmatch globs per segment).
The tree is searched for any node ("root") under which the patterns exist, so
applications are found even when they use a chroot such as /kafka or
/znode-<uuid>. Each match is then handed to an extractor that reads a bounded
number of znodes to pull out cluster ids, endpoints and principals.
"""

import fnmatch
import logging
import re
import urllib.parse
from dataclasses import dataclass, field

from kazoo.exceptions import KazooException, NoAuthError, NoNodeError

from dungshovel.util import (basename, decode_protobuf, join_path, ms_to_iso,
                             parse_hostport, parse_url_endpoint, pb_int, pb_str,
                             try_json)

log = logging.getLogger(__name__)


@dataclass
class Signature:
    key: str
    name: str
    all_of: tuple = ()
    any_of: tuple = ()
    min_any: int = 1
    root_name: str = None   # glob the root node's own name must match
    extract: object = None
    note: str = None
    owns: tuple = ()        # extra top-level names this app creates (used when root is "/")

    def owned_names(self):
        names = {p.split("/")[0] for p in self.all_of + self.any_of + self.owns}
        return {n for n in names if not any(c in n for c in "*?[")}


# --------------------------------------------------------------------------
# Extraction context
# --------------------------------------------------------------------------

@dataclass
class Ctx:
    zk: object
    tree: object
    allow_reads: bool = True
    budget: int = 2000
    clients: list = field(default_factory=list)
    reads: int = 0
    read_denied: int = 0
    app: str = None
    root: str = None
    _cache: dict = field(default_factory=dict)

    def children(self, path):
        return self.tree.children.get(path, [])

    def exists(self, path):
        return path in self.tree.nodes

    def data(self, path):
        if path in self.tree.raw_data:
            return self.tree.raw_data[path]
        if path in self._cache:
            return self._cache[path]
        if not self.allow_reads or self.zk is None or self.reads >= self.budget:
            return None
        self.reads += 1
        try:
            raw = self.zk.get(path)[0]
        except NoAuthError:
            self.read_denied += 1
            raw = None
        except (NoNodeError, KazooException):
            raw = None
        self._cache[path] = raw
        return raw

    def text(self, path):
        raw = self.data(path)
        if raw is None:
            return None
        return raw.decode("utf-8", "replace")

    def json(self, path):
        return try_json(self.data(path))

    def acl(self, path):
        rec = self.tree.nodes.get(path) or {}
        return rec.get("acl")

    def world_readable(self, path):
        acl = self.acl(path)
        if acl is None:
            return None
        return any(a["scheme"] == "world" and a["id"] == "anyone" and "r" in a["perms"] for a in acl)

    def add_client(self, znode=None, address=None, port=None, principal=None, role=None,
                   registered=None, evidence=None, session_id=None, **details):
        stat = self.tree.stat(znode) if znode else None
        if session_id is None and stat:
            session_id = stat.get("ephemeral_owner")
        if evidence is None:
            if stat:
                evidence = "ephemeral" if stat.get("ephemeral_owner") else "persistent"
            else:
                evidence = "registration"
        if registered is None and stat:
            registered = stat.get("ctime")
        self.clients.append({
            "address": address,
            "port": port,
            "principal": principal,
            "application": self.app,
            "app_root": self.root,
            "role": role,
            "evidence": evidence,
            "session_id": session_id,
            "znode": znode,
            "registered": registered,
            "details": {k: v for k, v in details.items() if v not in (None, "", [], {})},
        })


def _j(*parts):
    path = parts[0]
    for p in parts[1:]:
        for seg in p.split("/"):
            path = join_path(path, seg)
    return path


def _endpoint_children(ctx, parent, role, skip=()):
    """Add a client per child whose name is an endpoint; return the endpoints found."""
    found = []
    for name in ctx.children(parent):
        if name in skip:
            continue
        hp = parse_hostport(name)
        if hp:
            ctx.add_client(znode=_j(parent, name), address=hp[0], port=hp[1], role=role)
            found.append("%s:%d" % hp)
    return found


# --------------------------------------------------------------------------
# Extractors
# --------------------------------------------------------------------------

_KAFKA_CONSUMER_ID = re.compile(r"^(?P<group>.*)_(?P<host>[^_]+?)-(?P<ts>\d{13})-(?P<uuid>[0-9a-f]{8})$")


def extract_kafka(ctx, root):
    d = {}
    cid = ctx.json(_j(root, "cluster/id"))
    if isinstance(cid, dict):
        d["cluster_id"] = cid.get("id")
    ctrl = ctx.json(_j(root, "controller"))
    if isinstance(ctrl, dict):
        d["controller_broker_id"] = ctrl.get("brokerid")
    brokers = []
    for bid in ctx.children(_j(root, "brokers/ids")):
        path = _j(root, "brokers/ids", bid)
        info = ctx.json(path) or {}
        endpoints = info.get("endpoints") or []
        brokers.append({"id": bid, "endpoints": endpoints, "rack": info.get("rack"),
                        "registered": ms_to_iso(info.get("timestamp"))})
        if not endpoints and info.get("host"):
            endpoints = ["PLAINTEXT://%s:%s" % (info["host"], info.get("port"))]
        for ep in endpoints:
            parsed = parse_url_endpoint(ep)
            if parsed:
                ctx.add_client(znode=path, address=parsed[1], port=parsed[2], role="broker",
                               registered=ms_to_iso(info.get("timestamp")),
                               broker_id=bid, listener=parsed[0], rack=info.get("rack"))
    d["brokers"] = brokers
    d["topics"] = len(ctx.children(_j(root, "brokers/topics")))

    users = ctx.children(_j(root, "config/users"))
    if users:
        d["scram_users"] = users
        d["scram_users_world_readable"] = any(ctx.world_readable(_j(root, "config/users", u)) for u in users)
        for u in users:
            ctx.add_client(znode=_j(root, "config/users", u), principal="User:%s" % u,
                           role="scram-credential", evidence="persistent")

    principals = {}
    for acl_root in ("kafka-acl", "kafka-acl-extended/prefixed"):
        base = _j(root, acl_root)
        for rtype in ctx.children(base):
            for res in ctx.children(_j(base, rtype)):
                path = _j(base, rtype, res)
                acls = (ctx.json(path) or {}).get("acls") or []
                for a in acls:
                    key = (a.get("principal"), a.get("host"))
                    principals.setdefault(key, []).append("%s %s %s:%s" % (
                        a.get("permissionType"), a.get("operation"), rtype, res))
    for (principal, host), grants in sorted(principals.items(), key=lambda x: str(x[0])):
        ctx.add_client(principal=principal, address=None if host in (None, "*") else host,
                       role="acl-principal", evidence="persistent", grants=grants[:20],
                       grant_count=len(grants))
    if principals:
        d["acl_principals"] = sorted({p for p, _ in principals if p})

    groups = ctx.children(_j(root, "consumers"))
    if groups:
        d["zk_consumer_groups"] = len(groups)
        for g in groups:
            for cid_name in ctx.children(_j(root, "consumers", g, "ids")):
                m = _KAFKA_CONSUMER_ID.match(cid_name)
                ctx.add_client(znode=_j(root, "consumers", g, "ids", cid_name),
                               address=m.group("host") if m else None, role="zk-consumer",
                               registered=ms_to_iso(m.group("ts")) if m else None, group=g)
    return d


def extract_nifi(ctx, root):
    d = {"leader_elections": {}}
    leaders = _j(root, "leaders")
    for role in ctx.children(leaders):
        participants = []
        kids = sorted(ctx.children(_j(leaders, role)), key=lambda n: n.rsplit("-", 1)[-1])
        for i, child in enumerate(kids):
            path = _j(leaders, role, child)
            participant = (ctx.text(path) or "").strip()
            hp = parse_hostport(participant)
            participants.append(participant or child)
            ctx.add_client(znode=path, address=hp[0] if hp else (participant or None),
                           port=hp[1] if hp else None, role="election:%s" % role,
                           current_leader=(i == 0) or None)
        d["leader_elections"][role] = participants
    d["component_state_nodes"] = len(ctx.children(_j(root, "components")))
    return d


def _parse_dubbo_url(name):
    url = urllib.parse.unquote(name)
    try:
        parts = urllib.parse.urlsplit(url)
        host, port = parts.hostname, parts.port
    except ValueError:
        return None
    query = dict(urllib.parse.parse_qsl(parts.query))
    return {"protocol": parts.scheme, "host": host, "port": port, "query": query}


def extract_dubbo(ctx, root):
    services = []
    for svc in ctx.children(root):
        if svc in ("metadata", "config", "mapping"):
            continue
        providers = ctx.children(_j(root, svc, "providers"))
        consumers = ctx.children(_j(root, svc, "consumers"))
        if not providers and not consumers:
            continue
        services.append({"service": svc, "providers": len(providers), "consumers": len(consumers)})
        for role, names in (("provider", providers), ("consumer", consumers)):
            for name in names:
                u = _parse_dubbo_url(name)
                if not u:
                    continue
                q = u["query"]
                ctx.add_client(znode=_j(root, svc, role + "s", name), address=u["host"], port=u["port"],
                               role=role, registered=ms_to_iso(q.get("timestamp")), service=svc,
                               dubbo_application=q.get("application"), pid=q.get("pid"),
                               dubbo_version=q.get("release") or q.get("dubbo"))
    apps = sorted({c["details"].get("dubbo_application") for c in ctx.clients
                   if c["details"].get("dubbo_application")})
    return {"services": len(services), "dubbo_applications": apps, "service_list": services}


def _hbase_servername(raw):
    """Decode an HBase master/meta znode: magic 0xff, id, 'PBUF', protobuf."""
    if not raw:
        return None
    i = raw.find(b"PBUF")
    if i < 0:
        return None
    outer = decode_protobuf(raw[i + 4:])
    inner = outer.get(1, [None])[0]
    if not isinstance(inner, (bytes, bytearray)):
        return None
    sn = decode_protobuf(inner)
    host = pb_str(sn, 1)
    return (host, pb_int(sn, 2), pb_int(sn, 3)) if host else None


def extract_hbase(ctx, root):
    d = {}
    raw = ctx.data(_j(root, "hbaseid"))
    if raw:
        m = re.search(rb"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", raw)
        if m:
            d["cluster_id"] = m.group(0).decode()
    master = _hbase_servername(ctx.data(_j(root, "master")))
    if master:
        d["active_master"] = "%s:%s" % master[:2]
        ctx.add_client(znode=_j(root, "master"), address=master[0], port=master[1], role="active-master",
                       registered=ms_to_iso(master[2]))
    meta = _hbase_servername(ctx.data(_j(root, "meta-region-server")))
    if meta:
        d["meta_region_server"] = "%s:%s" % meta[:2]
    for sub, role in (("rs", "regionserver"), ("backup-masters", "backup-master")):
        for name in ctx.children(_j(root, sub)):
            parts = name.split(",")
            hp = parse_hostport(name)
            if hp:
                ctx.add_client(znode=_j(root, sub, name), address=hp[0], port=hp[1], role=role,
                               registered=ms_to_iso(parts[2]) if len(parts) == 3 else None)
    d["regionservers"] = len(ctx.children(_j(root, "rs")))
    d["tables"] = len(ctx.children(_j(root, "table")))
    return d


def extract_hadoop_ha(ctx, root):
    is_yarn = "yarn" in basename(root).lower()
    d = {"service": "YARN ResourceManager HA" if is_yarn else "HDFS NameNode HA", "nameservices": {}}
    if is_yarn:
        ctx.app = "Apache Hadoop YARN (ResourceManager HA)"
    for ns in ctx.children(root):
        entry = {}
        for node, label in (("ActiveStandbyElectorLock", "active"), ("ActiveBreadCrumb", "last_active")):
            path = _j(root, ns, node)
            if not ctx.exists(path):
                continue
            pb = decode_protobuf(ctx.data(path) or b"")
            if is_yarn:
                rm = pb_str(pb, 2)
                entry[label] = rm
                if rm:
                    ctx.add_client(znode=path, address=None, role="resourcemanager-" + label,
                                   cluster_id=pb_str(pb, 1), rm_id=rm)
            else:
                host, port = pb_str(pb, 3), pb_int(pb, 4)
                entry[label] = "%s:%s (%s)" % (host, port, pb_str(pb, 2)) if host else None
                if host:
                    ctx.add_client(znode=path, address=host, port=port, role="namenode-" + label,
                                   nameservice=pb_str(pb, 1), namenode_id=pb_str(pb, 2),
                                   zkfc_port=pb_int(pb, 5))
        d["nameservices"][ns] = entry
    return d


def extract_yarn_store(ctx, root):
    return {"applications": len(ctx.children(_j(root, "ZKRMStateRoot/RMAppRoot")))}


def extract_solr(ctx, root):
    d = {"live_nodes": _endpoint_children(ctx, _j(root, "live_nodes"), "solr-node")}
    d["collections"] = len(ctx.children(_j(root, "collections")))
    d["configsets"] = len(ctx.children(_j(root, "configs")))
    sec = _j(root, "security.json")
    if ctx.exists(sec):
        st = ctx.tree.stat(sec) or {}
        d["security_json"] = {"present": True, "size": st.get("data_length"),
                              "world_readable": ctx.world_readable(sec)}
    return d


def extract_hiveserver2(ctx, root):
    servers = []
    for name in ctx.children(root):
        kv = dict(p.split("=", 1) for p in name.split(";") if "=" in p)
        hp = parse_hostport(kv.get("serverUri", ""))
        if hp:
            servers.append("%s:%d" % hp)
            ctx.add_client(znode=_j(root, name), address=hp[0], port=hp[1], role="hiveserver2",
                           hive_version=kv.get("version"))
    return {"instances": servers}


def extract_druid(ctx, root):
    d = {"announced": _endpoint_children(ctx, _j(root, "announcements"), "druid-node")}
    disc = _j(root, "internal-discovery")
    d["roles"] = {}
    for role in ctx.children(disc):
        d["roles"][role] = _endpoint_children(ctx, _j(disc, role), "druid-" + role)
    return d


def extract_storm(ctx, root):
    return {"nimbuses": _endpoint_children(ctx, _j(root, "nimbuses"), "nimbus"),
            "supervisors": len(ctx.children(_j(root, "supervisors"))),
            "topologies": len(ctx.children(_j(root, "storms")))}


def extract_pulsar(ctx, root):
    return {"brokers": _endpoint_children(ctx, _j(root, "loadbalance/brokers"), "pulsar-broker"),
            "clusters": ctx.children(_j(root, "admin/clusters")),
            "tenants": len(ctx.children(_j(root, "admin/policies")))}


def extract_bookkeeper(ctx, root):
    return {"available_bookies": _endpoint_children(ctx, _j(root, "available"), "bookie", skip=("readonly",)),
            "readonly_bookies": _endpoint_children(ctx, _j(root, "available/readonly"), "bookie-readonly"),
            "registered_bookies": _endpoint_children(ctx, _j(root, "cookies"), "bookie-cookie")}


_FLINK_ADDR = re.compile(rb"(?:(?:akka|pekko)(?:\.tcp)?://flink@|https?://)([A-Za-z0-9._-]+):(\d{1,5})")


def extract_flink(ctx, root):
    d = {"jobgraphs": len(ctx.children(_j(root, "jobgraphs"))), "leaders": {}}
    leader = _j(root, "leader")
    for comp in ctx.children(leader):
        for path in (_j(leader, comp, "connection_info"), _j(leader, comp)):
            raw = ctx.data(path) if ctx.exists(path) else None
            m = _FLINK_ADDR.search(raw or b"")
            if m:
                host, port = m.group(1).decode(), int(m.group(2))
                d["leaders"][comp] = "%s:%d" % (host, port)
                ctx.add_client(znode=path, address=host, port=port, role="flink-" + comp)
                break
    return d


def extract_clickhouse(ctx, root):
    return {"tables": len(ctx.children(_j(root, "tables"))),
            "ddl_queue_entries": len(ctx.children(_j(root, "task_queue/ddl")))}


def extract_accumulo(ctx, root):
    d = {"instances": {}}
    for name in ctx.children(_j(root, "instances")):
        d["instances"][name] = (ctx.text(_j(root, "instances", name)) or "").strip() or None
    for inst in ctx.children(root):
        if inst == "instances":
            continue
        _endpoint_children(ctx, _j(root, inst, "tservers"), "tserver")
    return d


_HELIX_INSTANCE = re.compile(r"^(?:(?P<role>[A-Za-z]+)_)?(?P<host>[A-Za-z0-9.-]+)_(?P<port>\d{1,5})$")


def extract_helix(ctx, root):
    d = {"cluster": basename(root)}
    roles = set()
    for sub, evidence in (("LIVEINSTANCES", None), ("INSTANCES", "persistent")):
        for name in ctx.children(_j(root, sub)):
            m = _HELIX_INSTANCE.match(name)
            role = (m.group("role") if m else None) or "participant"
            roles.add(role)
            if sub == "INSTANCES" and name in ctx.children(_j(root, "LIVEINSTANCES")):
                continue
            ctx.add_client(znode=_j(root, sub, name), address=m.group("host") if m else name,
                           port=int(m.group("port")) if m else None,
                           role="helix-%s%s" % (role.lower(), "" if sub == "LIVEINSTANCES" else "-registered"),
                           evidence=evidence, instance=name)
    if roles & {"Server", "Broker", "Controller", "Minion"}:
        ctx.app = "Apache Pinot (Apache Helix)"
        for c in ctx.clients:
            c["application"] = ctx.app
    d["live_instances"] = len(ctx.children(_j(root, "LIVEINSTANCES")))
    d["registered_instances"] = len(ctx.children(_j(root, "INSTANCES")))
    return d


def extract_dolphinscheduler(ctx, root):
    return {"masters": _endpoint_children(ctx, _j(root, "nodes/master"), "master"),
            "workers": _endpoint_children(ctx, _j(root, "nodes/worker"), "worker")}


_SPARK_WORKER = re.compile(r"^worker_worker-\d{14}-(?P<host>.+)-(?P<port>\d{1,5})$")


def extract_spark(ctx, root):
    status = ctx.children(_j(root, "master_status"))
    for name in status:
        m = _SPARK_WORKER.match(name)
        if m:
            ctx.add_client(znode=_j(root, "master_status", name), address=m.group("host"),
                           port=int(m.group("port")), role="worker")
    return {"applications": sum(1 for n in status if n.startswith("app_")),
            "workers": sum(1 for n in status if n.startswith("worker_")),
            "drivers": sum(1 for n in status if n.startswith("driver_"))}


def extract_mesos(ctx, root):
    masters = []
    for name in ctx.children(root):
        if not name.startswith("json.info_"):
            continue
        info = ctx.json(_j(root, name)) or {}
        addr = info.get("address") or {}
        host = addr.get("hostname") or info.get("hostname") or addr.get("ip")
        port = addr.get("port") or info.get("port")
        if host:
            masters.append("%s:%s" % (host, port))
            ctx.add_client(znode=_j(root, name), address=host, port=port, role="mesos-master",
                           mesos_version=info.get("version"))
    return {"masters": masters}


def extract_patroni(ctx, root):
    clusters = {}
    for scope in ctx.children(root):
        members = []
        for m in ctx.children(_j(root, scope, "members")):
            path = _j(root, scope, "members", m)
            info = ctx.json(path) or {}
            conn = info.get("conn_url") or ""
            parsed = parse_url_endpoint(conn) if conn else None
            members.append(m)
            ctx.add_client(znode=path, address=parsed[1] if parsed else None,
                           port=parsed[2] if parsed else None, role="postgres-" + (info.get("role") or "member"),
                           member=m, cluster=scope, api_url=info.get("api_url"))
        clusters[scope] = {"members": members,
                           "leader": (ctx.text(_j(root, scope, "leader")) or "").strip() or None}
    return {"clusters": clusters}


def extract_curator_sd(ctx, root):
    services = {}
    for svc in ctx.children(root):
        instances = 0
        for inst in ctx.children(_j(root, svc)):
            path = _j(root, svc, inst)
            info = ctx.json(path)
            hp = parse_hostport(inst)
            if isinstance(info, dict) and "address" in info:
                instances += 1
                ctx.add_client(znode=path, address=info.get("address"),
                               port=info.get("port") or info.get("sslPort"), role="service-instance",
                               registered=ms_to_iso(info.get("registrationTimeUTC")), service=svc)
            elif hp:
                instances += 1
                ctx.add_client(znode=path, address=hp[0], port=hp[1], role="service-instance", service=svc)
        services[svc] = instances
    return {"services": services}


def extract_hive_locks(ctx, root):
    return {"lock_namespace_nodes": len(ctx.children(root))}


# --------------------------------------------------------------------------
# Signatures
# --------------------------------------------------------------------------

SIGNATURES = [
    Signature("kafka", "Apache Kafka (ZooKeeper mode)",
              all_of=("brokers/ids", "brokers/topics"),
              any_of=("controller", "controller_epoch", "cluster/id", "config/topics",
                      "isr_change_notification", "latest_producer_id_block", "admin"),
              owns=("config", "cluster", "consumers", "kafka-acl", "kafka-acl-extended",
                    "kafka-acl-changes", "kafka-acl-extended-changes", "log_dir_event_notification",
                    "feature", "delegation_token", "migration"),
              extract=extract_kafka),
    Signature("nifi", "Apache NiFi",
              any_of=("leaders/Cluster Coordinator", "leaders/Primary Node"),
              extract=extract_nifi),
    Signature("dubbo", "Apache Dubbo (interface-level registry)",
              # Registrations are URL-encoded URLs ("dubbo%3A%2F%2Fhost..."); requiring them, or the
              # configurators/routers categories, avoids matching e.g. Kafka's /consumers.
              any_of=("*/providers/*%3A%2F%2F*", "*/consumers/*%3A%2F%2F*", "*/configurators", "*/routers"),
              min_any=2,
              extract=extract_dubbo),
    Signature("hbase", "Apache HBase",
              all_of=("hbaseid",), any_of=("master", "rs", "meta-region-server", "table", "backup-masters"),
              extract=extract_hbase),
    Signature("hadoop-ha", "Apache Hadoop HDFS (NameNode HA)",
              any_of=("*/ActiveBreadCrumb", "*/ActiveStandbyElectorLock"),
              extract=extract_hadoop_ha),
    Signature("yarn-rmstore", "Apache Hadoop YARN (RM state store)",
              all_of=("ZKRMStateRoot",), extract=extract_yarn_store),
    Signature("solr", "Apache Solr (SolrCloud)",
              all_of=("live_nodes",),
              any_of=("collections", "overseer_elect", "aliases.json", "security.json",
                      "clusterstate.json", "configs", "clusterprops.json"),
              min_any=2, extract=extract_solr),
    Signature("hiveserver2", "Apache Hive (HiveServer2 discovery)",
              any_of=("serverUri=*",), extract=extract_hiveserver2),
    Signature("hive-locks", "Apache Hive (lock manager)",
              root_name="hive_zookeeper_namespace*", extract=extract_hive_locks),
    Signature("druid", "Apache Druid",
              any_of=("announcements", "internal-discovery", "coordinator", "segments",
                      "loadQueue", "listeners", "servedSegments"),
              min_any=3, extract=extract_druid),
    Signature("storm", "Apache Storm",
              any_of=("supervisors", "assignments", "nimbuses", "storms", "workerbeats",
                      "leader-lock", "blobstore"),
              min_any=3, extract=extract_storm),
    Signature("pulsar", "Apache Pulsar",
              any_of=("admin/clusters", "loadbalance/brokers", "namespace", "managed-ledgers",
                      "schemas", "admin/policies"),
              min_any=2, extract=extract_pulsar),
    Signature("bookkeeper", "Apache BookKeeper",
              any_of=("available", "cookies", "INSTANCEID", "LAYOUT", "underreplication"),
              min_any=3, extract=extract_bookkeeper),
    Signature("flink", "Apache Flink (HA services)",
              any_of=("leaderlatch", "jobgraphs", "checkpoint-counter", "checkpoints",
                      "leader/resource_manager*", "leader/dispatcher*", "leader/rest_server*"),
              min_any=2, extract=extract_flink),
    Signature("clickhouse", "ClickHouse",
              all_of=("task_queue/ddl",), extract=extract_clickhouse),
    Signature("accumulo", "Apache Accumulo",
              all_of=("instances",), any_of=("*/tservers", "*/managers", "*/masters"),
              extract=extract_accumulo),
    Signature("helix", "Apache Helix",
              any_of=("CONFIGS", "IDEALSTATES", "LIVEINSTANCES", "CONTROLLER",
                      "EXTERNALVIEW", "INSTANCES", "PROPERTYSTORE"),
              min_any=5, extract=extract_helix),
    Signature("dolphinscheduler", "Apache DolphinScheduler",
              any_of=("nodes/master", "nodes/worker", "lock/masters", "lock/failover"),
              min_any=2, extract=extract_dolphinscheduler),
    Signature("spark", "Apache Spark (standalone master recovery)",
              all_of=("leader_election", "master_status"), extract=extract_spark),
    Signature("mesos", "Apache Mesos",
              any_of=("json.info_*", "log_replicas"), extract=extract_mesos),
    Signature("patroni", "Patroni (PostgreSQL HA)",
              all_of=("*/members", "*/initialize"), extract=extract_patroni),
    Signature("curator-sd", "Apache Curator service discovery (e.g. Apache Dubbo 3, Spring Cloud Zookeeper)",
              all_of=("*/*",), root_name="services", extract=extract_curator_sd),
]


# --------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------

class _Index:
    def __init__(self, tree):
        self.parts = {p: tuple(p.strip("/").split("/")) for p in tree.nodes if p != "/"}

    def roots_for(self, pattern):
        segs = [re.compile(fnmatch.translate(s)) for s in pattern.split("/")]
        n = len(segs)
        roots = set()
        for parts in self.parts.values():
            if len(parts) < n or not segs[-1].match(parts[-1]):
                continue
            if all(r.match(x) for r, x in zip(segs[:-1], parts[-n:-1])):
                roots.add("/" + "/".join(parts[:-n]))
        return roots

    def nodes_named(self, glob):
        r = re.compile(fnmatch.translate(glob))
        return {"/" + "/".join(p) for p in self.parts.values() if r.match(p[-1])}


def match(tree):
    """Return [(signature, root, evidence_patterns)] for every match in the tree."""
    index = _Index(tree)
    cache = {}

    def roots(pattern):
        if pattern not in cache:
            cache[pattern] = index.roots_for(pattern)
        return cache[pattern]

    results = []
    for sig in SIGNATURES:
        if sig.all_of:
            candidates = set.intersection(*(roots(p) for p in sig.all_of))
        elif sig.any_of:
            candidates = set().union(*(roots(p) for p in sig.any_of))
        else:
            candidates = index.nodes_named(sig.root_name) if sig.root_name else set()
        name_re = re.compile(fnmatch.translate(sig.root_name)) if sig.root_name else None
        for root in sorted(candidates):
            if root == "/zookeeper" or root.startswith("/zookeeper/"):
                continue
            if name_re and not name_re.match(basename(root)):
                continue
            hits = [p for p in sig.any_of if root in roots(p)]
            if sig.any_of and len(hits) < sig.min_any:
                continue
            score = len(sig.all_of) + len(hits)
            results.append((sig, root, list(sig.all_of) + hits, score))
    return results


def _confidence(sig, score):
    if not sig.all_of and not sig.any_of:
        return "low"
    return "high" if score >= 3 else "medium"


def fingerprint(zk, tree, allow_reads=True, read_budget=2000):
    """Match signatures and run extractors. Returns (applications, clients, reads)."""
    apps, clients = [], []
    reads = denied = 0
    for sig, root, evidence, score in match(tree):
        ctx = Ctx(zk=zk, tree=tree, allow_reads=allow_reads, budget=read_budget - reads,
                  app=sig.name, root=root)
        details, error = {}, None
        if sig.extract:
            try:
                details = sig.extract(ctx, root) or {}
            except Exception as e:  # an extractor bug must not kill the audit
                log.exception("extractor %s failed on %s", sig.key, root)
                error = "%s: %s" % (type(e).__name__, e)
        reads += ctx.reads
        denied += ctx.read_denied
        apps.append({
            "key": sig.key,
            "application": ctx.app,
            "root": root,
            "confidence": _confidence(sig, score),
            "evidence": [_j(root, p) for p in evidence],
            "details": details,
            "client_count": len(ctx.clients),
            "_owns": sorted(sig.owned_names()),
            **({"note": sig.note} if sig.note else {}),
            **({"error": error} if error else {}),
        })
        clients.extend(ctx.clients)
    return apps, clients, {"reads": reads, "denied": denied, "budget": read_budget}


def unclassified_roots(tree, apps, basepath="/"):
    """Children of the base path that no detected application accounts for."""
    out = []
    for child in tree.children.get(basepath, []):
        path = join_path(basepath, child)
        if path == "/zookeeper":
            continue
        covered = False
        for a in apps:
            r = a["root"]
            if r == basepath:
                covered = child in a.get("_owns", ())
            else:
                covered = r == path or r.startswith(path + "/") or path.startswith(r + "/")
            if covered:
                break
        if not covered:
            st = tree.stat(path) or {}
            out.append({"path": path, "num_children": st.get("num_children"), "mtime": st.get("mtime")})
    return out
