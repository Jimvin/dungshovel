"""Correlate everything we know about clients of a ZooKeeper server.

ZooKeeper does not keep a log of past connections. What we can see is:

  connected   - open connections right now ('cons' / 'stat' / AdminServer).
  ephemeral   - sessions that own ephemeral znodes: alive now, possibly
                connected to another ensemble member.
  persistent  - addresses/principals applications wrote into persistent
                znodes (registrations, breadcrumbs, cookies, ACL grants).
                These survive after the client goes away, so they are the
                main source of *previously* connected clients.
  acl         - identities (ip:, sasl:, digest:, x509:) granted in znode ACLs.
"""

from dungshovel.util import ms_to_iso, session_server_id

MAX_SAMPLE_PATHS = 10


def build(probe_result, tree, app_clients, own_session=None, own_local_ports=()):
    probe_result = probe_result or {}
    dump = probe_result.get("dump") or {}
    dump_ephemerals = dump.get("ephemerals") or {}

    # Sessions that own ephemerals, from the tree walk (all members) and 'dump' (leader only).
    session_paths = {}
    for path, rec in (tree.nodes if tree else {}).items():
        owner = (rec.get("stat") or {}).get("ephemeral_owner")
        if owner:
            session_paths.setdefault(owner, []).append(path)
    for sid, paths in dump_ephemerals.items():
        session_paths.setdefault(sid, [])
        for p in paths:
            if p not in session_paths[sid]:
                session_paths[sid].append(p)

    apps_by_session = {}
    for c in app_clients:
        if c.get("session_id"):
            apps_by_session.setdefault(c["session_id"], set()).add(
                "%s/%s" % (c["application"], c["role"]) if c.get("role") else c["application"])

    # Live connections.
    connections = []
    addr_by_session = {}
    for conn in probe_result.get("connections") or []:
        conn = dict(conn)
        sid = conn.get("session_id")
        conn["self"] = bool((own_session and sid == own_session) or
                            (not sid and conn.get("port") in own_local_ports))
        conn["established_iso"] = ms_to_iso(conn.get("established")) if isinstance(conn.get("established"), int) else conn.get("established")
        conn["last_response_iso"] = _lresp(conn.get("last_response"))
        if sid:
            addr_by_session[sid] = (conn.get("address"), conn.get("port"))
            paths = session_paths.get(sid, [])
            conn["ephemeral_count"] = len(paths)
            conn["ephemeral_sample"] = sorted(paths)[:MAX_SAMPLE_PATHS]
            conn["applications"] = sorted(apps_by_session.get(sid, ()))
        conn.pop("raw", None)
        connections.append(conn)

    # Sessions (ephemeral owners + dump), whether or not connected to this server.
    all_sessions = set(session_paths) | set(dump.get("sessions") or [])
    sessions = []
    for sid in sorted(all_sessions):
        if sid == own_session:
            continue
        paths = session_paths.get(sid, [])
        addr = addr_by_session.get(sid)
        sessions.append({
            "session_id": sid,
            "created_by_server_id": session_server_id(sid),
            "connected_here": addr is not None,
            "address": addr[0] if addr else None,
            "port": addr[1] if addr else None,
            "ephemeral_count": len(paths),
            "ephemeral_sample": sorted(paths)[:MAX_SAMPLE_PATHS],
            "applications": sorted(apps_by_session.get(sid, ())),
        })

    # Application registrations: attach the observed network address where we can.
    registrations = []
    for c in app_clients:
        c = dict(c)
        addr = addr_by_session.get(c.get("session_id"))
        if addr:
            c["connected_from"] = "%s:%s" % addr
        registrations.append(c)

    acl_identities = _acl_identities(tree)
    watches = probe_result.get("watches")

    return {
        "connections_source": probe_result.get("connections_source"),
        "dump_source": probe_result.get("dump_source"),
        "connections": connections,
        "sessions": sessions,
        "registrations": registrations,
        "acl_identities": acl_identities,
        "watches": {sid: {"count": len(p), "sample": p[:MAX_SAMPLE_PATHS]} for sid, p in watches.items()}
        if watches else None,
        "hosts": _hosts_summary(connections, sessions, registrations, acl_identities),
    }


def _lresp(value):
    """'lresp' is wall-clock on old servers but a monotonic clock (ms since an
    arbitrary origin, usually JVM start) since 3.5, so only convert plausible epochs."""
    if isinstance(value, int):
        return ms_to_iso(value) if value > 10 ** 12 else "monotonic:%d" % value
    return value


def _acl_identities(tree):
    idents = {}
    for path, rec in (tree.nodes if tree else {}).items():
        for a in rec.get("acl") or []:
            if a["scheme"] == "world":
                continue
            key = (a["scheme"], a["id"])
            entry = idents.setdefault(key, {"scheme": a["scheme"], "id": a["id"], "perms": set(),
                                            "znodes": 0, "sample": []})
            entry["perms"].update(a["perms"])
            entry["znodes"] += 1
            if len(entry["sample"]) < 5:
                entry["sample"].append(path)
    out = []
    for e in sorted(idents.values(), key=lambda e: -e["znodes"]):
        e["perms"] = "".join(c for c in "cdrwa" if c in e["perms"])
        out.append(e)
    return out


def _hosts_summary(connections, sessions, registrations, acl_identities):
    """One row per distinct address or principal with every source that mentions it."""
    hosts = {}

    def add(key, kind, source, app=None, role=None, when=None, port=None):
        if not key:
            return
        h = hosts.setdefault(key, {"identity": key, "kind": kind, "evidence": set(), "applications": set(),
                                   "roles": set(), "ports": set(), "first_registered": None})
        h["evidence"].add(source)
        if app:
            h["applications"].add(app)
        if role:
            h["roles"].add(role)
        if port:
            h["ports"].add(str(port))
        if when and (h["first_registered"] is None or when < h["first_registered"]):
            h["first_registered"] = when

    for c in connections:
        if not c.get("self"):
            add(c.get("address"), "address", "connected")
    for r in registrations:
        if r.get("address"):
            add(r["address"], "address", r["evidence"], r["application"], r.get("role"),
                r.get("registered"), r.get("port"))
        if r.get("principal"):
            add(r["principal"], "principal", r["evidence"], r["application"], r.get("role"), r.get("registered"))
    for a in acl_identities:
        if a["scheme"] == "ip":
            add(a["id"], "address", "acl")
        else:
            add("%s:%s" % (a["scheme"], a["id"]), "principal", "acl")

    order = {"connected": 0, "ephemeral": 1, "persistent": 2, "registration": 3, "acl": 4}
    out = []
    for h in hosts.values():
        out.append({**h,
                    "evidence": sorted(h["evidence"], key=lambda e: order.get(e, 9)),
                    "applications": sorted(h["applications"]),
                    "roles": sorted(h["roles"]),
                    "ports": sorted(h["ports"], key=lambda p: int(p) if p.isdigit() else 0)})
    out.sort(key=lambda h: (min(order.get(e, 9) for e in h["evidence"]), h["identity"]))
    return out
