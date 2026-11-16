"""Turn audit observations into ranked findings.

Each finding has a stable id, a severity, a one-line title, a short detail,
the evidence it was derived from and a recommendation. Findings only use data
the audit already collected; nothing here talks to the server.

Severity guide:
  high    anyone who can reach the port can modify state, take over an
          application, or obtain credentials
  medium  information disclosure that materially helps an attacker, or
          missing transport/authentication protection
  low     hardening gaps with limited direct impact
  info    context for the auditor (incomplete coverage etc.)
"""

import re

from dungshovel import cves as cvedb

SEVERITIES = ("high", "medium", "low", "info")
SEVERITY_RANK = {s: i for i, s in enumerate(SEVERITIES)}

# 4lw / AdminServer commands and what they leak.
SENSITIVE_COMMANDS = {
    "conf": "configuration (data dirs, ports, ensemble, auth settings)",
    "envi": "environment (host name, OS user, Java version, classpath, working dir)",
    "cons": "client addresses and session ids",
    "stat": "client addresses",
    "dump": "session ids and every ephemeral znode they own",
    "wchc": "watched paths per session",
    "mntr": "detailed metrics",
}

# What an anonymous writer could do to each application.
WRITE_IMPACT = {
    "kafka": "alter topic configs, ACLs and broker registrations; delete topics",
    "nifi": "hijack cluster coordinator / primary node elections, tamper with component state",
    "dubbo": "register malicious providers and redirect RPC traffic (registry poisoning)",
    "hadoop-ha": "force NameNode/ResourceManager failover or block elections",
    "hbase": "redirect clients to a rogue master or meta region server",
    "solr": "replace security.json, configsets and cluster state",
    "hiveserver2": "register a rogue HiveServer2 and capture client connections/credentials",
    "pulsar": "alter tenant/namespace policies and broker ownership",
    "patroni": "influence PostgreSQL leader election",
}

SECRET_RE = re.compile(
    rb"(?i)(password|passwd|pwd|secret|api[_-]?key|access[_-]?key|token|credential)s?\"?\s*[=:]"
    rb"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    rb"|[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s:@]+@")


def _n(count, noun):
    return "%d %s%s" % (count, noun, "" if count == 1 else "s")


def _finding(fid, severity, title, detail, evidence=None, recommendation=None):
    return {"id": fid, "severity": severity, "title": title, "detail": detail,
            "evidence": evidence or [], "recommendation": recommendation}


def _world_perms(acl):
    return "".join(a["perms"] for a in acl or [] if a["scheme"] == "world" and a["id"] == "anyone")


def evaluate(result, tree=None):
    out = []
    conn = result.get("connection") or {}
    srv = result.get("server") or {}
    info = srv.get("info") or {}
    exposure = srv.get("exposure") or {}
    zn = result.get("znodes") or {}
    acl = zn.get("acl_summary") or {}
    apps = (result.get("fingerprint") or {}).get("applications") or []
    authenticated = bool(conn.get("auth_schemes"))
    nodes = tree.nodes if tree else {}

    auth_note = ("This audit connected with credentials; whether unauthenticated clients may connect at all "
                 "was not tested." if authenticated else
                 "This audit connected without credentials, so any client that can reach the port can do this.")

    # --- world-writable / world-admin znodes ---------------------------------
    if acl.get("world_write"):
        root_perms = _world_perms((nodes.get("/") or {}).get("acl"))
        evidence, affected_apps = [], []
        if any(c in root_perms for c in "wcd"):
            evidence.append("/ is world-%s: anyone can create top-level znodes" % root_perms)
        for a in apps:
            perms = _world_perms((nodes.get(a["root"]) or {}).get("acl"))
            if any(c in perms for c in "wcd"):
                affected_apps.append(a)
                impact = WRITE_IMPACT.get(a["key"], "tamper with its coordination state")
                evidence.append("%s at %s (world:%s): %s" % (a["application"], a["root"], perms, impact))
        severity = "high" if evidence else "medium"
        out.append(_finding(
            "ZK-ACL-WORLD-WRITE", severity,
            "%s writable by anyone" % _n(acl["world_write"], "znode"),
            "world:anyone holds write, create or delete permission. " + auth_note,
            evidence,
            "Set ACLs on application roots (sasl:, x509: or digest: principals) and enable "
            "zookeeper.enforce.auth.enabled; most applications have a 'secure ZooKeeper ACLs' option."))

    if acl.get("world_admin"):
        out.append(_finding(
            "ZK-ACL-WORLD-ADMIN", "high",
            "%s with ADMIN for anyone (anyone can change the ACL)" % _n(acl["world_admin"], "znode"),
            "world:anyone holds ADMIN, so anyone can lock legitimate users out of these znodes "
            "or open them up further. " + auth_note,
            [], "Remove the 'a' permission from world:anyone; grant ADMIN only to operator principals."))

    if acl.get("world_read"):
        out.append(_finding(
            "ZK-ACL-WORLD-READ", "low",
            "%s readable by anyone" % _n(acl["world_read"], "znode"),
            "Typical for coordination data, but it lets anyone map the applications, hosts and "
            "topology using this ensemble (as this report does). " + auth_note,
            [], "Restrict READ on application roots where the data is not meant to be public."))

    # --- credentials readable -------------------------------------------------
    digest = [i for i in (result.get("clients") or {}).get("acl_identities") or [] if i["scheme"] == "digest"]
    if digest:
        out.append(_finding(
            "ZK-DIGEST-HASHES", "high" if not authenticated else "medium",
            "Digest ACL password hashes are readable (%s)" % _n(len(digest), "identity").replace("identitys", "identities"),
            "getACL returns digest ids as user:base64(SHA1(user:password)), unsalted and fast to "
            "crack offline. " + ("They were readable without credentials." if not authenticated else
                                "They were readable with the credentials used for this audit."),
            ["%s on %d znodes (e.g. %s)" % (i["id"].split(":")[0], i["znodes"], i["sample"][0]) for i in digest],
            "Prefer SASL (Kerberos/SCRAM) or x509 ACLs; if digest is required, use long random passwords."))

    for a in apps:
        d = a.get("details") or {}
        if a["key"] == "kafka" and d.get("scram_users_world_readable"):
            out.append(_finding(
                "KAFKA-SCRAM-READABLE", "high",
                "Kafka SCRAM credentials are world-readable (%s)" % a["root"],
                "Salted SCRAM verifiers under config/users can be read by anyone and attacked offline.",
                ["user: %s" % u for u in d.get("scram_users", [])],
                "Enable zookeeper.set.acl=true on the brokers and run zookeeper-security-migration."))
        if a["key"] == "solr" and (d.get("security_json") or {}).get("world_readable"):
            out.append(_finding(
                "SOLR-SECURITY-JSON-READABLE", "high",
                "Solr security.json is world-readable (%s)" % a["root"],
                "security.json holds the authentication configuration, including BasicAuth password hashes.",
                ["%s/security.json" % a["root"].rstrip("/")],
                "Configure Solr's ZooKeeper ACL provider (zkACLProvider) with credentials."))

    if tree and tree.raw_data:
        hits = []
        for path, raw in tree.raw_data.items():
            if SECRET_RE.search(raw[:65536]):
                perms = _world_perms((nodes.get(path) or {}).get("acl"))
                hits.append((path, "r" in perms))
        if hits:
            public = [p for p, world in hits if world]
            out.append(_finding(
                "ZK-SECRETS-IN-DATA", "high" if public else "medium",
                "%s with what looks like credentials" % _n(len(hits), "znode"),
                "Matched password/secret/key patterns or URLs with embedded credentials. "
                "World-readable: %d. Values are not repeated here; see the znode data." % len(public),
                ["%s%s" % (p, " (world-readable)" if w else "") for p, w in sorted(hits)][:25],
                "Move secrets out of ZooKeeper (e.g. Kubernetes Secrets) or restrict READ on these znodes."))

    # --- server exposure --------------------------------------------------------
    for via, label, fid in (("4lw", "four-letter-word commands", "ZK-4LW-EXPOSED"),
                            ("admin", "AdminServer commands", "ZK-ADMIN-EXPOSED")):
        open_cmds = [c for c, s in (exposure.get(via) or {}).items() if s == "ok" and c in SENSITIVE_COMMANDS]
        if open_cmds:
            out.append(_finding(
                fid, "medium",
                "Sensitive %s are open: %s" % (label, ", ".join(open_cmds)),
                "These need no authentication and reveal internal details.",
                ["%s: %s" % (c, SENSITIVE_COMMANDS[c]) for c in open_cmds],
                "Set 4lw.commands.whitelist=srvr,ruok (or empty)" if via == "4lw" else
                "Disable the AdminServer (admin.enableServer=false) or bind it to localhost / require auth."))

    if conn.get("ok") and not conn.get("tls"):
        out.append(_finding(
            "ZK-PLAINTEXT", "medium",
            "Client port accepts plaintext connections",
            "Traffic, znode data and digest credentials (sent in clear by addAuth) can be sniffed or altered.",
            ["port %s" % result.get("port")] +
            (["secureClientPort %s is also configured" % info["secure_client_port"]]
             if str(info.get("secure_client_port") or "-1") not in ("-1", "0", "") else []),
            "Use secureClientPort with TLS and remove the plaintext clientPort."))

    if conn.get("ok") and not authenticated and not acl.get("world_write"):
        out.append(_finding(
            "ZK-UNAUTH-CONNECT", "low",
            "Unauthenticated clients can connect",
            "Connecting without credentials succeeded. ACLs limit what they can do, but every znode name "
            "readable via world:anyone parents is still visible.",
            [], "Enable zookeeper.enforce.auth.enabled=true (ZooKeeper 3.7+)."))

    # --- version and known CVEs ------------------------------------------------------
    version = cvedb.parse_version(info.get("version_short") or info.get("version"))
    if version:
        out += _version_findings(version, {
            "conn": conn, "authenticated": authenticated, "acl": acl, "info": info,
            "exposure": exposure, "nodes": nodes,
            "acl_identities": (result.get("clients") or {}).get("acl_identities") or [],
        })

    # --- coverage -------------------------------------------------------------------
    gaps = []
    if not conn.get("ok"):
        gaps.append("could not open a ZooKeeper session: %s" % conn.get("error"))
    if zn.get("truncated"):
        gaps.append("enumeration stopped at --max-nodes")
    if zn.get("aborted"):
        gaps.append(zn["aborted"])
    if zn.get("denied"):
        gaps.append("%s not readable with the credentials used" % _n(len(zn["denied"]), "znode"))
    if zn.get("depth_limited"):
        gaps.append("some subtrees were not listed (depth limit or --exclude)")
    if gaps:
        out.append(_finding("AUDIT-COVERAGE", "info", "Audit coverage is incomplete",
                            "Findings only cover what could be read.", gaps, None))

    out.sort(key=lambda f: SEVERITY_RANK[f["severity"]])  # stable: keeps impact order within a severity
    return out


# --- CVE applicability -------------------------------------------------------------

_SEV_BY_STATUS = {
    #               critical  high      important  moderate  low
    "exploitable": ("high",   "high",   "high",    "medium", "low"),
    "applies":     ("high",   "high",   "medium",  "low",    "low"),
    "conditional": ("medium", "medium", "medium",  "low",    "low"),
}
_APACHE_SEVERITIES = ("critical", "high", "important", "moderate", "low")
_STATUS_LABEL = {
    "exploitable": "preconditions observed on this server",
    "applies": "applies to this version; no extra preconditions",
    "conditional": "depends on configuration this audit cannot see",
    "client-side": "affects client code, not the server",
}


def _ver(v):
    return ".".join(map(str, v))


def _precondition(req, ctx):
    """Return (status, reason) for a CVE precondition given what the audit observed."""
    conn, acl, info, exposure = ctx["conn"], ctx["acl"], ctx["info"], ctx["exposure"]
    standalone = str(info.get("mode", "")).lower() == "standalone"
    admin = exposure.get("admin") or {}
    admin_open = any(s == "ok" for s in admin.values())
    world_root = _world_perms((ctx["nodes"].get("/") or {}).get("acl"))

    if req == "client_port":
        if conn.get("ok") and not ctx["authenticated"]:
            return "exploitable", "an unauthenticated session to the client port was accepted"
        return "applies", "needs a session on the client port"
    if req == "world_create":
        if acl.get("world_write"):
            return "exploitable", "world:anyone may create/write %s" % _n(acl["world_write"], "znode")
        return "conditional", "needs create permission; world:anyone has none here, authenticated users may"
    if req == "read_access":
        if acl.get("world_read"):
            return "exploitable", "world:anyone may read %s" % _n(acl["world_read"], "znode")
        return "conditional", "needs read access to a znode"
    if req == "quota_writable":
        quota = ctx["nodes"].get("/zookeeper/quota")
        perms = _world_perms((quota or {}).get("acl"))
        if any(c in perms for c in "wc"):
            return "exploitable", "/zookeeper/quota is world:%s" % perms
        if quota and quota.get("acl") is not None:
            return "conditional", "/zookeeper/quota is not world-writable; principals with write access could exploit it"
        return "conditional", "needs write access under /zookeeper/quota (ACL not read)"
    if req in ("quorum", "quorum_sasl", "quorum_tls_fips"):
        if standalone:
            return "not-applicable", "server runs standalone, there is no quorum"
        if req == "quorum":
            return "applies", "server is part of an ensemble"
        return "conditional", {"quorum_sasl": "only with quorum.auth.enableSasl=true",
                               "quorum_tls_fips": "only with sslQuorum, FIPS mode and hostname verification"}[req]
    if req == "sasl_client":
        if any(i["scheme"] == "sasl" for i in ctx["acl_identities"]):
            return "applies", "sasl: ACLs are in use, so client SASL is enabled"
        return "conditional", "only with SASL authentication on the client port"
    if req == "audit_log":
        return "conditional", "only with zookeeper.audit.enable=true"
    if req == "tls":
        return "conditional", "only with TLS hostname verification (client or quorum)"
    if req == "digest_auth":
        if any(i["scheme"] == "digest" for i in ctx["acl_identities"]):
            return "exploitable", "digest ACLs are in use"
        return "conditional", "only if digest ACLs are used (none seen)"
    if req == "admin_server":
        if admin_open and any(c in world_root for c in "wcda"):
            return "exploitable", "AdminServer reachable and the root ACL is open"
        if admin_open:
            return "applies", "AdminServer reachable"
        if admin:
            return "conditional", "AdminServer not reachable from here"
        return "conditional", "AdminServer not probed (use --admin-port)"
    if req == "admin_ip_auth":
        if admin_open:
            return "conditional", "AdminServer reachable; affected only with IP-based authentication"
        return "conditional", "only with AdminServer IP-based authentication"
    if req == "wchc_4lw":
        state = (exposure.get("4lw") or {}).get("wchc")
        if state == "ok":
            return "exploitable", "wchc answers"
        if state == "disabled":
            return "not-applicable", "wchc is disabled"
        return "applies", "wchc/wchp are enabled by default on affected versions"
    if req in ("client_library", "c_cli"):
        return "client-side", {"client_library": "affects applications using the Java client library",
                               "c_cli": "affects the C CLI example shells only"}[req]
    return "conditional", "precondition unknown"


def _version_findings(version, ctx):
    out = []
    target = cvedb.upgrade_target(version)
    line = version[:2]
    maintained_min = min(cvedb.MAINTAINED_LINES)

    hits = cvedb.affecting(version)
    if line < maintained_min:
        unassessed = [c["id"] for c in cvedb.CVES
                      if all(lo and cvedb.parse_version(lo) >= maintained_min + (0,) for lo, _ in c["affected"])
                      and c["requires"] not in ("client_library", "c_cli")]
        out.append(_finding(
            "ZK-VERSION-EOL", "medium",
            "ZooKeeper %s is end-of-life" % _ver(version),
            "The %d.%d release line no longer receives security fixes, and Apache only lists affected "
            "versions for maintained lines." % line,
            (["%s known to affect 3.8/3.9 were not assessed for %d.%d and may also apply: %s"
              % (_n(len(unassessed), "CVE"), line[0], line[1], ", ".join(unassessed))] if unassessed else []),
            "Upgrade to %s." % target))

    ruled_out = []
    counted = []
    for cve, disputed in hits:
        apache_sev = cve["severity"]
        sev_label = apache_sev + (" (CVSS %.1f)" % cve["cvss"] if cve.get("cvss") else "")
        evidence = ["Apache severity: %s" % sev_label,
                    "affected: %s; fixed in %s" % (cvedb.format_ranges(cve["affected"]), ", ".join(cve["fixed"]))]
        if disputed:
            out.append(_finding(
                cve["id"], "info", "%s: %s" % (cve["id"], cve["title"]),
                "Apache's advisory is contradictory for %s; it may or may not be affected." % _ver(version),
                evidence + [cve.get("note", "")], "Check the advisory: %s" % cvedb.CVE_SOURCE))
            continue
        status, reason = _precondition(cve["requires"], ctx)
        if status == "not-applicable":
            ruled_out.append("%s (%s)" % (cve["id"], reason))
            continue
        counted.append(cve["id"])
        if status == "client-side":
            severity = "info"
        else:
            severity = _SEV_BY_STATUS[status][_APACHE_SEVERITIES.index(apache_sev)]
        evidence.append("applicability: %s; %s" % (_STATUS_LABEL[status], reason))
        if cve.get("note"):
            evidence.append(cve["note"])
        out.append(_finding(
            cve["id"], severity, "%s: %s" % (cve["id"], cve["title"]),
            "ZooKeeper %s is in the affected range." % _ver(version),
            evidence, "Upgrade to %s." % target))

    if counted or ruled_out:
        out.append(_finding(
            "ZK-KNOWN-CVES", "info",
            "%s known for ZooKeeper %s" % (_n(len(counted), "CVE"), _ver(version)),
            "From %s, as of %s." % (cvedb.CVE_SOURCE, cvedb.CVE_DATA_AS_OF),
            (["listed: " + ", ".join(counted)] if counted else []) +
            (["ruled out: " + "; ".join(ruled_out)] if ruled_out else []),
            "Upgrade to %s." % target))
    return out


def counts(findings):
    c = {s: 0 for s in SEVERITIES}
    for f in findings:
        c[f["severity"]] += 1
    return c
