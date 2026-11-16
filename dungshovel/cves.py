"""Known Apache ZooKeeper CVEs and the versions they affect.

Source: https://zookeeper.apache.org/security.html (reviewed 2026-11-16).
Update CVE_DATA_AS_OF and the list when new advisories are published.

Version ranges are (min_inclusive, max_exclusive); None means unbounded. Where
the advisory's "Affected versions" header and its description disagree, the
range follows the description and the discrepancy is recorded in "note"
(and, where it changes the verdict for a release, in "disputed").

"requires" names a precondition that findings.py checks against what the
audit observed:
  client_port        reaching the client port is enough
  world_create       a client that may create znodes (default root ACL is open)
  read_access        a client that can read some znode
  quota_writable     write access under /zookeeper/quota
  sasl_client        SASL authentication on the client port
  audit_log          zookeeper.audit.enable=true
  quorum             a multi-server ensemble (not standalone)
  quorum_sasl        quorum.auth.enableSasl=true
  quorum_tls_fips    sslQuorum + zookeeper.fips-mode + hostname verification
  tls                TLS with hostname verification
  digest_auth        digest ACLs in use
  admin_server       AdminServer reachable
  admin_ip_auth      AdminServer using IPAuthenticationProvider
  wchc_4lw           wchc/wchp four-letter words enabled
  client_library     affects the Java client library, not the server
  c_cli              affects the C CLI example shells, not the server
"""

CVE_DATA_AS_OF = "2026-11-16"
CVE_SOURCE = "https://zookeeper.apache.org/security.html"

# Current release per maintained line (https://zookeeper.apache.org/releases.html).
LATEST = {(3, 9): "3.9.7", (3, 8): "3.8.8"}
MAINTAINED_LINES = set(LATEST)

CVES = [
    {"id": "CVE-2026-115448",
     "title": "Unbounded recursion in snapshot serialization lets a client render the server unrecoverable",
     "severity": "high", "cvss": 8.7,
     "affected": [("3.8.0", "3.8.8"), ("3.9.0", "3.9.7")], "fixed": ["3.8.8", "3.9.7"],
     "note": "The advisory header lists 3.8.8 and 3.9.7 as affected; they are fixed, as its description says.",
     "requires": "world_create", "jira": "ZOOKEEPER-5097"},
    {"id": "CVE-2026-100302",
     "title": "Denial of service through unbounded memory allocation in multiRead request handling",
     "severity": "important", "cvss": 7.1,
     "affected": [("3.7.0", "3.8.8"), ("3.9.0", "3.9.7")], "fixed": ["3.8.8", "3.9.7"],
     "note": "Header lists 3.8/3.9 only; the description says 3.7.0 through 3.9.6 are affected.",
     "requires": "read_access"},
    {"id": "CVE-2026-100300",
     "title": "Persistent denial of service via unvalidated quota-node data crashing the transaction-apply thread",
     "severity": "important", "cvss": 8.7,
     "affected": [(None, "3.8.8"), ("3.9.0", "3.9.7")], "fixed": ["3.8.8", "3.9.7"],
     "note": "Header lists 3.8/3.9 only; the description says all versions up to 3.9.6 are affected.",
     "requires": "quota_writable"},
    {"id": "CVE-2026-100252",
     "title": "SASL authorization bypass on the client port through a client-supplied authorization ID",
     "severity": "important", "cvss": 8.6,
     "affected": [("3.8.0", "3.8.8"), ("3.9.0", "3.9.7")], "fixed": ["3.8.8", "3.9.7"],
     "requires": "sasl_client"},
    {"id": "CVE-2026-100235",
     "title": "setACL race lets operations commit after an ACL revocation",
     "severity": "moderate", "cvss": 6.0,
     "affected": [(None, "3.8.8"), ("3.9.0", "3.9.7")], "fixed": ["3.8.8", "3.9.7"],
     "note": "Header lists 3.8/3.9 only; the description says all versions up to 3.9.6 are affected.",
     "requires": "client_port"},
    {"id": "CVE-2026-84501",
     "title": "Operational log forgery via newline injection in EnsembleAuthenticationProvider",
     "severity": "moderate",
     "affected": [("3.8.0", "3.8.7"), ("3.9.0", "3.9.6")], "fixed": ["3.8.7", "3.9.6"],
     "requires": "client_port"},
    {"id": "CVE-2026-84439",
     "title": "Audit log injection via unsanitized digest user names and ACL ids",
     "severity": "important",
     "affected": [("3.8.0", "3.8.7"), ("3.9.0", "3.9.6")], "fixed": ["3.8.7", "3.9.6"],
     "requires": "audit_log"},
    {"id": "CVE-2026-79993",
     "title": "Missing ACL check on deleteContainer lets unauthenticated clients delete empty znodes",
     "severity": "critical",
     "affected": [("3.8.0", "3.8.7"), ("3.9.0", "3.9.6")], "fixed": ["3.8.7", "3.9.6"],
     "requires": "client_port"},
    {"id": "CVE-2026-59969",
     "title": "Quorum TLS skips peer hostname verification in FIPS mode",
     "severity": "important",
     "affected": [("3.8.0", "3.8.7"), ("3.9.0", "3.9.6")], "fixed": ["3.8.7", "3.9.6"],
     "requires": "quorum_tls_fips"},
    {"id": "CVE-2026-59739",
     "title": "Path disclosure via SetWatches reconnect replay (incomplete fix for CVE-2024-23944)",
     "severity": "critical",
     "affected": [("3.8.0", "3.8.7"), ("3.9.0", "3.9.6")], "fixed": ["3.8.7", "3.9.6"],
     "requires": "client_port"},
    {"id": "CVE-2026-24308",
     "title": "Client configuration values (including secrets) logged at INFO level",
     "severity": "important",
     "affected": [("3.8.0", "3.8.6"), ("3.9.0", "3.9.5")], "fixed": ["3.8.6", "3.9.5"],
     "requires": "client_library"},
    {"id": "CVE-2026-24281",
     "title": "Reverse-DNS fallback enables hostname verification bypass in ZKTrustManager",
     "severity": "important",
     "affected": [("3.8.0", "3.8.6"), ("3.9.0", "3.9.5")], "fixed": ["3.8.6", "3.9.5"],
     "requires": "tls", "jira": "ZOOKEEPER-4986"},
    {"id": "CVE-2025-58457",
     "title": "Insufficient permission check in AdminServer snapshot/restore commands",
     "severity": "moderate",
     "affected": [("3.9.0", "3.9.4")], "fixed": ["3.9.4"],
     "requires": "admin_server"},
    {"id": "CVE-2024-51504",
     "title": "AdminServer IP-based authentication bypass via spoofed X-Forwarded-For",
     "severity": "important",
     "affected": [("3.9.0", "3.9.3")], "fixed": ["3.9.3"],
     "requires": "admin_ip_auth"},
    {"id": "CVE-2024-23944",
     "title": "Path disclosure in persistent watcher handling (missing ACL check)",
     "severity": "critical",
     "affected": [("3.6.0", "3.8.4"), ("3.9.0", "3.9.2")], "fixed": ["3.8.4", "3.9.2"],
     "requires": "client_port"},
    {"id": "CVE-2023-44981",
     "title": "Authorization bypass in SASL quorum peer authentication",
     "severity": "critical",
     "affected": [(None, "3.7.2"), ("3.8.0", "3.8.3"), ("3.9.0", "3.9.1")], "fixed": ["3.7.2", "3.8.3", "3.9.1"],
     "requires": "quorum_sasl"},
    {"id": "CVE-2019-0201",
     "title": "getACL discloses unsalted digest password hashes without a permission check",
     "severity": "critical",
     "affected": [(None, "3.4.14"), ("3.5.0", "3.5.5")], "fixed": ["3.4.14", "3.5.5"],
     "requires": "digest_auth", "jira": "ZOOKEEPER-1392"},
    {"id": "CVE-2018-8012",
     "title": "No authentication when a server joins the quorum",
     "severity": "critical",
     "affected": [(None, "3.4.10"), ("3.5.0", "3.5.4")], "fixed": ["3.4.10", "3.5.4"],
     "requires": "quorum", "jira": "ZOOKEEPER-1045"},
    {"id": "CVE-2017-5637",
     "title": "CPU exhaustion via wchp/wchc four-letter words",
     "severity": "moderate",
     "affected": [("3.4.0", "3.4.10"), ("3.5.0", "3.5.3")], "fixed": ["3.4.10", "3.5.3"],
     "requires": "wchc_4lw", "jira": "ZOOKEEPER-2693"},
    {"id": "CVE-2016-5017",
     "title": "Buffer overflow in the C client CLI shells (cli_st, cli_mt)",
     "severity": "moderate",
     "affected": [("3.4.0", "3.4.9"), ("3.5.0", "3.5.3")], "fixed": ["3.4.9", "3.5.3"],
     "requires": "c_cli"},
]


def parse_version(v):
    """'3.9.2', '3.5.4-beta' or a full srvr version string -> (3, 9, 2)."""
    import re
    m = re.match(r"\s*(\d+)\.(\d+)\.(\d+)", v or "")
    return tuple(int(x) for x in m.groups()) if m else None


def _in(version, ranges):
    for lo, hi in ranges or []:
        if (lo is None or version >= parse_version(lo)) and (hi is None or version < parse_version(hi)):
            return True
    return False


def affecting(version):
    """CVEs whose affected ranges include version. Returns [(cve, disputed: bool)]."""
    v = parse_version(version) if isinstance(version, str) else version
    if not v:
        return []
    out = []
    for c in CVES:
        if _in(v, c["affected"]):
            out.append((c, False))
        elif _in(v, c.get("disputed")):
            out.append((c, True))
    return out


def format_ranges(ranges):
    parts = []
    for lo, hi in ranges:
        if lo is None:
            parts.append("< %s" % hi)
        else:
            parts.append("%s ≤ v < %s" % (lo, hi))
    return ", ".join(parts)


def upgrade_target(version):
    """Release to recommend: the current release of a maintained line, else the latest line."""
    v = parse_version(version) if isinstance(version, str) else version
    if v and v[:2] in LATEST:
        return LATEST[v[:2]]
    return LATEST[max(LATEST)]
