"""Server fingerprinting via four-letter-word (4lw) commands and the AdminServer.

Since ZooKeeper 3.5.3 only 'srvr' is whitelisted by default, so most 4lw
commands will usually report 'disabled'. The AdminServer (default port 8080)
exposes the same information over HTTP and is often left enabled.
"""

import json
import logging
import re
import socket
import ssl
import urllib.error
import urllib.request

from dungshovel.util import format_host, session_hex, session_server_id

log = logging.getLogger(__name__)

DISABLED_MARKER = "is not executed because it is not in the whitelist"
MAX_RESPONSE = 16 * 1024 * 1024

FINGERPRINT_COMMANDS = ["ruok", "srvr", "conf", "envi", "mntr", "isro"]
CLIENT_COMMANDS = ["cons", "stat", "dump"]
WATCH_COMMANDS = ["wchc"]

# 4lw name -> AdminServer command name (ZooKeeper 3.5+)
ADMIN_COMMANDS = {
    "ruok": "ruok",
    "srvr": "server_stats",
    "conf": "configuration",
    "envi": "environment",
    "mntr": "monitor",
    "isro": "is_read_only",
    "cons": "connections",
    "dump": "dump",
    "wchc": "watches",
}


def fourlw(host, port, cmd, timeout, ssl_ctx=None):
    """Send a four-letter-word command and return a result dict."""
    res = {"command": cmd, "via": "4lw", "status": None, "local_port": None,
           "output": None, "error": None}
    try:
        with socket.create_connection((host, port), timeout=timeout) as raw:
            sock = ssl_ctx.wrap_socket(raw, server_hostname=host) if ssl_ctx else raw
            res["local_port"] = sock.getsockname()[1]
            sock.sendall(cmd.encode("ascii"))
            chunks, total = [], 0
            while total < MAX_RESPONSE:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            if sock is not raw:
                sock.close()
    except (OSError, ssl.SSLError) as e:
        res["status"] = "error"
        res["error"] = str(e)
        return res
    out = b"".join(chunks).decode("utf-8", "replace")
    res["output"] = out
    if DISABLED_MARKER in out:
        res["status"] = "disabled"
    elif not out.strip():
        res["status"] = "no-response"
    else:
        res["status"] = "ok"
    return res


def admin(host, port, cmd, timeout, use_https=False, ssl_ctx=None):
    """Query an AdminServer command and return a result dict."""
    name = ADMIN_COMMANDS[cmd]
    url = "%s://%s:%d/commands/%s" % ("https" if use_https else "http", format_host(host), port, name)
    res = {"command": cmd, "via": "admin", "url": url, "status": None, "output": None, "error": None}
    try:
        kwargs = {"timeout": timeout}
        if use_https:
            kwargs["context"] = ssl_ctx or ssl.create_default_context()
        with urllib.request.urlopen(url, **kwargs) as resp:
            body = resp.read(MAX_RESPONSE)
    except urllib.error.HTTPError as e:
        res["status"] = "unauthorized" if e.code in (401, 403) else "error"
        res["error"] = "HTTP %d" % e.code
        return res
    except (urllib.error.URLError, OSError, ssl.SSLError) as e:
        res["status"] = "error"
        res["error"] = str(getattr(e, "reason", e))
        return res
    try:
        data = json.loads(body)
    except ValueError:
        res["status"] = "error"
        res["error"] = "response is not JSON (not a ZooKeeper AdminServer?)"
        return res
    res["output"] = data
    if isinstance(data, dict) and data.get("error"):
        res["status"] = "error"
        res["error"] = data["error"]
    else:
        res["status"] = "ok"
    return res


# --- text parsers ---------------------------------------------------------

def parse_colon_kv(text):
    out = {}
    for line in text.splitlines():
        key, sep, val = line.partition(":")
        if sep and key.strip():
            out[key.strip()] = val.strip()
    return out


def parse_equals_kv(text):
    out = {}
    for line in text.splitlines():
        key, sep, val = line.partition("=")
        if sep and key.strip():
            out[key.strip()] = val.strip()
    return out


def parse_mntr(text):
    out = {}
    for line in text.splitlines():
        key, sep, val = line.partition("\t")
        if sep:
            out[key.strip()] = val.strip()
    return out


_CONN_RE = re.compile(r"^\s*/(?P<addr>\S+?)\[(?P<interest>\d+)\]\((?P<kv>.*)\)\s*$")


def parse_connection_line(line):
    m = _CONN_RE.match(line)
    if not m:
        return None
    addr = m.group("addr")
    host, _, port = addr.rpartition(":")
    kv = {}
    for item in m.group("kv").split(","):
        k, sep, v = item.partition("=")
        if sep:
            kv[k.strip()] = v.strip()
    conn = {
        "address": host,
        "port": int(port) if port.isdigit() else None,
        "session_id": session_hex(kv["sid"]) if "sid" in kv else None,
        "established": _maybe_int(kv.get("est")),
        "last_response": _maybe_int(kv.get("lresp")),
        "last_operation": kv.get("lop"),
        "session_timeout_ms": _maybe_int(kv.get("to")),
        "packets_received": _maybe_int(kv.get("recved")),
        "packets_sent": _maybe_int(kv.get("sent")),
        "raw": kv,
    }
    return conn


def parse_cons(text):
    conns = []
    for line in text.splitlines():
        conn = parse_connection_line(line)
        if conn:
            conns.append(conn)
    return conns


def parse_stat(text):
    """'stat' output: server info plus a 'Clients:' section."""
    clients, info_lines, in_clients = [], [], False
    for line in text.splitlines():
        if line.strip() == "Clients:":
            in_clients = True
            continue
        if in_clients:
            if not line.strip():
                in_clients = False
                continue
            conn = parse_connection_line(line)
            if conn:
                clients.append(conn)
        else:
            info_lines.append(line)
    return {"info": parse_colon_kv("\n".join(info_lines)), "clients": clients}


def parse_dump(text):
    """'dump' output (leader/standalone only): live sessions and their ephemerals."""
    sessions, ephemerals = set(), {}
    section, current = None, None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("SessionTracker dump"):
            section = "sessions"
        elif stripped.startswith("Sessions with Ephemerals"):
            section = "ephemerals"
        elif stripped.startswith("Connections dump"):
            section = None
        elif section == "sessions" and re.match(r"^0x[0-9a-fA-F]+$", stripped):
            sessions.add(session_hex(stripped))
        elif section == "ephemerals":
            m = re.match(r"^(0x[0-9a-fA-F]+):$", stripped)
            if m:
                current = session_hex(m.group(1))
                sessions.add(current)
                ephemerals.setdefault(current, [])
            elif stripped.startswith("/") and current:
                ephemerals[current].append(stripped)
    return {"sessions": sorted(sessions), "ephemerals": ephemerals}


def parse_wchc(text):
    """'wchc' output: session id followed by indented watched paths."""
    watches, current = {}, None
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r"^0x[0-9a-fA-F]+$", stripped):
            current = session_hex(stripped)
            watches.setdefault(current, [])
        elif stripped.startswith("/") and current:
            watches[current].append(stripped)
    return watches


def _maybe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --- AdminServer JSON normalisation --------------------------------------

def _admin_connections(data):
    conns = []
    for key, secure in (("connections", False), ("secure_connections", True)):
        for c in data.get(key) or []:
            addr = (c.get("remote_socket_address") or "").lstrip("/")
            host, _, port = addr.rpartition(":")
            conns.append({
                "address": host or addr,
                "port": int(port) if port.isdigit() else None,
                "session_id": session_hex(c["session_id"]) if c.get("session_id") not in (None, 0, "0") else None,
                "established": _date_or_int(c.get("established")),
                "last_response": _date_or_int(c.get("last_response_time")),
                "last_operation": c.get("last_operation"),
                "session_timeout_ms": c.get("session_timeout"),
                "packets_received": c.get("packets_received"),
                "packets_sent": c.get("packets_sent"),
                "secure": secure,
                "raw": c,
            })
    return conns


def _date_or_int(v):
    if isinstance(v, int):
        return v
    return v  # AdminServer formats some timestamps as strings; keep as-is


def _admin_dump(data):
    ephemerals = {}
    for sid, paths in (data.get("session_id_to_ephemeral_paths") or {}).items():
        ephemerals[session_hex(sid)] = list(paths)
    sessions = set(ephemerals)
    for sids in (data.get("expiry_time_to_session_ids") or {}).values():
        for sid in sids:
            sessions.add(session_hex(sid))
    return {"sessions": sorted(sessions), "ephemerals": ephemerals}


# --- probe orchestration --------------------------------------------------

def probe(host, port, conn_opts, use_4lw=True, admin_port=None, admin_https=False,
          clients=True, watches=False):
    """Run the configured probes and return raw results plus parsed info."""
    ssl_ctx = conn_opts.ssl_context() if conn_opts.tls else None
    commands = list(FINGERPRINT_COMMANDS)
    if clients:
        commands += CLIENT_COMMANDS
        if watches:
            commands += WATCH_COMMANDS

    raw = {"4lw": {}, "admin": {}}
    if use_4lw:
        for cmd in commands:
            raw["4lw"][cmd] = fourlw(host, port, cmd, conn_opts.timeout, ssl_ctx)
            log.debug("4lw %s on %s:%d -> %s", cmd, host, port, raw["4lw"][cmd]["status"])
    if admin_port:
        admin_ctx = ssl_ctx if admin_https and conn_opts.tls else None
        for cmd in commands:
            if cmd not in ADMIN_COMMANDS:
                continue
            raw["admin"][cmd] = admin(host, admin_port, cmd, conn_opts.timeout, admin_https, admin_ctx)
            log.debug("admin %s on %s:%d -> %s", cmd, host, admin_port, raw["admin"][cmd]["status"])
            if cmd == "ruok" and raw["admin"][cmd]["status"] == "error" and \
                    "not JSON" not in (raw["admin"][cmd]["error"] or "") and \
                    "HTTP" not in (raw["admin"][cmd]["error"] or ""):
                # Port unreachable: don't wait for every other command to time out.
                break

    return {"raw": raw, **interpret(raw)}


def _ok(raw, via, cmd):
    r = raw.get(via, {}).get(cmd)
    return r["output"] if r and r["status"] == "ok" else None


def interpret(raw):
    info = {}
    connections, conn_source = None, None
    dump, dump_source = None, None
    watches = None

    srvr = _ok(raw, "4lw", "srvr")
    if srvr:
        kv = parse_colon_kv(srvr)
        info["version"] = kv.get("Zookeeper version")
        info["mode"] = kv.get("Mode")
        info["zxid"] = kv.get("Zxid")
        info["node_count"] = _maybe_int(kv.get("Node count"))
        info["connection_count"] = _maybe_int(kv.get("Connections"))
    a_srvr = _ok(raw, "admin", "srvr")
    if a_srvr:
        ss = a_srvr.get("server_stats") or {}
        info.setdefault("version", a_srvr.get("version") or ss.get("version"))
        info.setdefault("mode", ss.get("server_state"))
        info.setdefault("node_count", a_srvr.get("node_count") or ss.get("node_count"))
        info.setdefault("connection_count", a_srvr.get("num_alive_client_connections")
                        or ss.get("num_alive_client_connections"))

    mntr = _ok(raw, "4lw", "mntr")
    mon = parse_mntr(mntr) if mntr else (_ok(raw, "admin", "mntr") or {})
    if mon:
        info.setdefault("version", mon.get("zk_version") or mon.get("version"))
        info.setdefault("mode", mon.get("zk_server_state") or mon.get("server_state"))
        info["quorum_size"] = mon.get("zk_quorum_size") or mon.get("quorum_size")
        info["followers"] = mon.get("zk_followers") or mon.get("followers")
        info["ephemerals_count"] = mon.get("zk_ephemerals_count") or mon.get("ephemerals_count")
        info["watch_count"] = mon.get("zk_watch_count") or mon.get("watch_count")

    envi = _ok(raw, "4lw", "envi")
    env = parse_equals_kv(envi) if envi else (_ok(raw, "admin", "envi") or {})
    if env:
        info["host_name"] = env.get("host.name")
        info["java_version"] = env.get("java.version")
        info["java_vendor"] = env.get("java.vendor")
        info["os"] = " ".join(x for x in (env.get("os.name"), env.get("os.version"), env.get("os.arch")) if x)
        info["user"] = env.get("user.name")
        info["working_dir"] = env.get("user.dir")
        info.setdefault("version", env.get("zookeeper.version"))

    conf = _ok(raw, "4lw", "conf")
    cfg = parse_equals_kv(conf) if conf else (_ok(raw, "admin", "conf") or {})
    if cfg:
        info["client_port"] = cfg.get("clientPort") or cfg.get("client_port")
        info["secure_client_port"] = cfg.get("secureClientPort") or cfg.get("secure_client_port")
        info["data_dir"] = cfg.get("dataDir") or cfg.get("data_dir")
        info["data_log_dir"] = cfg.get("dataLogDir") or cfg.get("data_log_dir")
        info["server_id"] = cfg.get("serverId") or cfg.get("server_id")
        members = {k: v for k, v in cfg.items() if re.match(r"^server\.\d+$", k)}
        if members:
            info["ensemble"] = members
        elif isinstance(cfg.get("server_list"), (list, dict)):
            info["ensemble"] = cfg["server_list"]

    isro = _ok(raw, "4lw", "isro")
    if isro:
        info["read_only"] = isro.strip() == "ro"
    a_isro = _ok(raw, "admin", "isro")
    if a_isro and "read_only" not in info:
        info["read_only"] = a_isro.get("read_only")

    if info.get("version"):
        info["version_short"] = info["version"].split(",")[0].split("-")[0].strip()

    cons = _ok(raw, "4lw", "cons")
    if cons:
        connections, conn_source = parse_cons(cons), "4lw:cons"
    elif _ok(raw, "admin", "cons"):
        connections, conn_source = _admin_connections(_ok(raw, "admin", "cons")), "admin:connections"
    elif _ok(raw, "4lw", "stat"):
        connections, conn_source = parse_stat(_ok(raw, "4lw", "stat"))["clients"], "4lw:stat"

    d = _ok(raw, "4lw", "dump")
    if d:
        dump, dump_source = parse_dump(d), "4lw:dump"
    elif _ok(raw, "admin", "dump"):
        dump, dump_source = _admin_dump(_ok(raw, "admin", "dump")), "admin:dump"

    w = _ok(raw, "4lw", "wchc")
    if w:
        watches = parse_wchc(w)
    elif _ok(raw, "admin", "wchc"):
        watches = {session_hex(k): v for k, v in (_ok(raw, "admin", "wchc").get("session_id_to_watched_paths") or {}).items()}

    exposure = {
        "4lw": {cmd: r["status"] for cmd, r in raw["4lw"].items()},
        "admin": {cmd: r["status"] for cmd, r in raw["admin"].items()},
    }
    for c in connections or []:
        if c.get("session_id"):
            c["server_id"] = session_server_id(c["session_id"])

    return {
        "info": {k: v for k, v in info.items() if v not in (None, "", {})},
        "exposure": exposure,
        "connections": connections,
        "connections_source": conn_source,
        "dump": dump,
        "dump_source": dump_source,
        "watches": watches,
    }
