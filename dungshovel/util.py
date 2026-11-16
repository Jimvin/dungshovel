"""Small helpers shared across modules: encoding, host parsing, protobuf, time."""

import base64
import datetime
import json
import re

_HOST_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
_HOSTPORT_RE = re.compile(r"^([^:/\s\[\]]+):(\d{1,5})(?:[_/;].*)?$")
_V6_HOSTPORT_RE = re.compile(r"^\[([0-9A-Fa-f:.]+)\]:(\d{1,5})")


def join_path(parent, child):
    if parent == "/":
        return "/" + child
    return parent + "/" + child


def parent_path(path):
    if path == "/":
        return None
    parent = path.rsplit("/", 1)[0]
    return parent or "/"


def basename(path):
    return path.rsplit("/", 1)[-1]


def format_host(host):
    """Bracket IPv6 literals for use in host:port strings and URLs."""
    if ":" in host and not host.startswith("["):
        return "[%s]" % host
    return host


def parse_target(target, default_port):
    """Parse 'host', 'host:port', '[v6]:port' or a bare IPv6 literal."""
    target = target.strip()
    if target.startswith("["):
        host, _, rest = target[1:].partition("]")
        port = rest[1:] if rest.startswith(":") else None
    elif target.count(":") == 1:
        host, port = target.split(":")
    else:
        host, port = target, None
    if not host:
        raise ValueError("empty host in target %r" % target)
    return host, int(port) if port else default_port


def _valid_port(p):
    return 0 < p < 65536


def parse_hostport(value):
    """Extract (host, port) from the common ways applications encode endpoints.

    Handles 'host:port', 'host:port_solr', '[v6]:port' and HBase server names
    'host,port,startcode'. Returns None if nothing plausible is found.
    """
    if not value:
        return None
    value = value.strip()
    m = _V6_HOSTPORT_RE.match(value)
    if m and _valid_port(int(m.group(2))):
        return m.group(1), int(m.group(2))
    if value.count(",") == 2:
        host, port, _ = value.split(",")
        if _HOST_RE.match(host) and port.isdigit() and _valid_port(int(port)):
            return host, int(port)
    m = _HOSTPORT_RE.match(value)
    if m and _HOST_RE.match(m.group(1)) and _valid_port(int(m.group(2))):
        return m.group(1), int(m.group(2))
    return None


def parse_url_endpoint(url):
    """Parse 'SCHEME://host:port...' into (scheme, host, port)."""
    scheme, sep, rest = url.partition("://")
    if not sep:
        return None
    hostport = rest.split("/", 1)[0].split("?", 1)[0]
    hp = parse_hostport(hostport)
    if hp:
        return scheme, hp[0], hp[1]
    if _HOST_RE.match(hostport):
        return scheme, hostport, None
    return None


def _is_printable(text):
    return all(c.isprintable() or c in "\r\n\t" for c in text)


def encode_data(raw, force_base64=False, max_bytes=None):
    """Return (value, encoding, truncated) for znode data."""
    if raw is None:
        return None, None, False
    truncated = max_bytes is not None and len(raw) > max_bytes
    if truncated:
        raw = raw[:max_bytes]
    if not force_base64:
        try:
            text = raw.decode("utf-8")
            if _is_printable(text):
                return text, "utf-8", truncated
        except UnicodeDecodeError:
            pass
    return base64.b64encode(raw).decode("ascii"), "base64", truncated


def try_json(raw):
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
    except (ValueError, UnicodeDecodeError):
        return None


def ms_to_iso(ms):
    """Epoch milliseconds to ISO 8601 UTC, or None."""
    try:
        ms = int(ms)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    try:
        dt = datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (ms % 1000)


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def session_hex(session_id):
    if session_id is None:
        return None
    if isinstance(session_id, str):
        s = session_id.strip().lower()
        try:
            session_id = int(s, 16) if s.startswith("0x") else int(s)
        except ValueError:
            return session_id
    return "0x%x" % (session_id & 0xFFFFFFFFFFFFFFFF)


def session_server_id(session_id):
    """ZooKeeper puts the myid of the server that created a session in the top byte."""
    hexed = session_hex(session_id)
    if not hexed or not hexed.startswith("0x"):
        return None
    return int(hexed, 16) >> 56


def decode_protobuf(data):
    """Minimal protobuf wire-format decoder.

    Returns {field_number: [values]} where varints are ints and length-delimited
    fields are bytes. Returns {} if the data does not look like protobuf.
    """
    fields = {}
    i, n = 0, len(data)
    try:
        while i < n:
            key, i = _varint(data, i)
            field, wire = key >> 3, key & 7
            if field == 0:
                return {}
            if wire == 0:
                val, i = _varint(data, i)
            elif wire == 1:
                val, i = data[i:i + 8], i + 8
            elif wire == 2:
                length, i = _varint(data, i)
                if i + length > n:
                    return {}
                val, i = data[i:i + length], i + length
            elif wire == 5:
                val, i = data[i:i + 4], i + 4
            else:
                return {}
            fields.setdefault(field, []).append(val)
    except IndexError:
        return {}
    return fields


def _varint(data, i):
    shift = result = 0
    while True:
        b = data[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, i
        shift += 7
        if shift > 63:
            raise IndexError("varint too long")


def pb_str(fields, num):
    vals = fields.get(num)
    if not vals or not isinstance(vals[0], (bytes, bytearray)):
        return None
    try:
        return vals[0].decode("utf-8")
    except UnicodeDecodeError:
        return None


def pb_int(fields, num):
    vals = fields.get(num)
    if not vals or not isinstance(vals[0], int):
        return None
    return vals[0]
