"""Render audit results as JSON or as a rich (coloured, tabular) text report.

Colour follows audit severity rather than decoration: red means exposed or
unprotected, yellow means worth a look, green means restricted or disabled.
Colour is dropped automatically for files, pipes and NO_COLOR.
"""

import io
import json

from rich import box
from rich.console import Console
from rich.padding import Padding
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.theme import Theme
from rich.tree import Tree

from dungshovel import __version__
from dungshovel import cves as cvedb
from dungshovel import findings as findings_mod

TEXT_DATA_LIMIT = 400
FILE_WIDTH = 200

THEME = Theme({
    "bad": "bold red",
    "warn": "yellow",
    "good": "green",
    "muted": "grey50",
    "key": "bold",
    "ident": "cyan",
    "path": "bright_blue",
    "title": "bold magenta",
    "app": "bold cyan",
    "ev.connected": "bold green",
    "ev.ephemeral": "cyan",
    "ev.persistent": "yellow",
    "ev.registration": "yellow",
    "ev.acl": "magenta",
})

SENSITIVE_COMMANDS = set(findings_mod.SENSITIVE_COMMANDS)
SEVERITY_STYLE = {"high": "bold white on red", "medium": "bold black on yellow",
                  "low": "bold black on cyan", "info": "grey50"}


# --------------------------------------------------------------------------
# JSON
# --------------------------------------------------------------------------

def _strip_internal(obj):
    if isinstance(obj, dict):
        return {k: _strip_internal(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip_internal(v) for v in obj]
    if isinstance(obj, set):
        return sorted(obj)
    return obj


def to_json(report):
    return json.dumps(_strip_internal(report), indent=2, default=str) + "\n"


# --------------------------------------------------------------------------
# Text
# --------------------------------------------------------------------------

def make_console(file=None, color="auto", width=None):
    """Console for stdout (color='auto' follows the terminal) or for a file (plain)."""
    kwargs = {"theme": THEME, "highlight": False, "soft_wrap": False}
    if file is not None:
        kwargs["file"] = file
    if color == "never":
        kwargs["no_color"] = True
        kwargs["color_system"] = None
    elif color == "always":
        kwargs["force_terminal"] = True
    console = Console(**kwargs)
    if width:
        console.width = width
    elif not console.is_terminal:
        console.width = FILE_WIDTH  # don't wrap tables at 80 columns in pipes and files
    return console


def to_text(report, color="never", width=FILE_WIDTH):
    buf = io.StringIO()
    render(report, make_console(buf, color=color, width=width))
    text = buf.getvalue()
    if color == "never":
        text = "\n".join(line.rstrip() for line in text.split("\n"))
    return text


def print_report(report, color="auto"):
    """Print to stdout: live rich output on a terminal, trimmed plain text otherwise."""
    return print_with(lambda console: render(report, console), color)


def print_with(draw, color="auto"):
    """Run draw(console) on a terminal, or render to trimmed text for pipes/files."""
    console = make_console(color=color)
    if console.is_terminal:
        return draw(console)
    buf = io.StringIO()
    out_color = "always" if color == "always" else "never"
    result = draw(make_console(buf, color=out_color, width=FILE_WIDTH))
    text = buf.getvalue()
    if out_color == "never":
        text = "\n".join(line.rstrip() for line in text.split("\n"))
    console.file.write(text)
    return result


def render(report, console):
    header = Text.assemble(("dungshovel ", "title"), (__version__, "muted"),
                           ("  ·  ZooKeeper audit  ·  ", "muted"), (report["generated"], "muted"))
    console.print(header)
    for t in report["targets"]:
        console.print()
        _render_target(t, console)
    if len(report["targets"]) > 1:
        console.print()
        console.print(_overview(report["targets"]))


def _overview(targets):
    table = _table("Overview", ("Target", ""), ("Version", ""), ("Mode", ""), ("Connection", ""),
                   ("Znodes", "right"), ("World-writable", "right"), ("Apps", "right"), ("Clients", "right"),
                   ("Findings", ""))
    for t in targets:
        info = ((t.get("server") or {}).get("info")) or {}
        zn = t.get("znodes") or {}
        acl = zn.get("acl_summary") or {}
        conn = t.get("connection") or {}
        table.add_row(
            Text(t["target"], "key"), info.get("version_short", "-"), str(info.get("mode", "-")),
            _conn_badge(conn),
            str(zn.get("count", "-")),
            _count(acl.get("world_write"), "bad") if acl else "-",
            str(len(((t.get("fingerprint") or {}).get("applications")) or [])),
            str(len(((t.get("clients") or {}).get("hosts")) or [])),
            _findings_summary(t.get("findings") or []),
        )
    return table


# --- building blocks -------------------------------------------------------

def _table(title=None, *columns, **kwargs):
    table = Table(title=title, title_justify="left", title_style="title", box=box.SIMPLE_HEAD,
                  header_style="bold", pad_edge=False, show_edge=False, **kwargs)
    for name, justify in columns:
        table.add_column(name, justify=justify or "left", overflow="fold")
    return table


def _kv_table(rows):
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="key", no_wrap=True)
    grid.add_column(overflow="fold")
    for k, v in rows:
        grid.add_row(k, v if isinstance(v, Text) else Text(str(v)))
    return grid


def _count(n, style_if_nonzero):
    if n is None:
        return Text("-", "muted")
    return Text(str(n), style_if_nonzero if n else "good")


def _status(cmd, status):
    if status == "ok":
        return Text("open", "bad" if cmd in SENSITIVE_COMMANDS else "warn")
    if status in ("disabled", "unauthorized"):
        return Text(status, "good")
    if status is None:
        return Text("-", "muted")
    return Text(status, "muted")


def _conn_badge(conn):
    if not conn.get("ok"):
        return Text("failed", "bad")
    if conn.get("auth_schemes"):
        label = "auth: " + ",".join(conn["auth_schemes"])
        style = "good"
    else:
        label, style = "unauthenticated", "warn"
    return Text.assemble((label, style), (" · ", "muted"),
                         ("TLS", "good") if conn.get("tls") else ("plaintext", "warn"))


def _evidence(evidence):
    out = Text()
    for i, e in enumerate(evidence):
        if i:
            out.append(" ")
        out.append(e, "ev." + e)
    return out


def _short(value, width=110):
    if isinstance(value, (dict, list)):
        value = json.dumps(_strip_internal(value), default=str)
    value = str(value)
    return value if len(value) <= width else value[:width - 1] + "…"


def _pad(renderable, pad):
    return Padding(renderable, pad, expand=False)


def _section(console, title):
    console.print()
    console.print(Text(title, "title"))


# --- per target ------------------------------------------------------------

def _render_target(t, console):
    console.print(Rule(Text(" %s " % t["target"], "bold"), style="magenta", align="left"),
                  width=min(console.width, 120))
    conn = t.get("connection") or {}
    srv = t.get("server") or {}
    info = srv.get("info") or {}
    zn = t.get("znodes")

    # Summary
    rows = []
    conn_text = _conn_badge(conn)
    if conn.get("ok"):
        conn_text.append("  session %s" % conn.get("session_id"), "muted")
    else:
        conn_text.append("  %s" % conn.get("error"), "muted")
    rows.append(("Connection", conn_text))
    if info:
        server = Text("ZooKeeper %s" % info.get("version_short", "?"), "bold")
        if info.get("mode"):
            server.append("  %s" % info["mode"])
        if info.get("read_only"):
            server.append("  READ-ONLY", "warn")
        rows.append(("Server", server))
    if zn:
        z = Text("%d enumerated from %s" % (zn["count"], zn["basepath"]))
        if zn["denied"]:
            z.append("  ·  %d not readable" % len(zn["denied"]), "warn")
        if zn["truncated"]:
            z.append("  ·  TRUNCATED at --max-nodes", "bad")
        elif zn.get("aborted"):
            z.append("  ·  %s" % zn["aborted"], "bad")
        rows.append(("Znodes", z))
        acl = zn.get("acl_summary")
        if acl:
            a = Text()
            for label, key, style in (("world-readable", "world_read", "warn"),
                                      ("world-writable", "world_write", "bad"),
                                      ("world-admin", "world_admin", "bad"),
                                      ("ACL unreadable", "acl_unreadable", "warn")):
                if a:
                    a.append("  ·  ", "muted")
                a.append_text(_count(acl[key], style))
                a.append(" " + label)
            rows.append(("ACL exposure", a))
    console.print(_pad(_kv_table(rows), (1, 0, 0, 2)))

    if t.get("findings") is not None:
        _render_findings(console, t["findings"])
    _render_server(console, t, info, srv.get("exposure") or {})
    if t.get("fingerprint") is not None:
        _render_apps(console, t["fingerprint"], zn)
    if t.get("clients") is not None:
        _render_clients(console, t["clients"])
    if zn and zn.get("nodes") is not None:
        _render_znodes(console, zn["nodes"])


def _severity_badge(sev):
    return Text(" %s " % sev.upper(), SEVERITY_STYLE.get(sev, ""))


def _findings_summary(fs):
    c = findings_mod.counts(fs)
    out = Text()
    for sev in findings_mod.SEVERITIES[:3]:
        if out:
            out.append(" · ", "muted")
        out.append("%d %s" % (c[sev], sev), {"high": "bad", "medium": "warn", "low": "ident"}[sev]
                   if c[sev] else "muted")
    return out


def _render_findings(console, fs):
    _section(console, "Findings")
    if not fs:
        console.print(_pad(Text("No findings.", "good"), (0, 0, 1, 2)))
        return
    console.print(_pad(_findings_summary(fs), (0, 0, 1, 2)))
    table = _table(None, ("Severity", ""), ("Finding", ""), show_lines=True)
    table.columns[0].no_wrap = True
    for f in fs:
        body = Text()
        body.append(f["title"], "bold")
        body.append("  %s" % f["id"], "muted")
        body.append("\n" + f["detail"])
        for e in f["evidence"][:8]:
            body.append("\n  • ", "muted")
            body.append(e)
        if len(f["evidence"]) > 8:
            body.append("\n  … %d more (see JSON)" % (len(f["evidence"]) - 8), "muted")
        if f.get("recommendation"):
            body.append("\n→ ", "good")
            body.append(f["recommendation"], "good")
        table.add_row(_severity_badge(f["severity"]), body)
    console.print(_pad(table, (0, 0, 1, 2)))


def _render_server(console, t, info, exposure):
    detail_keys = (("version", "Version"), ("host_name", "Host name"), ("java_version", "Java"),
                   ("java_vendor", "Java vendor"), ("os", "OS"), ("user", "Runs as"),
                   ("working_dir", "Working dir"), ("data_dir", "Data dir"), ("data_log_dir", "Data log dir"),
                   ("client_port", "Client port"), ("secure_client_port", "Secure port"),
                   ("server_id", "Server id"), ("node_count", "Znodes"), ("connection_count", "Connections"),
                   ("ephemerals_count", "Ephemerals"), ("watch_count", "Watches"), ("quorum_size", "Quorum size"))
    rows = [(label, _short(info[k])) for k, label in detail_keys if info.get(k) not in (None, "")]
    blocks = []
    if rows:
        blocks.append(_kv_table(rows))

    ens = t.get("ensemble")
    if ens:
        table = _table("Ensemble  (from %s)" % ens["source"], ("Member", ""), ("Address", ""))
        for k, v in sorted(ens["members"].items()):
            table.add_row(k, v)
        blocks.append(table)

    cmds = list(dict.fromkeys(list(exposure.get("4lw", {})) + list(exposure.get("admin", {}))))
    if cmds:
        cols = [("Command", "")]
        if exposure.get("4lw"):
            cols.append(("4lw", ""))
        if exposure.get("admin"):
            cols.append(("AdminServer", ""))
        table = _table("Exposed commands", *cols)
        for cmd in cmds:
            row = [cmd]
            if exposure.get("4lw"):
                row.append(_status(cmd, exposure["4lw"].get(cmd)))
            if exposure.get("admin"):
                row.append(_status(cmd, exposure["admin"].get(cmd)))
            table.add_row(*row)
        blocks.append(table)

    if blocks:
        _section(console, "Server")
        for b in blocks:
            console.print(_pad(b, (0, 0, 1, 2)))


def _render_apps(console, fp, zn):
    apps = fp.get("applications") or []
    _section(console, "Applications (%d)" % len(apps))
    if apps:
        table = _table(None, ("Application", ""), ("Root", ""), ("Confidence", ""), ("Clients", "right"))
        for a in apps:
            conf = a["confidence"]
            table.add_row(Text(a["application"], "app"), Text(a["root"], "path"),
                          Text(conf, {"high": "good", "medium": "warn"}.get(conf, "muted")),
                          str(a["client_count"]))
        console.print(_pad(table, (0, 0, 1, 2)))

        tree = Tree(Text("Details", "muted"), guide_style="muted", hide_root=True)
        for a in apps:
            branch = tree.add(Text.assemble((a["application"], "app"), ("  ", ""), (a["root"], "path")))
            for k, v in (a.get("details") or {}).items():
                if v in (None, "", [], {}):
                    continue
                _detail(branch, k, v)
            if a.get("error"):
                branch.add(Text("extractor error: %s" % a["error"], "bad"))
        console.print(_pad(tree, (0, 0, 1, 2)))
    else:
        console.print(_pad(Text("none detected", "muted"), (0, 0, 1, 2)))

    un = fp.get("unclassified") or []
    if un:
        table = _table("Unclassified top-level znodes", ("Path", ""), ("Children", "right"), ("Modified", ""))
        for u in un:
            if u["num_children"] is None:
                table.add_row(Text(u["path"], "path"), Text("NoAuth", "warn"), "-")
            else:
                table.add_row(Text(u["path"], "path"), str(u["num_children"]), u["mtime"] or "-")
        console.print(_pad(table, (0, 0, 1, 2)))

    notes = []
    if zn and (zn["truncated"] or zn.get("aborted") or zn.get("depth_limited")):
        notes.append(Text("Enumeration was limited; applications deeper in the tree may be missed.", "warn"))
    r = fp.get("reads") or {}
    if r:
        notes.append(Text("Fingerprint reads: %d  ·  denied: %d  ·  budget: %d" % (
            r["reads"], r["denied"], r["budget"]), "muted"))
    for n in notes:
        console.print(_pad(n, (0, 0, 0, 2)))


_RISKY_DETAILS = {"scram_users_world_readable", "world_readable"}


def _detail(branch, key, value):
    label = key.replace("_", " ")
    if isinstance(value, list) and value and isinstance(value[0], dict):
        sub = branch.add(Text.assemble((label, "key"), ("  %d" % len(value), "muted")))
        for item in value[:10]:
            sub.add(Text(_short(item, 140)))
        if len(value) > 10:
            sub.add(Text("… %d more (see JSON)" % (len(value) - 10), "muted"))
    elif isinstance(value, dict) and value and all(isinstance(v, (dict, list, str, int, bool, type(None)))
                                                   for v in value.values()):
        sub = branch.add(Text(label, "key"))
        for k, v in list(value.items())[:20]:
            _detail(sub, str(k), v)
    else:
        if isinstance(value, bool):
            text = Text(str(value).lower(), "bad" if value and key in _RISKY_DETAILS else "")
        elif isinstance(value, list):
            text = Text(", ".join(str(v) for v in value[:20]) + (" …" if len(value) > 20 else ""))
        else:
            text = Text(_short(value, 140))
        branch.add(Text.assemble((label + ": ", "key"), text))


def _render_clients(console, cl):
    hosts = cl["hosts"]
    _section(console, "Clients (%d distinct identities)" % len(hosts))
    if hosts:
        table = _table(None, ("Identity", ""), ("Evidence", ""), ("Ports", ""), ("First registered", ""),
                       ("Applications / roles", ""))
        for h in hosts:
            apps = Text("; ".join(h["applications"]))
            if h["roles"]:
                if apps:
                    apps.append("  ")
                apps.append(", ".join(h["roles"]), "muted")
            table.add_row(Text(h["identity"], "ident"), _evidence(h["evidence"]),
                          ",".join(h["ports"]) or Text("-", "muted"),
                          h["first_registered"] or Text("-", "muted"), apps or Text("-", "muted"))
        console.print(_pad(table, (0, 0, 1, 2)))

    conns = [c for c in cl["connections"] if not c.get("self")]
    src = cl.get("connections_source")
    title = ("Live connections to this server (%d, via %s)" % (len(conns), src) if src
             else "Live connections: unknown (cons/stat disabled, no AdminServer queried)")
    if not conns:
        console.print(_pad(Text(title, "title" if src else "warn"), (0, 0, 1, 2)))
    else:
        table = _table(title, ("Address", ""), ("Session", ""), ("SID", "right"), ("Established", ""),
                       ("Last response", ""), ("Ephemerals", "right"), ("Applications", ""))
        for c in conns:
            table.add_row(
                Text("%s:%s" % (c["address"], c["port"]), "ident"),
                c.get("session_id") or Text("none (4lw probe?)", "muted"),
                str(c["server_id"]) if c.get("server_id") is not None else "-",
                c.get("established_iso") or "-",
                Text(str(c.get("last_response_iso") or "-"), "muted"),
                str(c.get("ephemeral_count", "-")),
                ", ".join(c.get("applications") or []) or Text("-", "muted"))
        console.print(_pad(table, (0, 0, 1, 2)))

    remote = [s for s in cl["sessions"] if not s["connected_here"]]
    if remote:
        table = _table("Sessions owning ephemerals, not connected to this server (%d)" % len(remote),
                       ("Session", ""), ("Created by SID", "right"), ("Ephemerals", "right"),
                       ("Applications / sample", ""))
        for s in remote:
            table.add_row(s["session_id"], str(s["created_by_server_id"]), str(s["ephemeral_count"]),
                          ", ".join(s["applications"]) or
                          Text(s["ephemeral_sample"][0] if s["ephemeral_sample"] else "-", "path"))
        console.print(_pad(table, (0, 0, 1, 2)))

    if cl["acl_identities"]:
        table = _table("Identities in znode ACLs (%d)" % len(cl["acl_identities"]),
                       ("Identity", ""), ("Perms", ""), ("Znodes", "right"), ("Example", ""))
        for a in cl["acl_identities"]:
            table.add_row(Text("%s:%s" % (a["scheme"], a["id"]), "ident"), _perms(a["perms"]),
                          str(a["znodes"]), Text(a["sample"][0], "path"))
        console.print(_pad(table, (0, 0, 1, 2)))
    if cl.get("watches"):
        console.print(_pad(Text("Sessions with watches: %d" % len(cl["watches"]), "muted"), (0, 0, 1, 2)))


def _perms(perms, world=False):
    risky = world and any(c in perms for c in "wcda")
    return Text(perms, "bad" if risky else ("warn" if world else ""))


# --- znode tree ------------------------------------------------------------

def _acl_text(acl):
    if acl is None:
        return None
    out = Text()
    for i, a in enumerate(acl):
        if i:
            out.append(" ")
        if a["scheme"] == "world" and a["id"] == "anyone":
            style = "bad" if any(c in a["perms"] for c in "wcda") else "warn"
            out.append("world:" + a["perms"], style)
        else:
            out.append("%s:%s:" % (a["scheme"], _short(a["id"], 40)), "good")
            out.append(a["perms"], "good")
    return out


def _render_znodes(console, nodes):
    _section(console, "Znodes (%d)" % len(nodes))
    console.print(Text.assemble(
        ("  legend: ", "muted"), ("name", "path"), ("  ephemeral", "ident"), ("  NoAuth", "warn"),
        ("  world-writable ACL", "bad"), ("  world-readable ACL", "warn"), ("  restricted ACL", "good")))

    by_path = {n["path"]: n for n in nodes}
    kids = {}
    roots = []
    for path in sorted(by_path, key=lambda p: p.strip("/").split("/") if p != "/" else []):
        parent = path.rsplit("/", 1)[0] or "/" if path != "/" else None
        if parent in by_path:
            kids.setdefault(parent, []).append(path)
        else:
            roots.append(path)

    out = Text()
    stack = [(p, "", True, True) for p in reversed(roots)]
    while stack:
        path, prefix, last, is_root = stack.pop()
        n = by_path[path]
        connector = "" if is_root else ("└── " if last else "├── ")
        out.append("  " + prefix + connector, "muted")
        _node_line(out, n, path if is_root else path.rsplit("/", 1)[-1])
        out.append("\n")
        child_prefix = prefix if is_root else prefix + ("    " if last else "│   ")
        if n.get("data") is not None:
            data = n["data"]
            shown = data if len(data) <= TEXT_DATA_LIMIT else data[:TEXT_DATA_LIMIT] + "… (see JSON)"
            shown = shown.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
            bar = "│   " if kids.get(path) else "    "
            out.append("  " + child_prefix + bar, "muted")
            out.append("data(%s): " % n.get("data_encoding"), "muted")
            out.append(shown)
            out.append("\n")
        children = kids.get(path, [])
        for i, c in enumerate(reversed(children)):
            stack.append((c, child_prefix, i == 0, False))
    console.print(out, end="", soft_wrap=True)


def _node_line(out, n, name):
    st = n.get("stat") or {}
    if n.get("error"):
        out.append(name, "warn")
        out.append("  %s" % n["error"], "warn")
        return
    out.append(name, "ident" if st.get("ephemeral_owner") else "path")
    meta = []
    if st.get("num_children"):
        meta.append("%d child%s" % (st["num_children"], "" if st["num_children"] == 1 else "ren"))
    if st.get("data_length"):
        meta.append("%d B" % st["data_length"])
    if st.get("mtime"):
        meta.append(st["mtime"])
    if st.get("ephemeral_owner"):
        meta.append("ephemeral " + st["ephemeral_owner"])
    if meta:
        out.append("  " + " · ".join(meta), "muted")
    acl = _acl_text(n.get("acl"))
    if acl:
        out.append("  ")
        out.append_text(acl)
    for k in ("acl_error", "data_error", "children_error"):
        if n.get(k):
            out.append("  %s: %s" % (k.replace("_", " "), n[k]), "warn")
    if n.get("children_not_listed"):
        out.append("  (children not listed)", "muted")


# --- CVE list (--cves) ---------------------------------------------------------

_APACHE_SEV_STYLE = {"critical": "bad", "high": "bad", "important": "bad", "moderate": "warn", "low": "muted"}


def render_cves(console, version=None):
    if version:
        v = cvedb.parse_version(version)
        if not v:
            console.print(Text("Not a version: %s" % version, "bad"))
            return 2
        hits = cvedb.affecting(v)
        title = "Known CVEs for ZooKeeper %s (%d)" % (".".join(map(str, v)), len(hits))
        rows = hits
    else:
        title = "Known Apache ZooKeeper CVEs (%d)" % len(cvedb.CVES)
        rows = [(c, False) for c in cvedb.CVES]
    table = _table(title, ("CVE", ""), ("Severity", ""), ("Affected", ""), ("Fixed in", ""),
                   ("Precondition", ""), ("Title", ""), show_lines=False)
    table.columns[0].no_wrap = True
    for c, disputed in rows:
        sev = c["severity"] + (" %.1f" % c["cvss"] if c.get("cvss") else "")
        table.add_row(Text(c["id"], "ident"), Text(sev, _APACHE_SEV_STYLE.get(c["severity"], "")),
                      cvedb.format_ranges(c["affected"]) + (" (disputed for this version)" if disputed else ""),
                      ", ".join(c["fixed"]), Text(c["requires"].replace("_", " "), "muted"), c["title"])
    if rows:
        console.print(table)
    else:
        console.print(Text.assemble((title.rsplit(" (", 1)[0] + ": ", "title"), ("none", "good")))
    if version and cvedb.parse_version(version)[:2] < min(cvedb.MAINTAINED_LINES):
        console.print(Text("End-of-life release line: Apache only lists affected versions for maintained "
                           "lines, so newer CVEs may also apply.", "warn"))
    console.print(Text("Source: %s (as of %s). Upgrade target: %s" % (
        cvedb.CVE_SOURCE, cvedb.CVE_DATA_AS_OF,
        cvedb.upgrade_target(version) if version else ", ".join(sorted(cvedb.LATEST.values()))), "muted"))
    return 0
