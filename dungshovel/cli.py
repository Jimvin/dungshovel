"""Command-line entry point."""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

from dungshovel import __version__, clients, enumerate as zkwalk, findings, fingerprint, report, server
from dungshovel.connection import ConnOpts, make_client
from dungshovel.util import now_iso, parse_target, session_hex

log = logging.getLogger("dungshovel")


def build_parser():
    p = argparse.ArgumentParser(
        prog="dungshovel",
        description="Audit Apache ZooKeeper servers: enumerate znodes, gather data, fingerprint "
                    "the server and the applications using it, and list known clients. "
                    "Only use against systems you are authorised to test.")
    p.add_argument("targets", nargs="*", metavar="HOST[:PORT]", help="servers to audit")
    p.add_argument("-f", "--file", help="file with one HOST[:PORT] per line ('#' comments allowed)")
    p.add_argument("-p", "--port", type=int, default=2181, help="default client port (default: 2181)")
    p.add_argument("-t", "--threads", type=int, default=4, help="targets audited in parallel (default: 4)")
    p.add_argument("-v", "--verbose", action="count", default=0, help="log progress to stderr (-vv: debug)")
    p.add_argument("--version", action="version", version="%(prog)s " + __version__)
    p.add_argument("--cves", nargs="?", const="", metavar="ZK_VERSION",
                   help="print the known ZooKeeper CVEs (only those affecting ZK_VERSION if given) and exit")

    o = p.add_argument_group("output")
    o.add_argument("--format", choices=("text", "json"), default="text", help="stdout format (default: text)")
    o.add_argument("-o", "--output", help="also write the report to this file")
    o.add_argument("--output-format", choices=("text", "json"),
                   help="format for --output (default: json if the file ends in .json, else --format)")
    o.add_argument("-q", "--quiet", action="store_true", help="don't print the report to stdout")
    o.add_argument("--color", choices=("auto", "always", "never"), default="auto",
                   help="colour in the stdout text report (default: auto; NO_COLOR is honoured)")
    o.add_argument("--fail-on", choices=("high", "medium", "low", "never"), default="never",
                   help="exit with status 3 if any finding is at least this severe (default: never)")
    o.add_argument("--no-tree", action="store_true",
                   help="walk the tree but leave the per-znode listing out of the report")

    e = p.add_argument_group("enumeration")
    e.add_argument("-b", "--basepath", default="/", help="znode to start from (default: /)")
    e.add_argument("-n", "--non-recursive", action="store_true", help="only list the base path's children")
    e.add_argument("--max-depth", type=int, help="maximum depth below the base path")
    e.add_argument("--max-nodes", type=int, default=100000, help="stop after this many znodes (default: 100000)")
    e.add_argument("-e", "--exclude", default="", help="comma-separated paths whose children are not listed")
    e.add_argument("--no-acls", action="store_true", help="don't fetch ACLs (halves the request count)")
    e.add_argument("--batch-size", type=int, default=256, help="requests in flight per batch (default: 256)")

    d = p.add_argument_group("data")
    d.add_argument("-d", "--data", action="store_true", help="gather the data of every znode")
    d.add_argument("-D", "--base64", action="store_true", help="gather data, always base64-encoded")
    d.add_argument("--max-data-bytes", type=int, default=1024 * 1024,
                   help="truncate znode data above this size (default: 1 MiB)")

    f = p.add_argument_group("fingerprinting")
    f.add_argument("--no-fingerprint", action="store_true", help="skip application fingerprinting")
    f.add_argument("--no-fingerprint-reads", action="store_true",
                   help="fingerprint from the znode layout only; don't read znode data for details")
    f.add_argument("--max-fingerprint-reads", type=int, default=2000,
                   help="cap on znode reads made by fingerprint extractors (default: 2000)")
    f.add_argument("--no-4lw", action="store_true", help="don't send four-letter-word commands")
    f.add_argument("--admin-port", type=int, nargs="?", const=8080,
                   help="also query the AdminServer on this port (default when given without value: 8080)")
    f.add_argument("--admin-https", action="store_true", help="use HTTPS for the AdminServer")

    c = p.add_argument_group("clients")
    c.add_argument("--no-clients", action="store_true", help="skip client listing")
    c.add_argument("--watches", action="store_true",
                   help="also run 'wchc' (lists watches per session; can be expensive on busy servers)")

    n = p.add_argument_group("connection")
    n.add_argument("--timeout", type=float, default=10.0, help="connect/command timeout in seconds (default: 10)")
    n.add_argument("--tls", action="store_true", help="use TLS (client port must be a secureClientPort)")
    n.add_argument("--ca", help="CA bundle for TLS")
    n.add_argument("--cert", help="client certificate for TLS")
    n.add_argument("--key", help="client key for TLS")
    n.add_argument("--insecure", action="store_true", help="don't verify TLS certificates")
    n.add_argument("--auth", action="append", default=[], metavar="SCHEME:CREDENTIAL",
                   help="add auth info, e.g. digest:user:password (repeatable)")
    return p


def load_targets(args, parser):
    raw = list(args.targets)
    if args.file:
        with open(args.file) as fh:
            raw += [line.split("#", 1)[0].strip() for line in fh]
    raw = [r for r in raw if r]
    if not raw:
        parser.error("no targets given (positional HOST[:PORT] or -f FILE)")
    targets = []
    for r in raw:
        try:
            targets.append(parse_target(r, args.port))
        except ValueError as e:
            parser.error(str(e))
    return targets


def acl_summary(tree):
    s = {"world_read": 0, "world_write": 0, "world_admin": 0, "acl_unreadable": 0}
    for rec in tree.nodes.values():
        if "acl" not in rec:
            if rec.get("acl_error"):
                s["acl_unreadable"] += 1
            continue
        perms = "".join(a["perms"] for a in rec["acl"] if a["scheme"] == "world" and a["id"] == "anyone")
        s["world_read"] += "r" in perms
        s["world_write"] += any(c in perms for c in "wcd")
        s["world_admin"] += "a" in perms
    return s


def read_ensemble_config(zk, server_info):
    """/zookeeper/config exists on 3.5+ and is readable even when 4lw are locked down."""
    try:
        raw = zk.get("/zookeeper/config")[0]
    except Exception:
        raw = None
    members = {}
    for line in (raw or b"").decode("utf-8", "replace").splitlines():
        k, sep, v = line.partition("=")
        if sep and k.startswith("server."):
            members[k] = v
    if members:
        return {"source": "/zookeeper/config", "members": members}
    if server_info.get("ensemble"):
        return {"source": "conf", "members": server_info["ensemble"]}
    return None


def audit(host, port, args, conn_opts, walk_opts):
    target = "%s:%d" % (host, port)
    result = {"target": target, "host": host, "port": port, "started": now_iso()}
    log.info("%s: connecting", target)

    zk = make_client(host, port, conn_opts)
    own_session = None
    try:
        zk.start(timeout=conn_opts.timeout)
        own_session = session_hex(zk.client_id[0]) if zk.client_id else None
        result["connection"] = {"ok": True, "session_id": own_session, "tls": conn_opts.tls,
                                "auth_schemes": [s for s, _ in conn_opts.auth]}
    except Exception as e:
        result["connection"] = {"ok": False, "error": "%s: %s" % (type(e).__name__, e) if str(e) else type(e).__name__}
        log.warning("%s: connection failed: %s", target, result["connection"]["error"])
        zk = None

    probe = None
    if not args.no_fingerprint or not args.no_clients:
        if not args.no_4lw or args.admin_port:
            log.info("%s: probing server", target)
            probe = server.probe(host, port, conn_opts, use_4lw=not args.no_4lw,
                                 admin_port=args.admin_port, admin_https=args.admin_https,
                                 clients=not args.no_clients, watches=args.watches)
            result["server"] = {"info": probe["info"], "exposure": probe["exposure"], "raw": probe["raw"]}

    tree = None
    if zk:
        try:
            log.info("%s: enumerating from %s", target, walk_opts.basepath)
            tree = zkwalk.walk(zk, walk_opts)
            log.info("%s: %d znodes", target, len(tree.nodes))
            result["znodes"] = {
                "basepath": walk_opts.basepath,
                "count": len(tree.nodes),
                "truncated": tree.truncated,
                "aborted": tree.aborted,
                "denied": tree.denied,
                "depth_limited": any(n.get("children_not_listed") for n in tree.nodes.values()),
                "acl_summary": acl_summary(tree) if walk_opts.acls else None,
                "nodes": None if args.no_tree else [tree.nodes[p] for p in sorted(tree.nodes)],
            }
            if not args.no_fingerprint_reads:
                ens = read_ensemble_config(zk, (probe or {}).get("info") or {})
                if ens:
                    result["ensemble"] = ens

            app_clients = []
            if not args.no_fingerprint:
                log.info("%s: fingerprinting applications", target)
                apps, app_clients, reads = fingerprint.fingerprint(
                    zk, tree, allow_reads=not args.no_fingerprint_reads,
                    read_budget=args.max_fingerprint_reads)
                result["fingerprint"] = {"applications": apps, "reads": reads,
                                         "unclassified": fingerprint.unclassified_roots(tree, apps, walk_opts.basepath)}
            if not args.no_clients:
                own_ports = [r["local_port"] for r in ((probe or {}).get("raw") or {}).get("4lw", {}).values()
                             if r.get("local_port")]
                result["clients"] = clients.build(probe, tree, app_clients, own_session, own_ports)
        finally:
            zk.stop()
            zk.close()
    elif probe and not args.no_clients:
        result["clients"] = clients.build(probe, None, [], None, [])

    result["findings"] = findings.evaluate(result, tree)
    result["finished"] = now_iso()
    return result


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.WARNING - 10 * min(args.verbose, 2),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    logging.getLogger("kazoo").setLevel(logging.DEBUG if args.verbose >= 3 else logging.CRITICAL)

    if args.cves is not None:
        return report.print_with(lambda c: report.render_cves(c, args.cves or None), args.color)

    targets = load_targets(args, parser)
    auth = []
    for a in args.auth:
        scheme, sep, cred = a.partition(":")
        if not sep:
            parser.error("--auth must be SCHEME:CREDENTIAL")
        auth.append((scheme, cred))
    conn_opts = ConnOpts(timeout=args.timeout, tls=args.tls, ca=args.ca, cert=args.cert, key=args.key,
                         verify=not args.insecure, auth=auth)
    exclude = [x.strip() for x in args.exclude.split(",") if x.strip()]
    walk_opts = zkwalk.WalkOpts(
        basepath=args.basepath, max_depth=1 if args.non_recursive else args.max_depth,
        max_nodes=args.max_nodes, exclude=exclude, acls=not args.no_acls,
        data=args.data or args.base64, force_base64=args.base64,
        max_data_bytes=args.max_data_bytes, batch_size=max(1, args.batch_size))

    with ThreadPoolExecutor(max_workers=max(1, args.threads)) as pool:
        futures = [pool.submit(audit, h, p, args, conn_opts, walk_opts) for h, p in targets]
        results = []
        for (h, p), fut in zip(targets, futures):
            try:
                results.append(fut.result())
            except Exception as e:
                log.exception("audit of %s:%d failed", h, p)
                results.append({"target": "%s:%d" % (h, p), "host": h, "port": p,
                                "connection": {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}})

    options = {k: v for k, v in vars(args).items() if k not in ("auth", "targets", "file")}
    options["auth_schemes"] = [s for s, _ in auth]  # never put credentials in the report
    rep = {"tool": "dungshovel", "version": __version__, "generated": now_iso(),
           "options": options, "targets": results}

    render = {"json": report.to_json, "text": report.to_text}
    if not args.quiet:
        if args.format == "text":
            report.print_report(rep, color=args.color)
        else:
            sys.stdout.write(report.to_json(rep))
    if args.output:
        fmt = args.output_format or ("json" if args.output.lower().endswith(".json") else args.format)
        with open(args.output, "w") as fh:
            fh.write(render[fmt](rep))
        log.info("report written to %s", args.output)

    reachable = any((r.get("connection") or {}).get("ok") or r.get("server") for r in results)
    if not reachable:
        return 1
    if args.fail_on != "never":
        limit = findings.SEVERITY_RANK[args.fail_on]
        if any(findings.SEVERITY_RANK[f["severity"]] <= limit for r in results for f in r.get("findings") or []):
            return 3
    return 0
