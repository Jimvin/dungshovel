"""Breadth-first znode enumeration with pipelined (async) requests."""

import logging
from collections import deque
from dataclasses import dataclass, field

from kazoo.exceptions import (ConnectionLoss, KazooException, NoAuthError,
                              NoNodeError, SessionExpiredError)
from kazoo.security import Permissions

from dungshovel.util import encode_data, join_path, ms_to_iso, session_hex

log = logging.getLogger(__name__)

_PERM_LETTERS = [(Permissions.CREATE, "c"), (Permissions.DELETE, "d"), (Permissions.READ, "r"),
                 (Permissions.WRITE, "w"), (Permissions.ADMIN, "a")]


@dataclass
class WalkOpts:
    basepath: str = "/"
    max_depth: int = None
    max_nodes: int = 100000
    exclude: list = field(default_factory=list)
    acls: bool = True
    data: bool = False
    force_base64: bool = False
    max_data_bytes: int = 1024 * 1024
    batch_size: int = 256


@dataclass
class Tree:
    nodes: dict = field(default_factory=dict)      # path -> record
    children: dict = field(default_factory=dict)   # path -> [child names]
    raw_data: dict = field(default_factory=dict)   # path -> bytes (only with --data)
    truncated: bool = False
    aborted: str = None
    denied: list = field(default_factory=list)     # paths whose children could not be listed

    def stat(self, path):
        rec = self.nodes.get(path)
        return rec.get("stat") if rec else None

    def ephemeral_owner(self, path):
        st = self.stat(path)
        return st.get("ephemeral_owner") if st else None


def perms_to_str(perms):
    return "".join(letter for bit, letter in _PERM_LETTERS if perms & bit)


def format_acl(acl):
    return {"scheme": acl.id.scheme, "id": acl.id.id, "perms": perms_to_str(acl.perms)}


def format_stat(stat):
    owner = stat.ephemeralOwner
    return {
        "czxid": "0x%x" % stat.czxid,
        "mzxid": "0x%x" % stat.mzxid,
        "pzxid": "0x%x" % stat.pzxid,
        "ctime": ms_to_iso(stat.ctime),
        "mtime": ms_to_iso(stat.mtime),
        "ctime_ms": stat.ctime,
        "mtime_ms": stat.mtime,
        "version": stat.version,
        "cversion": stat.cversion,
        "aversion": stat.aversion,
        "ephemeral_owner": session_hex(owner) if owner else None,
        "data_length": stat.dataLength,
        "num_children": stat.numChildren,
    }


def _excluded(path, exclude):
    return any(path == ex or path.startswith(ex.rstrip("/") + "/") for ex in exclude)


def _gather(zk, method, paths):
    """Issue one async call per path, then collect results."""
    pending = [(p, getattr(zk, method)(p)) for p in paths]
    results = {}
    for path, async_result in pending:
        try:
            results[path] = ("ok", async_result.get())
        except NoAuthError:
            results[path] = ("noauth", None)
        except NoNodeError:
            results[path] = ("nonode", None)
        except (ConnectionLoss, SessionExpiredError):
            raise
        except KazooException as e:
            results[path] = ("error", "%s: %s" % (type(e).__name__, e))
    return results


def walk(zk, opts):
    tree = Tree()
    queue = deque([(opts.basepath, 0)])
    try:
        while queue:
            room = opts.max_nodes - len(tree.nodes)
            if room <= 0:
                tree.truncated = True
                break
            batch = [queue.popleft() for _ in range(min(opts.batch_size, room, len(queue)))]
            _walk_batch(zk, opts, tree, batch, queue)
    except (ConnectionLoss, SessionExpiredError) as e:
        tree.aborted = "%s during enumeration; results are partial" % type(e).__name__
        log.warning("%s", tree.aborted)
    return tree


def _walk_batch(zk, opts, tree, batch, queue):
    paths = [p for p, _ in batch]
    depth = dict(batch)
    stats, records = {}, {p: {"path": p} for p in paths}

    if opts.acls:
        for p, (status, val) in _gather(zk, "get_acls_async", paths).items():
            if status == "ok":
                acls, stat = val
                records[p]["acl"] = [format_acl(a) for a in acls]
                stats[p] = stat
            elif status != "nonode":
                records[p]["acl_error"] = status if status != "error" else val
    if opts.data:
        for p, (status, val) in _gather(zk, "get_async", paths).items():
            if status == "ok":
                raw, stat = val
                stats[p] = stat
                if raw:
                    tree.raw_data[p] = raw
                    value, enc, trunc = encode_data(raw, opts.force_base64, opts.max_data_bytes)
                    records[p].update(data=value, data_encoding=enc)
                    if trunc:
                        records[p]["data_truncated"] = True
            elif status != "nonode":
                records[p]["data_error"] = status if status != "error" else val
    missing = [p for p in paths if p not in stats]
    denied = set()
    if missing:
        for p, (status, val) in _gather(zk, "exists_async", missing).items():
            if status == "ok" and val is not None:
                stats[p] = val
            elif status == "noauth":
                denied.add(p)

    want_children = []
    for p in paths:
        if p in denied:
            # Newer servers check READ even for exists(): only the name is visible.
            records[p].update(stat=None, error="NoAuth")
            tree.nodes[p] = records[p]
            tree.denied.append(p)
            continue
        if p not in stats:
            continue  # deleted while walking
        rec = records[p]
        rec["stat"] = format_stat(stats[p])
        tree.nodes[p] = rec
        if stats[p].numChildren == 0:
            continue
        if _excluded(p, opts.exclude) or (opts.max_depth is not None and depth[p] >= opts.max_depth):
            rec["children_not_listed"] = True
            continue
        want_children.append(p)

    for p, (status, val) in _gather(zk, "get_children_async", want_children).items():
        if status == "ok":
            kids = sorted(val)
            tree.children[p] = kids
            for k in kids:
                queue.append((join_path(p, k), depth[p] + 1))
        elif status != "nonode":
            tree.nodes[p]["children_error"] = status if status != "error" else val
            tree.denied.append(p)
