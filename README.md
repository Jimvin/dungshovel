# dungshovel

Audit Apache ZooKeeper servers. For each target, dungshovel:

- **enumerates the znodes**: stat, ACL, and optionally data for every znode
- **fingerprints the server**: version, mode, host, JVM, OS, data dirs and ensemble, plus which
  four-letter-word (4lw) commands and AdminServer endpoints are exposed
- **fingerprints applications**: recognises Apache Kafka, Apache NiFi, Apache Dubbo, Apache HBase,
  Apache Solr and others from their znode layout, and extracts cluster ids, endpoints and principals
- **reports findings**: ranked issues (high/medium/low/info), each with evidence and a fix
- **lists clients**: live connections, sessions, and the hosts/principals that applications
  registered in ZooKeeper (see [What "clients" means](#what-clients-means))

The report goes to stdout as coloured tables (or JSON with `--format json`). Use `-o` to also write
it to a file; text files are written without colour codes.

Colour follows audit severity: **red** = exposed (world-writable ACLs, open `conf`/`envi`/`cons`/`dump`,
world-readable credentials), **yellow** = worth a look (world-readable, plaintext, unauthenticated),
**green** = restricted or disabled. Piped output is plain text automatically.

> Only run this against systems you are authorised to audit. Reports can contain secrets: znode
> data, digest ACL hashes (crackable offline), SCRAM user names and server paths. Handle them
> accordingly.

## Install

```sh
python3 -m venv .venv && .venv/bin/pip install .
```

Requires Python ≥ 3.9, [kazoo](https://github.com/python-zk/kazoo) (Apache-2.0) and
[rich](https://github.com/Textualize/rich) (MIT).

## Usage

```sh
# audit one server, text report on stdout
dungshovel zk1.example.com

# several servers from a file, 8 in parallel, also query the AdminServer on :8080,
# text on screen and JSON to a file
dungshovel -f targets.txt -t 8 --admin-port -o report.json

# gather znode data too (-D forces base64), skip noisy subtrees
dungshovel zk1:2181 -d -e /brokers/topics,/config/topics

# authenticated, TLS
dungshovel zk1:2182 --tls --ca ca.pem --cert client.pem --key client.key --auth digest:user:pass

# only a summary: no per-znode listing in the report
dungshovel zk1 --no-tree
```

| Option | Purpose |
|---|---|
| `-f FILE` | targets, one `HOST[:PORT]` per line, `#` comments allowed |
| `-p PORT` | default port (2181) |
| `-t N` | targets audited in parallel |
| `--format text\|json` | stdout format |
| `-o FILE`, `--output-format` | also write the report to a file (JSON if it ends in `.json`) |
| `-q` | no stdout report |
| `--color auto\|always\|never` | colour in the stdout text report (`auto`: only on a terminal; `NO_COLOR` honoured) |
| `--fail-on high\|medium\|low\|never` | exit 3 if any finding is at least this severe (for CI) |
| `--cves [VERSION]` | print the known-CVE list (optionally for one version) and exit |
| `--no-tree` | walk the tree but omit the per-znode listing |
| `-b PATH`, `-n`, `--max-depth`, `--max-nodes`, `-e PATHS` | limit the walk |
| `--no-acls` | skip `getACL` (halves the request count) |
| `-d`, `-D`, `--max-data-bytes` | gather znode data (`-D`: always base64) |
| `--no-fingerprint`, `--no-fingerprint-reads`, `--max-fingerprint-reads` | control app fingerprinting |
| `--no-4lw`, `--admin-port [PORT]`, `--admin-https` | control server probing |
| `--no-clients`, `--watches` | control client listing (`--watches` runs `wchc`) |
| `--tls`, `--ca`, `--cert`, `--key`, `--insecure`, `--auth SCHEME:CRED` | connection |

Exit codes: 0 = done, 1 = no target answered, 2 = usage error, 3 = `--fail-on` threshold reached.

## What it sends

- One ZooKeeper session per target, with requests pipelined in batches (`--batch-size`):
  `getACL` + `getData` (with `-d`) or `exists`, then `getChildren`. The session is read-only;
  nothing is written.
- 4lw commands, one TCP connection each: `ruok srvr conf envi mntr isro cons stat dump` (+ `wchc`
  with `--watches`). Since ZooKeeper 3.5.3 only `srvr` is whitelisted by default; the rest show
  as `disabled`.
- With `--admin-port`: the equivalent `GET /commands/<name>` AdminServer calls. The AdminServer is
  enabled by default on port 8080 and often exposes what 4lw hides.
- Fingerprint extractors read the data of specific znodes (broker registrations, leader-election
  nodes and so on), capped by `--max-fingerprint-reads`. Use `--no-fingerprint-reads` to identify
  apps from the layout only.

## Application fingerprints

Signatures match on znode layout **anywhere** in the tree, so chrooted applications (`/kafka`,
`/znode-<uuid>` as created by Stackable operators, etc.) are found. See `SIGNATURES` in
[dungshovel/fingerprint.py](dungshovel/fingerprint.py).

| Application | Detected by | Extracted |
|---|---|---|
| Apache Kafka (ZooKeeper mode) | `brokers/ids`, `brokers/topics`, + controller/cluster nodes | cluster id, controller, broker endpoints, topic count, SCRAM user names, `kafka-acl` principals/hosts, old ZK consumer groups |
| Apache NiFi | `leaders/Cluster Coordinator`, `leaders/Primary Node` | election participants (node addresses), current leader, component-state count |
| Apache Dubbo | `<svc>/providers\|consumers/<url-encoded URL>`, `configurators`, `routers` | provider/consumer IPs and ports, application names, registration timestamps |
| Apache HBase | `hbaseid` + `master`/`rs`/... | cluster id, active master, meta server, region servers (with start time) |
| Apache Hadoop HDFS / YARN HA | `*/ActiveBreadCrumb`, `*/ActiveStandbyElectorLock` | active and *last active* NameNode host:port (protobuf), RM ids |
| Apache Hadoop YARN RM store | `ZKRMStateRoot` | application count |
| Apache Solr | `live_nodes` + 2 of `collections`, `configs`, `security.json`, … | live nodes, collections, `security.json` presence and world-readability |
| Apache Hive | `serverUri=*` children, `hive_zookeeper_namespace*` | HiveServer2 instances and versions |
| Apache Druid, Storm, Pulsar, BookKeeper, Flink, Accumulo, Helix/Pinot, DolphinScheduler, Spark, Mesos | characteristic node sets | node endpoints where encoded in names/data |
| ClickHouse, Patroni | characteristic node sets | tables / cluster members |
| Apache Curator service discovery (Dubbo 3, Spring Cloud Zookeeper) | `/services/<svc>/<id>` | instance address/port, registration time |

Top-level znodes no signature accounts for are listed as *unclassified*. Only the Apache Kafka
layout has been checked against a real deployment. The other signatures are based on the
projects' documented and source-code layouts and on synthetic test trees
([tests/seed_zk.py](tests/seed_zk.py)), so expect gaps. Adding a signature is a few lines.

## Findings

Findings are derived from what the audit already collected; no extra requests are made. Each
target's findings appear first in the text report and as `targets[].findings` in JSON.

| ID | Severity | Raised when |
|---|---|---|
| `ZK-ACL-WORLD-WRITE` | high (medium if neither `/` nor an app root is affected) | `world:anyone` has w/c/d anywhere. Evidence names each detected app at risk and what an attacker could do |
| `ZK-ACL-WORLD-ADMIN` | high | `world:anyone` has ADMIN (can rewrite ACLs) |
| `ZK-DIGEST-HASHES` | high unauthenticated, else medium | digest ACL hashes (unsalted SHA-1) were readable |
| `KAFKA-SCRAM-READABLE` | high | Kafka `config/users` SCRAM verifiers are world-readable |
| `SOLR-SECURITY-JSON-READABLE` | high | Solr `security.json` is world-readable |
| `ZK-SECRETS-IN-DATA` | high if world-readable, else medium | znode data matches password/key/credential-URL patterns (needs `-d`; values are not repeated) |
| `ZK-4LW-EXPOSED`, `ZK-ADMIN-EXPOSED` | medium | `conf`, `envi`, `cons`, `stat`, `dump`, `wchc` or `mntr` answer without auth |
| `ZK-PLAINTEXT` | medium | the client port accepted a non-TLS session |
| `ZK-VERSION-EOL` | medium | release lines older than 3.8, which no longer get fixes |
| `CVE-…` | by applicability (see below) | the server version is in the affected range of a known advisory |
| `ZK-KNOWN-CVES` | info | summary: CVEs listed for this version, and those ruled out |
| `ZK-ACL-WORLD-READ` | low | `world:anyone` can read |
| `ZK-UNAUTH-CONNECT` | low | a session without credentials was allowed (and nothing worse was found) |
| `AUDIT-COVERAGE` | info | the walk was truncated, aborted, depth-limited or hit unreadable znodes |

A stock ZooKeeper install typically reports `ZK-ACL-WORLD-WRITE`/`-ADMIN` (`/` is
`world:anyone:cdrwa` by default), `ZK-ADMIN-EXPOSED` and `ZK-PLAINTEXT`. Findings reflect what the
credentials used could see: run once without `--auth` to see the anonymous view.

## Known CVEs

[dungshovel/cves.py](dungshovel/cves.py) lists every advisory on the
[Apache ZooKeeper security page](https://zookeeper.apache.org/security.html) (reviewed 2026-11-16)
with its affected version ranges, fixed versions, Apache's severity and its precondition.
Print it with:

```sh
dungshovel --cves            # all known CVEs
dungshovel --cves 3.8.4      # only those affecting 3.8.4
```

During an audit, each CVE affecting the server's version becomes a finding. Its severity depends on
whether the precondition was observed on that server:

| Applicability | Meaning | critical / high | important | moderate |
|---|---|---|---|---|
| exploitable | precondition observed (e.g. unauthenticated session accepted, `/zookeeper/quota` world-writable, digest ACLs present, AdminServer reachable with an open root ACL) | high | high | medium |
| applies | no special precondition | high | medium | low |
| conditional | depends on configuration the audit cannot see (quorum SASL, audit log, FIPS, client SASL, TLS hostname verification) | medium | medium | low |
| client-side | affects the Java client library or the C CLI shells, not the server | info | info | info |
| not applicable | precondition ruled out (e.g. quorum CVEs on a standalone server) | listed in `ZK-KNOWN-CVES` only | | |

Notes:

- Apache only lists affected versions for maintained lines (3.8, 3.9). For end-of-life versions,
  `ZK-VERSION-EOL` names the newer CVEs that were not assessed for that line.
- Where an advisory's "Affected versions" header and its description disagree, the range follows
  the description and the entry has a `note`. An entry can also list `disputed` ranges for releases
  where the advisory is contradictory; those are reported as `info`.
- Update the list by editing `CVES`, `LATEST` and `CVE_DATA_AS_OF` in `cves.py`.
  `tests/test_unit.py::test_cve_ranges` checks that no fixed version falls inside its own
  affected range.

## What "clients" means

ZooKeeper keeps **no log of past connections**. dungshovel collects the evidence that does exist
and labels each identity with where it came from:

| Evidence | Source | Meaning |
|---|---|---|
| `connected` | `cons` / `stat` 4lw, or AdminServer `connections` | open connection to *this* server now (address, session, established time) |
| `ephemeral` | `ephemeralOwner` in znode stats, `dump` | session is alive now, possibly connected to another ensemble member (the top byte of the session id is the creating server's `myid`) |
| `persistent` | app registrations in persistent znodes | **previously connected** clients: e.g. Dubbo consumers with `dynamic=false`, HDFS `ActiveBreadCrumb`, BookKeeper cookies, Kafka ACL hosts, Helix `INSTANCES` |
| `acl` | `ip:`, `digest:`, `sasl:`, `x509:` ids in ACLs | identities that were granted access |

Sessions are joined across sources, so an application registration (e.g. a Kafka broker's
advertised name) is paired with the network address it actually connects from when the session
is visible in `cons`. Some limits:

- `cons`/`dump` are usually disabled over 4lw; `dump` only lists sessions on the leader.
- `lresp` (last response) is a monotonic server clock on ZooKeeper ≥ 3.5, not wall time, so it
  is reported as `monotonic:<ms>`.
- The no-session connection in `cons` output is usually the 4lw probe itself.
- Timestamps are UTC (ISO 8601).

## Tests

```sh
.venv/bin/pip install -e '.[test]'
.venv/bin/pytest

# against a throwaway ZooKeeper (never a real cluster):
docker run -d --name zk -p 2181:2181 -p 8080:8080 -e ZOO_4LW_COMMANDS_WHITELIST='*' zookeeper:3.9
python tests/seed_zk.py 127.0.0.1:2181 --hold 600 &
dungshovel 127.0.0.1 --admin-port
```
