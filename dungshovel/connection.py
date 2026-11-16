"""Connection settings shared by the ZooKeeper client, 4lw probes and AdminServer probes."""

import ssl
from dataclasses import dataclass, field

from dungshovel.util import format_host


@dataclass
class ConnOpts:
    timeout: float = 10.0
    tls: bool = False
    ca: str = None
    cert: str = None
    key: str = None
    verify: bool = True
    auth: list = field(default_factory=list)  # [(scheme, credential)]

    def ssl_context(self):
        ctx = ssl.create_default_context(cafile=self.ca)
        if not self.verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if self.cert:
            ctx.load_cert_chain(self.cert, self.key)
        return ctx


def make_client(host, port, opts):
    from kazoo.client import KazooClient
    from kazoo.retry import KazooRetry

    kwargs = dict(
        hosts="%s:%d" % (format_host(host), port),
        timeout=opts.timeout,
        read_only=True,  # also allow connecting to servers in read-only mode
        connection_retry=KazooRetry(max_tries=1, delay=0.5, ignore_expire=False),
        command_retry=KazooRetry(max_tries=2, delay=0.2),
        auth_data=list(opts.auth) or None,
    )
    if opts.tls:
        kwargs.update(use_ssl=True, ca=opts.ca, certfile=opts.cert,
                      keyfile=opts.key, verify_certs=opts.verify)
    return KazooClient(**kwargs)
