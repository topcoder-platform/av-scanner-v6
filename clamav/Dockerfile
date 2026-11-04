FROM clamav/clamav:stable@sha256:6d87c712e3391e13c1ba6e05dfb074d4b1ddb5a6834b23bb9da990d2a4ef042a

RUN apk upgrade --no-cache

COPY clamd.conf /etc/clamav/clamd.conf

ARG CLAMAV_TCP_ADDR=127.0.0.1
RUN case "$CLAMAV_TCP_ADDR" in \
      0.0.0.0|127.0.0.1) ;; \
      *) echo "Unsupported CLAMAV_TCP_ADDR" >&2; exit 1 ;; \
    esac \
    && sed -i "s/^TCPAddr .*/TCPAddr ${CLAMAV_TCP_ADDR}/" /etc/clamav/clamd.conf \
    && grep -qx "TCPAddr ${CLAMAV_TCP_ADDR}" /etc/clamav/clamd.conf \
    && grep -qx "LocalSocket /run/clamav/clamd.sock" /etc/clamav/clamd.conf

# clamd is intentionally unauthenticated, so the safe default is loopback for
# ECS awsvpc tasks. Compose explicitly opts into its private bridge address.

# The upstream /init performs root-only ownership/configuration changes. Prepare
# fixed paths at build time and start both daemons as clamav instead.
RUN mkdir -p /run/clamav /var/log/clamav \
    && chown -R clamav:clamav /run/clamav /var/log/clamav /var/lib/clamav
COPY start.sh /usr/local/bin/start-clamav
RUN chmod 0555 /usr/local/bin/start-clamav
USER clamav
ENTRYPOINT ["/sbin/tini", "-g", "--", "/usr/local/bin/start-clamav"]
CMD []
