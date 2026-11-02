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
