FROM clamav/clamav:1.5.3@sha256:7f5389ccaa2368c383fa80e167ccfe44348d71e685f926fce4755eed1757673a

COPY clamd.conf /etc/clamav/clamd.conf

# clamd is intentionally unauthenticated. Restrict access at the container and
# task network boundary as described in the deployment documentation.
