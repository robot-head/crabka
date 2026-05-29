#!/usr/bin/env bash
# Entry point for the KDC container. Bootstraps the CRABKA.TEST realm, creates
# the broker service principal (kafka/localhost) and a client principal (alice),
# exports their keytabs into the shared /fixtures volume, then runs krb5kdc in
# the foreground so the container stays up and the port mapping stays live.
set -euo pipefail

REALM="CRABKA.TEST"
MASTER_PW="masterkey"
FIXTURES="/fixtures"

export KRB5_CONFIG=/etc/krb5.conf
export KRB5_KDC_PROFILE=/etc/krb5kdc/kdc.conf

cp /seed/krb5.conf /etc/krb5.conf
mkdir -p /etc/krb5kdc
cp /seed/kdc.conf /etc/krb5kdc/kdc.conf

# Only one principal (kadmin/admin) is granted admin rights; harmless for tests.
echo "*/admin@${REALM} *" > /var/lib/krb5kdc/kadm5.acl

# Create the KDC database (stash file holds the master key so krb5kdc starts
# without an interactive password prompt).
kdb5_util create -r "${REALM}" -s -P "${MASTER_PW}"

# Service + client principals. -randkey for the service (key only ever lives in
# the keytab); a fixed password for alice so the spike can also exercise the
# password-based client path if needed.
#
# Two service SPNs share one keytab:
#   kafka/localhost          — the SPN Rust in-process clients target (they dial
#                              "localhost"), used by the inter-broker GSSAPI test.
#   kafka/host.docker.internal — the SPN a containerized cp-kafka client derives,
#                              since the broker advertises host.docker.internal:9092
#                              and the stock client builds serviceName/<advertised-host>.
kadmin.local -q "addprinc -randkey kafka/localhost@${REALM}"
kadmin.local -q "addprinc -randkey kafka/host.docker.internal@${REALM}"
kadmin.local -q "addprinc -pw alicepw alice@${REALM}"

# sspi-rs's client AS-exchange REQUIRES the KDC to demand pre-authentication:
# it first sends an AS-REQ with no PA data expecting a KDC_ERR_PREAUTH_REQUIRED
# (to learn the salt), then resends with the encrypted timestamp. If the KDC
# issues an AS-REP on the first try (preauth not required), sspi errors with
# "KDC server should not process AS_REQ without the pa-pac data". So force
# pre-auth on the client principal.
kadmin.local -q "modprinc +requires_preauth alice@${REALM}"

# Export keytabs into the shared volume the host reads from. Both service SPNs
# land in the single kafka.keytab (ktadd appends), so one broker keytab serves
# both the "localhost" (Rust) and "host.docker.internal" (containerized cp-kafka)
# clients.
rm -f "${FIXTURES}/kafka.keytab" "${FIXTURES}/alice.keytab"
kadmin.local -q "ktadd -k ${FIXTURES}/kafka.keytab -norandkey kafka/localhost@${REALM}"
kadmin.local -q "ktadd -k ${FIXTURES}/kafka.keytab -norandkey kafka/host.docker.internal@${REALM}"
kadmin.local -q "ktadd -k ${FIXTURES}/alice.keytab  -norandkey alice@${REALM}"
chmod 0644 "${FIXTURES}/kafka.keytab" "${FIXTURES}/alice.keytab"

# Start the KDC in the background so the smoke-test kinit below can reach it.
krb5kdc -n &
KDC_PID=$!
sleep 2

# Smoke test: prove the exported service keytab authenticates against the KDC.
kinit -kt "${FIXTURES}/kafka.keytab" "kafka/localhost@${REALM}"
klist
kdestroy

echo "KDC_READY"

# Hand control to the foregrounded KDC so the container (and its 88/tcp+udp
# mapping) stays alive.
wait "${KDC_PID}"
