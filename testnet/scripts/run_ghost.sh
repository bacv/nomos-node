#!/bin/sh

set -e

exec /usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ hello" --network-config /etc/nomos/ghost-cli.yml --node-addr http://bootstrap
exec /usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ nodes will be restarted in 10 minutes" --network-config /etc/nomos/ghost-cli.yml --node-addr http://bootstrap
exec /usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ bye" --network-config /etc/nomos/ghost-cli.yml --node-addr http://bootstrap

