#!/bin/sh

sleep 20
/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ hello" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

sleep 1
/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ nodes will be restarted in 10 minutes" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

sleep 1
/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date) ~ bye" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

