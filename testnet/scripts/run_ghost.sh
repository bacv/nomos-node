#!/bin/bash

start_time=$(date +%s)

sleep 25

while true; do
	/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date +%H:%M:%S) ~ hello" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

	sleep 1
	/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date +%H:%M:%S) ~ pinging" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

	sleep 1
	/usr/bin/nomos-cli disseminate --user gh057-1n-7h3-5h311 --data "$(date +%H:%M:%S) ~ bye" --network-config /etc/nomos/ghost-config.yml --node-addr http://bootstrap:8080

	sleep 2
done
