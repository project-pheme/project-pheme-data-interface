#!/bin/bash

if [[ -n "$PLATFORM_HOST" && -n "$PLATFORM_PORT" ]]; then
	echo "Waiting for platform to come up"
	PLATFORM_PROTO=${PLATFORM_PROTO:-http}
	url="${PLATFORM_PROTO}://${PLATFORM_HOST}:${PLATFORM_PORT}/api/v3/config"
	echo "Checking against $url"
	while true; do
		curl -f -m 5 $url && break
		sleep 1
	done
fi

if [[ -n "$CAPTURE_HOST" && -n "$CAPTURE_PROTO" ]]; then
	echo "Waiting for capture link to come up"
	while true; do
		nc -z $CAPTURE_HOST $CAPTURE_PROTO && break
		sleep 1
	done
fi


cd /var/app/src
exec python ./app.py --logging=info
