#!/bin/bash


# List all Decision Services deployed to DI CMS Runtime

# Note: this script is designed for an instance of DI running locally from the development environment
# The script needs to be adapted for use in a customer environment (Authentification through an API key)

set -e

# get access token for rtsadmin
# Credentials must be supplied via RTSADMIN_USER / RTSADMIN_PASSWORD environment variables.
if [ -z "${RTSADMIN_USER}" ] || [ -z "${RTSADMIN_PASSWORD}" ]; then
    echo "ERROR: RTSADMIN_USER and RTSADMIN_PASSWORD environment variables must be set"
    exit 1
fi

ACCESS_TOKEN=$(curl -s -k "https://localhost:18082/token"\
    -H "Content-Type: application/x-www-form-urlencoded"\
    --data-urlencode "client_id=OIDC_Test"\
    --data-urlencode "client_secret=secret" \
    --data-urlencode "grant_type=password" \
    --data-urlencode "scope=openid"\
    --data-urlencode "username=${RTSADMIN_USER}"\
    --data-urlencode "password=${RTSADMIN_PASSWORD}" | jq -r .access_token)

curl -X "GET" -k "https://localhost:3000/ads/runtime/api/v1/decisionServices" \
    -H 'accept: */*' \
    -H 'Content-Type: application/octet-stream' \
    -H "x-ads-internal-service-id: 58AEE13AFFCB2AC937C3048DE3578B97B5797414E228D7D0A0C378F2D4255ACE" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" | jq .