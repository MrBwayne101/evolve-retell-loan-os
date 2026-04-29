#!/usr/bin/env bash
set -euo pipefail

mkdir -p /data/voice-agent/retell /data/voice-agent/reactivation-enrichment

if [[ ! -f /data/voice-agent/retell/proof-config.json && -f /app/data/voice-agent/retell/proof-config.json ]]; then
  cp /app/data/voice-agent/retell/proof-config.json /data/voice-agent/retell/proof-config.json
fi

if ! find /data/voice-agent/reactivation-enrichment -maxdepth 1 -name 'launch-batch-2026-04-28*.csv' -print -quit | grep -q .; then
  if [[ -d /app/data/voice-agent/reactivation-enrichment ]]; then
    cp -R /app/data/voice-agent/reactivation-enrichment/. /data/voice-agent/reactivation-enrichment/
  fi
fi

exec "$@"
