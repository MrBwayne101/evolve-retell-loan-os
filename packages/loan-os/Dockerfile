FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    EVOLVE_REPO_ROOT=/app \
    EVOLVE_DATA_ROOT=/data

WORKDIR /app

COPY packages/loan-os/pyproject.toml packages/loan-os/README.md ./packages/loan-os/
COPY packages/loan-os/src ./packages/loan-os/src
COPY packages/loan-os/render-entrypoint.sh ./packages/loan-os/render-entrypoint.sh
COPY data/voice-agent/retell/proof-config.json ./data/voice-agent/retell/proof-config.json
COPY data/voice-agent/retell-web/dist/client.bundle.js ./data/voice-agent/retell-web/dist/client.bundle.js
COPY data/voice-agent/reactivation-enrichment ./data/voice-agent/reactivation-enrichment

RUN pip install --upgrade pip \
  && pip install ./packages/loan-os gunicorn

RUN mkdir -p /data/voice-agent/retell /data/voice-agent/reactivation-enrichment \
  && cp /app/data/voice-agent/retell/proof-config.json /data/voice-agent/retell/proof-config.json \
  && cp -R /app/data/voice-agent/reactivation-enrichment/. /data/voice-agent/reactivation-enrichment/ \
  && chmod +x /app/packages/loan-os/render-entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/app/packages/loan-os/render-entrypoint.sh"]
CMD ["gunicorn", "loan_os.retell.render_app:app", "--bind", "0.0.0.0:8080", "--worker-class", "aiohttp.GunicornWebWorker", "--workers", "1", "--timeout", "90", "--access-logfile", "-", "--error-logfile", "-"]
