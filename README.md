# Jellycat Analytics

Jellycat analytics is my analytics platform for how to calculate sentiments towards a jellycat.

## What you need

- Apache Spark (PySpark)
- Apache Flink
- ClickHouse
- NATs

Kubernetes is preferred.

### What I might add

- transformers? I'm sure I can make these analytics better.


## Setup

Local development uses Docker Compose; production targets Kubernetes with External Secrets.

1) Environment variables (copy into your shell or a local `.env` you source yourself):

- Social APIs
	- REDDIT_CLIENT_ID=
	- REDDIT_CLIENT_SECRET=
	- REDDIT_USER_AGENT=jellycat_sentiment_app
	- TWITTER_API_KEY=
	- TWITTER_API_SECRET=
	- TWITTER_ACCESS_TOKEN=
	- TWITTER_ACCESS_TOKEN_SECRET=
	- FB_ACCESS_TOKEN=

- NATS
	- NATS_URL=nats://localhost:4222
	- NATS_SUBJECT=jellycat_reviews
	- NATS_STREAM=jellycat_reviews
	- NATS_CONSUMER=spark_consumer

- ClickHouse
	- CLICKHOUSE_URL=jdbc:clickhouse://localhost:8123/default
	- CLICKHOUSE_USER=default
	- CLICKHOUSE_PASSWORD=

Python code should read env with `os.getenv`. For local dev only, you may use python-dotenv:

	from dotenv import load_dotenv; load_dotenv()  # then use os.getenv("REDDIT_CLIENT_ID")

2) Python deps (choose one):

- uv (recommended):

	uv sync

- Poetry:

	poetry install

- pip (fallback):

	pip install -r requirements.txt

3) Start local infra with Docker Compose:

	docker compose up -d

This brings up NATS (with JetStream) and ClickHouse. ClickHouse is initialized with `default.jellycat_sentiments`.

4) Kubernetes (prod/staging):

- Install External Secrets Operator in your cluster (see https://external-secrets.io/).
- Create a ClusterSecretStore for your provider (example AWS Secrets Manager is in `k8s/base/externalsecret.yaml`).
- Create secrets in your provider with keys matching the properties referenced.
- Deploy shared base resources (NATS, ClickHouse, producer, ExternalSecrets) with Kustomize:

	kubectl create ns jellycat
	kubectl apply -k k8s/base

Then deploy Spark and Flink jobs via Helm:

	helm upgrade --install jellycat-jobs ./helm/jellycat-jobs -n jellycat \
		--set image.owner=Billy-Davies-2 \
		--set image.spark.tag=latest \
		--set image.flink.tag=latest

Images in manifests/values use your registry owner (`ghcr.io/Billy-Davies-2/...`). Push your images and update tags as needed in Helm values.

### Sentiment via Transformers

This repo uses Hugging Face transformers for sentiment.

- Default model: `distilbert-base-uncased-finetuned-sst-2-english`
- Override via env: `TRANSFORMERS_MODEL=cardiffnlp/twitter-roberta-base-sentiment`
- CPU-only by default. Torch is added as a dependency; on aarch64 you may need to pin a compatible wheel or use a base image with PyTorch preinstalled.

Notes:
- `producer.py` now uses `facebook-sdk` (`from facebook import GraphAPI`).
- Fixed minor typos in Spark job and sentiment list.
