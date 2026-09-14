# Deployment Guide

How BerlinBikeWatch gets from a code change on your laptop to running on the Contabo server.

> Actual values for every `<PLACEHOLDER>` below (account ID, server IP, passwords) live in `.credentials` (gitignored, never commit it). Load them into your shell before running anything here:
> ```bash
> set -a; source .credentials; set +a
> ```
> That exports `AWS_ACCOUNT_ID`, `CONTABO_SERVER_IP`, and the rest as env vars, so every command below can be copy-pasted as-is (they reference `$AWS_ACCOUNT_ID` / `$CONTABO_SERVER_IP`).

## Facts you need every time

| What | Value |
|---|---|
| AWS account ID | `$AWS_ACCOUNT_ID` (see `.credentials`) |
| AWS region | `eu-central-1` |
| AWS CLI profile (local) | `berlinbikewatch` |
| ECR image | `$AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest` |
| Server | `root@$CONTABO_SERVER_IP` (hostname `vmd201164`) |
| Server project path | `~/bbw` |
| Services in `docker-compose.yaml` | `kafka1`, `kafka-ui`, `postgres`, `producer`, `consumer`, `loader`, `streamlit-frontend` |
| Ports | Kafka `9092`/`9093`/`9094` · Kafka UI `8080` · Postgres `5432` · Dashboard `8501` |

## Architecture

One shared Docker image (`bbw-app`, built from the repo's `Dockerfile`) runs every Python stage of the pipeline — `producer`, `consumer`, `loader`, `streamlit-frontend` are the **same image**, each just running a different `command:` in `docker-compose.yaml`. `kafka1`, `kafka-ui`, and `postgres` are unmodified upstream images. Everything is defined in one `docker-compose.yaml`, deployed as one stack on the Contabo server.

## Normal redeploy (you changed some Python code)

Run on your **local machine**, from the repo root (after `source .credentials` as above):

```bash
# 1. Build the image with your latest code
docker build -t bbw-app .

# 2. Tag it for ECR and push
docker tag bbw-app:latest $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest
aws ecr get-login-password --region eu-central-1 --profile berlinbikewatch \
  | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com
docker push $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest

# 3. Authorize the server to pull (short-lived token, no AWS creds stored on the server)
aws ecr get-login-password --region eu-central-1 --profile berlinbikewatch \
  | ssh root@$CONTABO_SERVER_IP "docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com"
```

Then on the **server**:

```bash
ssh root@$CONTABO_SERVER_IP
cd ~/bbw
docker compose pull
docker compose up -d --force-recreate producer consumer loader streamlit-frontend
```

That's the whole loop. `kafka1`/`kafka-ui`/`postgres` don't need recreating unless you changed their config in `docker-compose.yaml`.

## First-time server setup / adding a new service

If `docker-compose.yaml` on the server is missing a service that exists in the repo (like when `streamlit-frontend` was added), copy the block over by hand or `scp` the whole file — then:

```bash
cd ~/bbw
docker compose pull
docker compose up -d --force-recreate <new-service-name>
```

If you're exposing a new port (like `8501` for the dashboard), open it in the server's firewall:

```bash
sudo ufw allow 8501
```

## Checking it worked

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker compose logs -f streamlit-frontend   # or producer / consumer / loader
```

Dashboard is then at `http://$CONTABO_SERVER_IP:8501`.

## Troubleshooting

**`pull access denied for bbw-app, repository does not exist or may require 'docker login'`**
The server's `docker-compose.yaml` has `image: bbw-app` (a bare name) instead of the full ECR path. `docker compose pull` looks on Docker Hub for a bare name, and there's no public `bbw-app` there. Fix — on the server:
```bash
sed -i "s|image: bbw-app|image: $AWS_ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest|" ~/bbw/docker-compose.yaml
```
Then re-run the docker login step above and `docker compose pull`.

**`failed to bind port 0.0.0.0:9092/tcp: address already in use`**
Something else on that machine already holds the port `kafka1` wants (9092/9093/9094). Find it:
```bash
ss -ltnp | grep 9092
```
Either stop whatever that is, or remap `kafka1`'s host port in `docker-compose.yaml` (e.g. `"19092:9092"`) if you need both running side by side.

**Redshift tiles in the dashboard show a connection error**
Redshift Serverless only accepts inbound connections from IPs allowlisted in its security group. The Contabo server's IP is already allowlisted (that's why `loader` works) — if you're instead running the dashboard from your laptop or anywhere else, that IP needs to be added: AWS Console → Redshift Serverless → your workgroup → Network and security → security group → Inbound rules → add your IP on port `5439`.

**A container shows `Created` but never starts, or `Exited` immediately**
Check its logs first, always:
```bash
docker compose logs <service-name>
```

## Don't run the stack on your laptop and the server at the same time

Running `docker compose up` locally *and* on Contabo means two `loader` instances could race to load the same S3 files into Redshift, and two `producer`s double-polling NextBike. Use your laptop only for building/testing images, not for running the live pipeline — the live one lives on Contabo.
