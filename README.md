## Docker & ECR Commands

### Push Docker image to ECR

\```bash
docker build -t bbw-app .
docker tag bbw-app:latest <YOUR_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest
aws ecr get-login-password --region eu-central-1 --profile berlinbikewatch | docker login --username AWS --password-stdin <YOUR_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com
docker push <YOUR_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com/bbw-app:latest
\```

### Authenticate remote server (e.g. Contabo) to pull from ECR

Generates a short-lived (~12h) login token locally and pipes it over SSH, so long-lived AWS credentials never touch the remote server.

\```bash
aws ecr get-login-password --region eu-central-1 --profile berlinbikewatch | ssh root@<YOUR_SERVER_IP> "docker login --username AWS --password-stdin <YOUR_ACCOUNT_ID>.dkr.ecr.eu-central-1.amazonaws.com"
\```

### Pull and redeploy on the remote server

\```bash
cd ~/bbw
docker compose pull
docker compose up -d --force-recreate producer consumer loader
\```