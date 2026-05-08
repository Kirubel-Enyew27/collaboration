# Collaboration Server

## Docker

Build a production Docker image (multi-stage):

```bash
docker build -t collaboration:latest .
```

Run with environment file:

```bash
cp .env.example .env
docker run --env-file .env -p 8080:8080 --name collaboration collaboration:latest
```

Or with docker-compose:

```bash
docker compose up --build
```

Notes:
- Uses multi-stage build to produce a small distroless runtime image.
- `DATABASE_PATH` can be passed via environment; default is `./data.db` inside the container.
- Healthcheck is implemented using a small compiled helper and wired to Docker's `HEALTHCHECK`.