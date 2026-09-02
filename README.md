# Flink Stream Studio

Open this repository in VS Code, then run **Dev Containers: Reopen in Container**.
The Dev Container provides Ubuntu 24.04, Java 17, Maven, Node/npm, Docker Compose,
and PostgreSQL client tools. It uses the host Docker daemon through its socket.

## Run the stack

```bash
cd infrastructure
docker compose up -d
```

The Flink dashboard is available at http://localhost:8081.

## Build and run the Flink job

```bash
cd backend
mvn package
cd ../infrastructure
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.studio.flink.SqlPipelineRunner \
  /opt/flink/usrlib/sql-pipeline-runner-1.0-SNAPSHOT.jar \
  /opt/flink/pipelines/pipeline.sql
```

## Run the frontend

```bash
cd frontend
npm install
npm start
```

The React development server is available at http://localhost:3000.
