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
cd /workspaces/flink-ui/bbackend
mvn package
cd /workspaces/flink-ui/infrastructure
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.studio.flink.SqlPipelineRunner \
  /opt/flink/usrlib/sql-pipeline-runner-1.0-SNAPSHOT.jar \
  /opt/flink/pipelines/pipeline.sql
  
```

validate compose file"

```
docker compose config --quiet && echo "compose valid"
```

update changes

```bash
cd /workspaces/flink-ui/backend/ & mvn clean package &&\
cd /workspaces/flink-ui/infrastructure &&\
docker compose down &&\
docker compose pull &&\
docker compose up -d &&\
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

The React development server is available at http://localhost:5173.


### reference

shading

e right fix here is the ServicesResourceTransformer, not exclusions. Two of your overlaps are Java SPI registration files (META-INF/services/...), and simply excluding/keeping-first-wins on those will silently break your job at runtime:

META-INF/services/org.apache.flink.table.factories.Factory — this is how Flink discovers the kafka and jdbc table connectors. Without merging, only one connector factory survives in the uber-jar and the other throws NoMatchingTableFactoryException at deploy time.
META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory — same issue, but for the Postgres JDBC dialect.
META-INF/services/com.fasterxml.jackson.databind.Module — same issue, for Jackson's JSR-310/JDK8 datatype modules.
The LICENSE/NOTICE/DEPENDENCIES/MANIFEST.MF overlaps are harmless text-file duplicates — those are fine to just drop via <filters>.

module-info.class (including the multi-release META-INF/versions/9/module-info.class variant) — Jackson/commons-lang3 ship JPMS module descriptors for when they're run on the module path. Since your uber-jar runs on the classpath (not as a JPMS module), these are inert and safe to drop.
META-INF/MANIFEST.MF — every jar has one; you already have ManifestResourceTransformer regenerating the merged manifest, so the duplicates from source jars are noise.


## restart link n conaintes

docker compose down && docker compose up -d flink-jobmanager flink-taskmanager && sleep 5 && docker compose logs flink-taskmanager 2>&1 | grep -i "unknown module\|jdk.compiler" | head -5; echo "---done---"






### cleanup

cd /workspaces/flink-ui/infrastructure && docker compose down -v --remove-orphans
docker volume prune -f 
docker images --format "{{.Repository}}:{{.Tag}}" | grep -iE "flink|kafka|zookeeper|postgres" | xargs -r docker rmi
rm -rf /workspaces/flink-ui/backend/target /workspaces/flink-ui/frontend/node_modules /workspaces/flink-ui/frontend/dist


### to brign everything up

cd frontend && npm install                 # regenerate node_modules
cd ../backend && mvn clean package         # regenerate target/*.jar
cd ../infrastructure && docker compose up -d   # re-pulls all images automatically
cd /workspaces/flink-ui/bbackend
mvn package
cd /workspaces/flink-ui/infrastructure
docker compose exec flink-jobmanager /opt/flink/bin/flink run \
  -c com.studio.flink.SqlPipelineRunner \
  /opt/flink/usrlib/sql-pipeline-runner-1.0-SNAPSHOT.jar \
  /opt/flink/pipelines/pipeline.sql

docker compose logs kafka-init && echo "---topics---" && docker compose exec kafka kafka-topics --list --bootstrap-server kafka:29092


cd /workspaces/flink-ui/infrastructure && docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic orders <<'EOF'
{"product_id":"sku-1","amount":10.50,"order_time":"2026-09-02 09:00:00"}
{"product_id":"sku-1","amount":22.00,"order_time":"2026-09-02 09:15:00"}
{"product_id":"sku-2","amount":5.75,"order_time":"2026-09-02 09:20:00"}
{"product_id":"sku-1","amount":8.25,"order_time":"2026-09-02 09:45:00"}
{"product_id":"sku-2","amount":13.00,"order_time":"2026-09-02 10:05:00"}
{"product_id":"sku-1","amount":9.99,"order_time":"2026-09-02 10:20:00"}
EOF


/workspaces/flink-ui/infrastructure (main) $ docker compose exec postgres psql -U postgres -d analytics_db -c "SELECT * FROM elastic_order_metrics ORDER BY window_start;"

sku-1 and sku-2 both appear across multiple overlapping windows (expected — that's how HOP windows work: each event lands in every window it falls within)
total_revenue sums correctly per product/window (e.g. sku-1 window 08:30–09:30 = 32.5, consistent with orders at 09:00 and 09:15 both landing in that window)
total_orders shows 0 for every row — that's a pre-existing gap: pipeline.sql's INSERT INTO postgres_metrics only computes SUM(amount), never a COUNT(*) for total_orders, even though the target table has that column with a DEFAULT 0.

