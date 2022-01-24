## How to start the project

In your /docker/.env file, add the following variables:

```
DATA_DIR="/.../<cloned-dir>/src/etl_data"
DAGS_DIR="/.../<cloned-dir>/dags/data"
```

```
make build
make build-up
```

The following services should start:
```
➜  project git:(master) ✗ docker ps
CONTAINER ID   IMAGE                        COMMAND                  CREATED        STATUS                   PORTS                              NAMES
357b712341f4   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   5 hours ago    Up 5 hours               8080/tcp                           xxx_airflow-scheduler_1
372d440f1b6a   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   5 hours ago    Up 5 hours (healthy)     0.0.0.0:5555->5555/tcp, 8080/tcp   xxx_flower_1
77eadeb3c0aa   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   5 hours ago    Up 5 hours (healthy)     0.0.0.0:8080->8080/tcp             xxx_airflow-webserver_1
7f68845efbc3   apache/airflow:2.2.3         "/usr/bin/dumb-init …"   5 hours ago    Up 5 hours               8080/tcp                           xxx_airflow-worker_1
af9fb24ad6ff   postgres:13                  "docker-entrypoint.s…"   5 hours ago    Up 5 hours (healthy)     5432/tcp                           xxx_postgres_1
4dadbd44de13   postgres                     "docker-entrypoint.s…"   5 hours ago    Up 5 hours               0.0.0.0:5433->5432/tcp             xxx_db_app_1
432837c58485   redis:latest                 "docker-entrypoint.s…"   5 hours ago    Up 5 hours (healthy)     0.0.0.0:6379->6379/tcp             superset_cache
04ee1864109a   apache/superset:latest-dev   "/app/docker/docker-…"   14 hours ago   Up 7 hours (unhealthy)   8088/tcp                           superset_worker
8254b5f3d984   apache/superset:latest-dev   "/app/docker/docker-…"   14 hours ago   Up 7 hours (unhealthy)   8088/tcp                           superset_worker_beat
b35323ed9f27   apache/superset:latest-dev   "/app/docker/docker-…"   14 hours ago   Up 7 hours (healthy)     0.0.0.0:8088->8088/tcp             superset_app
aeefa7a99c4e   postgres:10                  "docker-entrypoint.s…"   14 hours ago   Up 7 hours               5432/tcp                           superset_db

```

