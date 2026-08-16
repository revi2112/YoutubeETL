docker context ls 
 1. docker runs container on one machine
 2. docker compose coordinates multiple containers on the same machine - redis, posegres, webser, scheduler etc...
 3. kubernates coordinates containers across multiple machines (a cluster) handles stuff like auto schalling , if a machine dies restart the worker on a differnt machine etc.. 

 Kubernetes orchestrates containers across machines → Docker/Compose runs containers on a machine → Airflow orchestrates your DAG's tasks inside those containers.\


 docker exec -it airflow-webserver bash for airflow ui
docker build -t revi1202/youtube_etl:1.0.1 .

 Bind mount: ./dags:/opt/airflow/dags links your Mac folder directly to the container's folder — they're the same files, not copies. Saving in VS Code changes the file on disk, which the container sees instantly. No rebuild/restart needed for DAG code changes.

 production -> In a production scenario, you would have a dedicated server where airflow is running 24 seven, be
it self-managed or on the cloud, most likely running on some Kubernetes environment.

local - containers need to be up nd running.

docker exec -it postgres psql -u yt_api_user -d elt_db
\du -> list of conne
\dn -> list of schema
\q
\d staging.yt_api
\dt -> public schema relations

ALTER USER yt_api_user WITH PASSWORD '';
elt_db=> alter table staging.yt_api alter column "Video_Title" TYPE TEXT;



docker build -t revi1202/youtube_etl:1.0.1 .
docker login -u revi1203

docker compose pull
docker compose up -d  --force-recreate