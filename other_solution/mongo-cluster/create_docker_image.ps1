docker compose up -d

# PowerShell uses Start-Sleep, not sleep
Write-Host "Waiting for MongoDB nodes to start..."
Start-Sleep -Seconds 15

docker cp init/01-init-configsvr.js configsvr1:/init-configsvr.js
docker exec -it configsvr1 mongosh /init-configsvr.js

docker cp init/02-init-shard1.js shard1a:/init-shard1.js
docker exec -it shard1a mongosh /init-shard1.js

docker cp init/03-init-shard2.js shard2a:/init-shard2.js
docker exec -it shard2a mongosh /init-shard2.js

docker cp init/04-init-shard3.js shard3a:/init-shard3.js
docker exec -it shard3a mongosh /init-shard3.js

Write-Host "Waiting for replica set elections to complete..."
Start-Sleep -Seconds 30

docker cp init/05-init-router.js mongos:/init-router.js
docker exec -it mongos mongosh /init-router.js

Write-Host "Cluster is ready."