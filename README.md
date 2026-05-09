# BigData_Assignment3

**SETUP REPLICAS IN DOCKER**
1. Open terminal
2. Run command ```sudo nano /etc/hosts```
3. Paste ```127.0.0.1 mongo1 mongo2 mongo3```
4. Press ```Ctrl + O```, then ```Enter``` to save. Press ```Ctrl + X``` to exit
5. In your terminal, navigate to the cloned repository where ```docker-compose.yml``` file is located
6. Start the container in the background ```docker compose up -d```
7. Run this command:
   ```
   docker exec -it mongo1 mongosh --port 27017 --eval 'rs.initiate({
     _id: "rs0",
     members: [
       {_id: 0, host: "mongo1:27017"},
       {_id: 1, host: "mongo2:27018"},
       {_id: 2, host: "mongo3:27019"}
     ]
   })'
   ```
