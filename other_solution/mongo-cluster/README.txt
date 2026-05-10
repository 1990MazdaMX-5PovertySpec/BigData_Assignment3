Step 1:
Using Powershell or CMD (I used Powershell) navigate to the location of this folder

Step 2:
Create the Docker image by using this command:

docker compose up -d


Step 3:
Run the create_docker_image.ps1 or create_docker_image file, depending on
whether you are using Powershell or CMD. For Powershell you can run it like
this:

& .\create_docker_image.ps1


Step 4:
Wait for confirmation that the cluster was created. You should see a message
"Cluster is ready."

Step 5:
Run the Python code through Powershell or CMD, specifying the csv file location
like so:

py Project_3.py --file <path to file>\<file>.csv


Notes:
If you need to redo everything, use this command in Powershell or CMD:

docker compose down -v

If running the files does not work, you can run the commands by hand.