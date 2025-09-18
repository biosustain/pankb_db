# The PanKB website DBs: ETL & Self-deployed MongoDB instance 

The repo contains :
- ETL scripts used to populate the PanKB website DEV, PREPROD and PROD databases;
- scripts used to set up a dockerized self-deployed standalone MongoDB instance for the development purposes.

## Authors

Liubov Pashkova, liupa@dtu.dk

Pascal A. Pieters, paspie@biosustain.dtu.dk

Daniel C. Romero Yianni, danyia@dtu.dk

## Set up the common git repository
Create the necessary directories if they do not exist:
```
sudo mkdir -p /projects
cd /projects
sudo mkdir -p pankb_web
sudo chown -R $USER pankb_web
cd pankb_web
```
Clone the PanKB git repo into the subdirectory /pankb_db and change to it:
```
git clone --branch main https://github.com/biosustain/pankb_db.git pankb_db
cd pankb_db
```

## 1. ETL scripts

The ETL (Extract-Transform-Load) scripts:
1) extract information about pangenomes from the Microsoft Azure Blob Storage *.json files. The storage serves as the data lake;
2) transform it into the Django- and MongoDB-compatible model;
3) load the transformed data into a MongoDB database instance;
4) (optionally) upload the logs needed for statistics and quality control to the Azure Blob Storage after the pipeline scripts are executed.

Initially, the database tables are created by Django web framework, which the PanKB website is built on. It is achieved by setting the parameter `managed = True` in the `models.py` files.

### 1.1. Development configuration on Ubuntu servers

Tested on Linux Ubuntu 20.04 (may need tweaks for other systems) with the following configuration:
- Git
- Python 3.8.10

The python packages versions to be installed can be found in the `requirements.txt` file and installed via:
```
pip install -r requirements.txt
```
or 
```
pip3 install -r requirements.txt
```

### 1.2 Copy BGCFlow results to Azure

After running the [PanKB fork of BGCFlow](https://github.com/pascalaldo/bgcflow) for your species of interest, copy its data to the relevant Azure blob (when using an existing setup, you can check which storage account is being used in the `BLOB_STORAGE_CONN_STRING` varaible in `.env`, which contains the storage account name you can find in Azure: `https://<storage_account_name>.blob.core.windows.net`). Currently, we are using this [pankb storage account](https://portal.azure.com/#@dtudk.onmicrosoft.com/resource/subscriptions/aee8556f-d2fd-4efd-a6bd-f341a90fa76e/resourceGroups/rg-recon/providers/Microsoft.Storage/storageAccounts/pankb/overview).

Generally, you simply need to copy the the species directory from `<bgcflow_repo>/data/processed/species/pankb/web_data/species/<species_name>` to `<blob_url>/data/PanKB/web_data_v2/species/<species_name>` using `azcopy` (remember to always test first with `--dry-run` and use `--overwrite=false` unless you specifically need to update existing files).

If you need more information about which specific files are needed, the blob and path can be found in the `BlobConnection` class in `connections.py`. The specific files required can subsequently be found in each of the ETL scripts.

You can use a script like this as a template (note that it requires you set the `SAS` environment variable to a [SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens) with read/write permissions on the blob):
```
#!/usr/bin/bash

source .env

PANKB_WEB_DATA="https://<storage_account_name>.blob.core.windows.net/data/PanKB/web_data_v2"
LOCAL_WEB_DATA="<bgcflow_path>/data/processed/species/pankb/web_data/species"

for species in $(ls "${LOCAL_WEB_DATA}/"); do
    echo "--- $species ---"

    azcopy copy --recursive --overwrite=false --follow-symlinks \
        "$LOCAL_WEB_DATA/$species" \
        "${PANKB_WEB_DATA}/species/?$SAS" --dry-run
done
```


### 1.3. Execute the ETL Scripts
Before executing any scripts, create the `.env` file under the subfolder `/etl` with the following content in case of populating a self-deployed MongoDB instance: 
```
## Do not put this file under version control!

# The MongoDB database name
MONGODB_NAME = 'pankb' 

# The MongoDB root username
MONGO_INITDB_ROOT_USERNAME = 'allDbAdmin'    

# The MongoDB root password                          
MONGO_INITDB_ROOT_PASSWORD = '<any password you choose>'  

## Azure Blob Storage Connection String
BLOB_STORAGE_CONN_STRING = '<copy the Azure Blob Storage connection string from the Azure web portal>'                                 
```
or in case of populating a cloud-based Azure CosmosDB for MongoDB instance:
```
## Do not put this file under version control!

# The MongoDB database name
MONGODB_NAME = 'pankb'          
                          
## MongoDB-PROD (Azure CosmosDB for MongoDB) Connection String
MONGODB_CONN_STRING = '<copy the Azure CosmosDB for MongoDB connection string from the Azure web portal>'

## Azure Blob Storage Connection String
BLOB_STORAGE_CONN_STRING = '<copy the Azure Blob Storage connection string from the Azure web portal>'
```
Then, edit the included `etl/config.py` file setting the following parameters:
- the database type (self-deployed or cloud-based MongoDB instance);
- species for which pangenome data should be inserted or updated (all species or only chosen ones);
- a local folder on your machine where the etl scripts' logs will be saved (the folder should be created beforehand).

Finally, the ETL scripts must be executed (with e.g. `python3 <script_name.py>`) with pankb_stats_nova.py running at last:
1. `organism_nova.py`
2. `pangene_nova.py`
3. `genome_nova.py`
4. `pathway_nova.py`
5. `gene_nova.py`
6. `isolation_nova.py` (has some interactivity to do semi-manual annotation of isolation source strings)
7. `phylons.py`
8. `pankb_stats_nova.py`

It is safe to run the scripts multiple times for the same pangenome analysis (data is updated, not duplicated).

The scripts were not joined into one pipeline, because in practice it is more convenient to run them one by one for the sake of:
- quality control after each step;
- monitoring that the storage and RAM are not running out on the DEV server and CPUs both on the DEV and PROD servers are not overloaded (via "Metrics" section on the Azure Portal or with the help of a Remote IDE, e.g., PyCharm).

A good practice is to clean up unneccessary docker images and containers and restart the docker daemon after with the following commands:
```
docker system prune
sudo systemctl restart docker
```

Another good practice is to prepend the commands with `nice -n 19` when running them on the prod server (sets lowest possible resource usage priority and reduces possible slowdowns of the web server). Also, on the prod server it is a good idea to keep track of RAM usage as you run these scripts and kill them if they start hoarding too much memory.   
       
For Pankb, we are currently using the self-deployed MongoDB in respective virtual machines.  

## 2. Self-deployed MongoDB

### 2.1. Development configuration on Ubuntu servers

Tested on Linux Ubuntu 20.04 (may need tweaks for other systems) with the following configuration:
- Git
- Docker & Docker Compose

### 2.2. Build the MongoDB docker container
Create a directory to be mounted as a docker volume:
```
mkdir -p projects/pankb_web/docker_volumes/{mongodb}
```
Create a file with the name ".env" under the /projects/pankb_web/pankb_db/mongodb folder in the following format (do not forget to choose your own MONGO_INITDB_ROOT_PASSWORD and MONGODB_PASSWORD and optionally other fields):
```
## Do not put this file under version control!

# The MongoDB root username
MONGO_INITDB_ROOT_USERNAME = 'allDbAdmin'    

# The MongoDB root password                
MONGO_INITDB_ROOT_PASSWORD = '<any password you choose>'   

# The MongoDB database admin password
MONGODB_USERNAME = 'pankbDbOwner'

# The MongoDB database admin password 
MONGODB_PASSWORD = '<any password you choose>'

MONGODB_AUTH_SOURCE = 'pankb'
```
Change to the appropriate folder and build the containers with Docker Compose:
```
cd /projects/pankb_web/pankb_db/mongodb
docker compose up -d --build
```
The MongoDB instance must now be ready to accept connections at the port 27017 (standard for MongoDB). The command `docker ps` should show the docker container up and running, e.g.:
```
>>> docker ps
CONTAINER ID   IMAGE                            COMMAND                  CREATED       STATUS        PORTS                                           NAMES
deac2ceebf43   mongo:6.0-rc                     "docker-entrypoint.s…"   2 weeks ago   Up 41 hours   0.0.0.0:27017->27017/tcp, :::27017->27017/tcp   pankb-mongodb
```
