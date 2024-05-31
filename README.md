# ETL scripts for the PanKB Website Databases & Self-deployed MongoDB

The repo contains :
- ETL scripts used to populate the PanKB website DEV and PROD databases;
- scripts used to set up a dockerized self-deployed standalone MongoDB instance for the development purposes.

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
git clone --branch develop https://github.com/biosustain/pankb_db.git pankb_db
cd pankb_db
```

## 1. ETL scripts

The ETL (Extract-Transform-Load) scripts:
1) extracts information about pangenomes from the Microsoft Azure Blob Storage *.json files. The storage serves as the data lake;
2) transforms it into the Django- and MongoDB-compatible model;
3) loads the transformed data into a MongoDB database instance.

Initially, the database tables are created by Django web framework, which the PanKB website is built on. It is achieved by setting the parameter `managed = True` in the `models.py` files.

### 1.1. Development configuration on Ubuntu servers

Tested on Linux Ubuntu 20.04 (may need tweaks for other systems) with the following configuration:
- Git
- Python 3.8.10

The python packages versions to be installed can be found in the `requirements.txt` file and installed via:
```
pip install -r requirements.txt
```

### 1.2. Execute the ETL Scripts
Before executing any scripts, create the `.env` file under the subfolder `/etl` with the following content in case of populating a self-deployed MongoDB instance: 
```
## Do not put this file under version control!

MONGODB_NAME = 'pankb'                                    # the db name
MONGO_INITDB_ROOT_USERNAME = 'allDbAdmin'                 # the db admin name                
MONGO_INITDB_ROOT_PASSWORD = '<any password you choose>'  # the db admin pass

## Azure Blob Storage Connection String
BLOB_STORAGE_CONN_STRING = '<copy the Azure Blob Storage connection string from the Azure web portal>'
```
or in case of populating a cloud-based Azure CosmosDB for MongoDB instance:
```
## Do not put this file under version control!

MONGODB_NAME = 'pankb'                                    # the db name
## MongoDB-PROD (Azure CosmosDB for MongoDB) Connection String
MONGODB_CONN_STRING = '<copy the Azure CosmosDB for MongoDB connection string from the Azure web portal>'

## Azure Blob Storage Connection String
BLOB_STORAGE_CONN_STRING = '<copy the Azure Blob Storage connection string from the Azure web portal>'
```
Then, edit the included `etl/config.py` file setting the following parameters:
- the database type (self-deployed or cloud-based MongoDB instance);
- species for which pangenome data should be inserted or modified (all species or only chosen ones);
- a local folder on your machine where the etl scripts' logs will be saved (the folder should be created beforehand);
- whether the logs produced by the individual ETL scripts should be uploaded to the Azure Blob storage or not.

Finally, the ETL scripts must be executed in the following order:
1. `organisms.py`
2. `gene_annotations.py`
3. `gene_info.py`
4. `genome_info.py`
5. `pathway_info.py`
```
python3 <insert the respective script name here>
```
The scripts were not joined into one pipeline, because in practice it is more convenient to run them one by one for the sake of:
- quality control after each step;
- monitoring that the storage and RAM are not running out on the DEV server and CPUs both on the DEV and PROD servers are not overloaded (via "Metrics" section on the Azure Portal or with the help of a Remote IDE, e.g., PyCharm).

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

## MongoDB: Docker Compose Env Variables
MONGO_INITDB_ROOT_USERNAME = 'allDbAdmin'                   # also the remote CosmosDB admin username: DbAdmin
MONGO_INITDB_ROOT_PASSWORD = '<any password you choose>'    # also the remote CosmosDB admin pass
MONGODB_USERNAME = 'pankbDbOwner'
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