# Conda Setup

## Installation

### install latest 64-bit version of miniconda3 for linux
```bash
mkdir -p ~/miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm -rf ~/miniconda3/miniconda.sh
```
### initialize conda in bash
```bash
~/miniconda3/bin/conda init bash
```

## Environment Setup

### create the env
```bash
conda create -n DE-ZoomCamp python=3.11
conda activate DE-ZoomCamp
```
### install common packages
```bash
conda install jupyter pandas numpy matplotlib=3.7.2 scikit-learn seaborn
```
### get sql deps
```bash
conda install sqlalchemy psycopg2 -c anaconda
pip install pgcli
```

# Working with Docker

## Run Postgres in Docker Container
make sure you are in the root directory of the project
```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny-taxi" \
    -v "$(pwd)"/ny_taxi_postgre_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

## Ingesting Data into Postgres

### Build the Docker Image with the Ingestion Script
make sure you are in the directory with the Dockerfile and the ingestion script
```bash
docker build -t taxi_ingest:v001 .
```

### Run the Ingestion Script in the Docker Container
```bash
docker run -it \
    taxi_ingest:v001 \
    --network=docker_default \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny-taxi \
    --table_name=green_taxi \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
```
 
## Working with Terraform

### Initialize Terraform
make sure you are in the root directory of the project where the main.tf file is located
```bash
terraform init
```