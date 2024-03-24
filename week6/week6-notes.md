## env setup

- [ ] create the environment
```bash
conda create --prefix ./.env/w6-env python=3.10
```
- [ ] install conda dependencies
```bash
conda install jupyter pandas pyspark python-confluent-kafka avro fastavro chardet
```
- [ ] install pip dependencies
```bash
pip install -r requirements.txt
```

## steps
- [ ] run the env setup
- [ ] run the docker cluster setup
  - [ ] kafka
  - [ ] spark
