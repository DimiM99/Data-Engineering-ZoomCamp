## env setup

- [ ] create the environment
```bash
conda create --prefix ./.env/w6-env python=3.10    
```
- [ ] install conda dependencies
```bash
conda install python-confluent-kafka avro fastavro 
```
- [ ] install pip dependencies
```bash
pip install -r requirements.txt
```