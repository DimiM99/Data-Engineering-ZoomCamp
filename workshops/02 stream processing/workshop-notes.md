### python env setup
I will not be using pyenv or virtualenv for this workshop.\
I will be using conda to manage my python environment.

```bash
cd w6-ws2-stream-processing-project
conda create --prefix ./.venv/w6-ws2-risingwave-env python=3.9
conda activate ./.venv/w6-ws2-risingwave-env
pip install -r requirements.txt      
```