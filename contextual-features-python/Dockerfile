FROM amazoncorretto:8

WORKDIR /code

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

RUN python3 -m pip install --upgrade pip

RUN python3 -m pip install --quiet --no-cache-dir \
    'torch==1.7.1+cu101' \
    'torchvision==0.8.2+cu101' \
    'torchaudio==0.7.2' -f https://download.pytorch.org/whl/torch_stable.html

RUN python3 -m pip install --quiet --no-cache-dir \
    'pyspark==3.1.1' \
    'sentence-transformers==1.2.0' \
    'symspellpy' \
    'pandas' \
    'pyarrow'

# load weights for cached testing
RUN python3 -c "from sentence_transformers import SentenceTransformer; model = SentenceTransformer('paraphrase-MiniLM-L3-v2'); model.save('huggingface/paraphrase-MiniLM-L3-v2')"
RUN python3 -m pip install -U pytest
RUN pip3 install pex

RUN zip -r weights.zip huggingface

# Cache layers above and only run rest of image on code change:
# Creating folders, and files for a project:
COPY ./ ./

RUN zip -r project.zip contextual_features