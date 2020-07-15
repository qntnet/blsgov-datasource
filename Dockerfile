FROM debian:9.5
# registry.quantnet-ai.ru/quantnet/blsgov:dev

RUN apt update && apt -y install curl bzip2 openssh-client \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda update conda \
    && apt -y remove curl bzip2 openssh-client \
    && apt -y autoremove \
    && apt autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean -tipsy && conda clean --all --yes

RUN conda install -y python=3.7 flask=1.1.2 pyquery=1.4.0 portalocker=1.5 gunicorn=20.0 \
     && conda clean -tipsy && conda clean --all --yes

COPY . /opt/

# server | update
ENV RUN_MODE=server
ENV REGISTRATION_KEY=''

WORKDIR /opt/

CMD sh run.sh $RUN_MODE
