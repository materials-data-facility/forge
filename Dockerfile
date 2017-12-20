FROM jupyter/scipy-notebook

LABEL maintainer="Ben Galewsky, National Data Service"

USER root

COPY ./ /forge/
RUN chmod -R 777 /forge

USER $NB_USER
RUN cd /forge; \
    pip install --user -e .

CMD ["sh", "-c", "/forge/start-notebook-with-examples.sh --NotebookApp.token=${APP_TOKEN}"]
