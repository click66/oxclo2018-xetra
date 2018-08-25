FROM gettyimages/spark

RUN pip install pytest

ADD entrypoint.sh /spark-submit-entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/bin/bash", "/spark-submit-entrypoint.sh"]
