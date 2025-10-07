# spark/Dockerfile
FROM spark:3.5.1-python3

# Create writable dirs as root, then drop back to the 'spark' user
USER root

ENV SPARK_JARS_IVY=/opt/spark/work-dir/.ivy2 \
    ALPHA_TZ_DEFAULT=US/Eastern

RUN mkdir -p /opt/spark/work-dir/.ivy2 /opt/spark/work-dir/apps \
 && chown -R spark:spark /opt/spark/work-dir\
 && pip install --no-cache-dir python-dotenv psycopg2-binary==2.9.9

# copy your consumer.py into the image
COPY --chown=spark:spark spark/consumer.py /opt/spark/work-dir/apps/consumer.py

USER spark

# Submit the job to your standalone master
CMD ["/opt/spark/bin/spark-submit",\
     "--master","spark://spark-master:7077",\
     "--deploy-mode","client",\
     "--conf","spark.jars.ivy=/opt/spark/work-dir/.ivy2",\
     "--conf","spark.executor.extraJavaOptions=-Duser.timezone=UTC",\
     "--conf","spark.driver.extraJavaOptions=-Duser.timezone=UTC",\
     "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",\
     "/opt/spark/work-dir/apps/consumer.py"]
