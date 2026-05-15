FROM apache/spark:3.5.8

# Apache Spark 3.5.8 идёт с Python 3.10. Airflow 2.9.3-python3.11 — с 3.11.
# PySpark требует совпадения minor-версии Python между driver и executor,
# иначе [PYTHON_VERSION_MISMATCH] при foreachPartition / JDBC write.
# Ставим Python 3.11 в Spark-контейнер и подсовываем его PySpark через ENV.

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      software-properties-common gnupg ca-certificates \
 && apt-get install -y --no-install-recommends python3.11 python3.11-distutils \
 && rm -rf /var/lib/apt/lists/*

# Создаём пользователя airflow с uid=50000 — таким же, как в apache/airflow.
# Нужно для запуска spark-worker под этим uid: driver (Airflow) и executor
# (worker) пишут в общий volume под одним пользователем, нет конфликтов прав.
# Hadoop UserGroupInformation требует записи в /etc/passwd, иначе worker
# падает на старте с LoginException.
RUN useradd -u 50000 -m -s /bin/bash airflow \
 && mkdir -p /opt/spark/work \
 && chown -R airflow:airflow /opt/spark/work

# PySpark на executor найдёт python3.11 по этому пути.
ENV PYSPARK_PYTHON=/usr/bin/python3.11
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11

USER spark
