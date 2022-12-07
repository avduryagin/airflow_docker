FROM ubuntu:latest
RUN apt-get update
RUN apt-get install -y python3 python3-pip
COPY requirements.txt /root/requirements.txt
RUN python3 -m pip install -r /root/requirements.txt
#RUN pip3 install apache-airflow
#RUN pip3 install kubernetes
#RUN pip3 install virtualenv cx_Oracle keras=2.8.0 scikit-learn=0.23.2
RUN airflow db init
RUN airflow users create     --username duryagin  --firstname Andrey  --lastname Duryagin --role Admin --email avduryagin@mail.ru  --password nabukh0d0n0s0r
#CMD airflow webserver -p 8080
#CMD airflow scheduler
#CMD airflow standalone
COPY dags /root/airflow/dags
CMD (airflow scheduler &) && airflow webserver

# Fix timezone issue
ENV TZ=Europe/London
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY code /root/code
COPY models /root/models

#--mount type=bind,source="$(pwd)"/dags,target=/root/airflow/dags\
#--mount type=bind,source="$(pwd)"/models,target=/root/airflow/models\
#--mount type=bind,source="$(pwd)"/code,target=/root/airflow/code\

#run -p 8080:8080 --mount type=bind,source="$(pwd)"/dags,target=/root/airflow/dags --mount type=bind,source="$(pwd)"/models,target=/root/airflow/models --mount type=bind
 #,source="$(pwd)"/code,target=/root/airflow/code airflow