From ubuntu:latest
MAINTAINER GrupoFixe
RUN apt-get update -y
RUN apt-get install -y python-pip python-dev build-essential
RUN export 
COPY . /donorschoose
WORKDIR /donorschoose
RUN pip install -r requirements.txt
ENTRYPOINT ["python"]
CMD ["spark_server2.py"]
