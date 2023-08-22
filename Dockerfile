FROM python:3.7-slim-buster

WORKDIR /usr/src/app


RUN apt update ; apt install -y  \
      ca-certificates   \
      bash              \
      coreutils         \
      gcc               \
      git               \
      make              \
      wget              \
      gnupg             \
      python3-mysqldb ;

RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com {KEY}
RUN curl -LsS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup | bash
RUN apt install -y apt-transport-https
RUN apt update ; apt upgrade; \
    apt install -y  libmariadb3 libmariadb-dev;


COPY requirements.txt ./
RUN  pip3.7 install --upgrade pip && \
     pip3.7  install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python3.7", "./main.py" ]
