FROM ubuntu:latest 
WORKDIR /usr/src/app
ENV USER app_user
ENV HOME /home/$USER
ADD telus_chargeback_reporting/ /usr/src/app/
COPY requirements.txt requirements.txt
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip 
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
