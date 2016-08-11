FROM python:2-onbuild
MAINTAINER dbaskette@pivotal.io

WORKDIR /usr/local/
RUN git clone https://github.com/dbbaskette/cape.git
WORKDIR /usr/local/cape
ENTRYPOINT [ "python", "cape.py"]
EXPOSE 22

#cmd ["ls"]
 
