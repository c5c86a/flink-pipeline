FROM centos

RUN yum -y install nc

EXPOSE 8080

ENTRYPOINT ["nc", "-l", "8080"]
