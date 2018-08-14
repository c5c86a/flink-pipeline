FROM flink:1.6.0

ENV JYTHON_VERSION="2.5.3"
ENV JYTHON_HOME="/jython"
ENV FLINK_HOME="/opt/flink"

RUN wget -cO jython-installer.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/$JYTHON_VERSION/jython-installer-$JYTHON_VERSION.jar" 
RUN    java -jar jython-installer.jar -s -t standard -d "$JYTHON_HOME"
RUN    rm -rf "$JYTHON_HOME"/{Docs,Demo,tests} 
RUN    rm -f jython-installer.jar 
RUN    ln -sfv "$JYTHON_HOME/bin/"* /usr/local/bin/

RUN mkdir bind_mount
# - pip install unittest2
VOLUME ["/opt/flink/bind_mount"]


RUN apt-get update
RUN apt-get install -y --allow-unauthenticated software-properties-common
RUN add-apt-repository ppa:webupd8team/java -y 
RUN apt-get update
RUN apt-get install -y --allow-unauthenticated openjdk-8-jdk maven

ENTRYPOINT ["bash", "bind_mount/entrypoint.sh"] 

