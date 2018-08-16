FROM flink:1.6.0

ENV JYTHON_VERSION="2.5.3"
ENV JYTHON_HOME="/jython"
ENV FLINK_HOME="/opt/flink"

RUN wget -cO jython-installer.jar "http://search.maven.org/remotecontent?filepath=org/python/jython-installer/$JYTHON_VERSION/jython-installer-$JYTHON_VERSION.jar" \
   && java -jar jython-installer.jar -s -t standard -d "$JYTHON_HOME" \
   && rm -rf "$JYTHON_HOME"/{Docs,Demo,tests} \
   && rm -f jython-installer.jar \
   && ln -sfv "$JYTHON_HOME/bin/"* /usr/local/bin/

RUN mkdir bind_mount
# - pip install unittest2
VOLUME ["/opt/flink/bind_mount"]

RUN apt-get update \
   && apt-get install -y --allow-unauthenticated software-properties-common \
   && add-apt-repository ppa:webupd8team/java -y \
   && apt-get update \
   && apt-get install -y --allow-unauthenticated openjdk-8-jdk maven

ENTRYPOINT ["./bind_mount/entrypoint.sh"] 

