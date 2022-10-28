FROM ubuntu

ENV OPERATOR=/usr/local/bin/CSDInstance \
    USER_UID=root \
    CSDTBLFILE=/json_data/

RUN apt update -y
RUN apt upgrade -y
RUN apt install -y curl build-essential
    
COPY _output/CSDInstance ${OPERATOR}
COPY GeneratedCSDTableManager.json /json_data/GeneratedCSDTableManager.json
COPY GeneratedTable.json /json_data/GeneratedTable.json

ENTRYPOINT ["/usr/local/bin/CSDInstance"]