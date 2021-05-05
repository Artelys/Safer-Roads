FROM docker.elastic.co/kibana/kibana-oss:7.4.1

ENV PATH /usr/share/kibana/bin:$PATH

COPY build/data_management-0.0.0.zip /tmp
COPY build/risk_prediction-0.0.0.zip /tmp

RUN mkdir /tmp/data

RUN kibana-plugin install file:///tmp/data_management-0.0.0.zip
RUN kibana-plugin install file:///tmp/risk_prediction-0.0.0.zip
