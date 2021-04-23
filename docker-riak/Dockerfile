FROM debian:buster-slim

# Install deps
RUN apt-get update -qq  && apt-get upgrade -y  && apt-get install -y libncurses5 logrotate sudo

# Install Riak
COPY riak_2.9.8-1_amd64.deb /
RUN dpkg -i /riak_2.9.8-1_amd64.deb && rm /riak_2.9.8-1_amd64.deb
RUN echo "ulimit -n 65536" | tee -a /etc/default/riak

# Copy startup files
COPY riak-start.sh /riak-start.sh
RUN chmod a+x /riak-start.sh
COPY riak.conf /etc/riak/riak.conf
COPY advanced.config /etc/riak/advanced.config

# Protocol Buffers
EXPOSE 8087

# HTTP API
EXPOSE 8098

# Cluster manager
EXPOSE 9080

CMD ["/riak-start.sh"]
