FROM hysds/pge-base:latest

MAINTAINER mkarim "Mohammed.R.Karim@jpl.nasa.gov"
LABEL description="multi acquisition-localizer"

ARG git_oauth_token
ARG id
ARG gid

# provision
USER root
#RUN set -ex && \
 # usermod -u ${id} ops && \
  #groupmod -g ${gid} ops && \
  #chown -R ops:ops /home/ops/ /data && \
  #usermod -g ${gid} ops

# copy config
#COPY .aws /home/ops/.aws
#COPY .netrc /home/ops/.netrc

# copy packages
COPY . /home/ops/verdi/ops/multi_acquisition_localizer

# as ops user
USER ops

# create work directory
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && pip install elasticsearch \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data \
 && sudo chown -R ops:ops /home/ops/verdi/ops/multi_acquisition_localizer


WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
