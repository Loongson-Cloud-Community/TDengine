FROM lcr.loongnix.cn/library/debian:sid

WORKDIR /root

ARG pkgFile
ARG dirName
RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/
RUN tar -zxf ${pkgFile}
WORKDIR /root/
RUN cd /root/${dirName}/ && /bin/bash install.sh -e no && cd /root
RUN rm /root/${pkgFile}
RUN rm -rf /root/${dirName}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean && apt-get update && apt-get install -y locales tzdata netcat-traditional && rm -rf /var/lib/apt/lists && locale-gen en_US.UTF-8
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

COPY ./bin/* /usr/bin/

ENV TINI_VERSION v0.19.0
COPY tini-static /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile" ]
