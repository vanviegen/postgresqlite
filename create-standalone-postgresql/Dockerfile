FROM archlinux:latest
RUN pacman -Sy --noconfirm postgresql
RUN mkdir -p /target/bin /target/lib /target/share /target/slib
RUN cp /usr/sbin/postgres /target/bin/postgres.real
RUN cp /usr/sbin/initdb /target/bin/initdb.real
COPY run-with-libs /target/bin/postgres
COPY run-with-libs /target/bin/initdb
RUN cp $(ldd /usr/sbin/postgres | cut -c 2- | cut -f 3 -d " ") /target/slib
RUN cp -n $(ldd /usr/sbin/initdb | cut -c 2- | cut -f 3 -d " ") /target/slib
RUN cp -r /usr/lib/postgresql /target/lib
RUN cp -r /usr/share/postgresql /target/share
RUN tar czf /standalone-postgresql-$(postgres --version | cut -d ' ' -f 3).tar.gz -C target .
RUN rm -rf /target

