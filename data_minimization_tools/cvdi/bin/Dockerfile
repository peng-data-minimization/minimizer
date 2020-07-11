FROM ubuntu:focal AS builder
ENV DEBIAN_FRONTEND noninteractive
RUN	apt-get update -y && \
	apt-get install git npm apt-transport-https ca-certificates gnupg software-properties-common wget -y && \
	wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null && \
	apt-add-repository 'deb https://apt.kitware.com/ubuntu/ focal main' && \
	apt-get install cmake -y && \
	npm i -g cmake-js && \
	git clone https://github.com/usdot-its-jpo-data-portal/privacy-protection-application.git src && \
	cd src && \
	npm i electron nan
RUN cmake-js -d /src -O /release -a x64 -r electron -v 1.7.12
	
FROM ubuntu:focal
VOLUME /conf
VOLUME /out
COPY --from=builder /release/cl-tool/cv_di /app/
RUN echo "/conf/THE_FILE.csv" > /app/data_file
CMD ["/app/cv_di", "-n", "-c",  "/conf/config", "-q", "/conf/quad", "-o",  "/out", "-k", "/out", "/app/data_file"]
