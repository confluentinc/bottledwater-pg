DOCKER_TAG = dev

.PHONY: all install clean

all:
	$(MAKE) -C ext all
	$(MAKE) -C client all
	$(MAKE) -C kafka all

install:
	$(MAKE) -C ext install

clean:
	$(MAKE) -C ext clean
	$(MAKE) -C client clean
	$(MAKE) -C kafka clean

docker: avro-1.7.7.tar.gz librdkafka-0.9.0.tar.gz bottledwater-bin.tar.gz bottledwater-ext.tar.gz
	docker build -f build/Dockerfile.postgres -t local-postgres-bw:$(DOCKER_TAG) .
	docker build -f build/Dockerfile.client -t local-bottledwater:$(DOCKER_TAG) .

*.tar.gz: docker-build
	docker run --rm bwbuild:$(DOCKER_TAG) cat /$@ > $@

docker-build:
	docker build -f build/Dockerfile.build -t bwbuild:$(DOCKER_TAG) .
