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

docker: docker-client docker-postgres

docker-compose: docker
	docker-compose build

tmp:
	mkdir tmp

tmp/%.tar.gz: tmp docker-build
	docker run --rm bwbuild:$(DOCKER_TAG) cat /$*.tar.gz > $@

tmp/%: build/% tmp
	cp $< $@

docker-build:
	docker build -f build/Dockerfile.build -t bwbuild:$(DOCKER_TAG) .

docker-client: tmp/Dockerfile.client tmp/avro-1.8.0.tar.gz tmp/librdkafka-0.9.0.tar.gz tmp/bottledwater-bin.tar.gz tmp/bottledwater-docker-wrapper.sh
	docker build -f $< -t local-bottledwater:$(DOCKER_TAG) tmp

docker-postgres: tmp/Dockerfile.postgres tmp/bottledwater-ext.tar.gz tmp/avro-1.8.0.tar.gz tmp/replication-config.sh
	docker build -f $< -t local-postgres-bw:$(DOCKER_TAG) tmp
