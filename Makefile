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

docker: tmp/avro-1.7.7.tar.gz tmp/librdkafka-0.9.0.tar.gz tmp/bottledwater-bin.tar.gz tmp/bottledwater-ext.tar.gz
	docker build -f build/Dockerfile.postgres -t local-postgres-bw:$(DOCKER_TAG) .
	docker build -f build/Dockerfile.client -t local-bottledwater:$(DOCKER_TAG) .

tmp:
	mkdir tmp

tmp/%.tar.gz: tmp docker-build
	docker run --rm bwbuild:$(DOCKER_TAG) cat /$*.tar.gz > $@

docker-build:
	docker build -f build/Dockerfile.build -t bwbuild:$(DOCKER_TAG) .
