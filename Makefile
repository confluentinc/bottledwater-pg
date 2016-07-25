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

test-bundle: Gemfile.lock
	bundle install

spec/functional/type_specs.rb: spec/bin/generate_type_specs.rb test-bundle docker-compose
	bundle exec ruby -Ispec $< >$@

test: spec/functional/type_specs.rb
	bundle exec rspec --order random

docker: docker-client docker-postgres docker-postgres94

docker-compose: docker
	docker-compose build

tmp:
	mkdir tmp

tmp/%-94.tar.gz: tmp docker-build-94
	docker run --rm bwbuild-94:$(DOCKER_TAG) cat /$*-94.tar.gz > $@

tmp/%.tar.gz: tmp docker-build
	docker run --rm bwbuild:$(DOCKER_TAG) cat /$*.tar.gz > $@

tmp/%: build/% tmp
	cp $< $@

docker-build-94:
	docker build -f build/Dockerfile.build94 -t bwbuild-94:$(DOCKER_TAG) .

docker-build:
	docker build -f build/Dockerfile.build -t bwbuild:$(DOCKER_TAG) .

docker-client: tmp/Dockerfile.client tmp/avro.tar.gz tmp/librdkafka.tar.gz tmp/bottledwater-bin.tar.gz tmp/bottledwater-docker-wrapper.sh
	docker build -f $< -t local-bottledwater:$(DOCKER_TAG) tmp

docker-postgres: tmp/Dockerfile.postgres tmp/bottledwater-ext.tar.gz tmp/avro.tar.gz tmp/replication-config.sh
	docker build -f $< -t local-postgres-bw:$(DOCKER_TAG) tmp

docker-postgres94: tmp/Dockerfile.postgres94 tmp/bottledwater-ext-94.tar.gz tmp/avro.tar.gz tmp/replication-config.sh
	docker build -f $< -t local-postgres94-bw:$(DOCKER_TAG) tmp
