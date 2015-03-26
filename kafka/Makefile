SOURCES=bottledwater.c registry.c
EXECUTABLE=bottledwater
STATICLIB=../client/libbottledwater.a

PG_CFLAGS = -I$(shell pg_config --includedir)
PG_LDFLAGS = -L$(shell pg_config --libdir) -lpq
KAFKA_CFLAGS =
KAFKA_LDFLAGS = -lrdkafka -lz -lpthread # TODO -lrt needed on Linux for clock_gettime, but not on OSX?
AVRO_CFLAGS = $(shell pkg-config --cflags avro-c)
AVRO_LDFLAGS = $(shell pkg-config --libs avro-c)
CURL_CFLAGS = $(shell curl-config --cflags)
CURL_LDFLAGS = $(shell curl-config --libs)
JSON_CFLAGS = $(shell pkg-config --cflags jansson)
JSON_LDFLAGS = $(shell pkg-config --libs jansson)

WARNINGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security
CFLAGS=-c -g -I../client $(PG_CFLAGS) $(KAFKA_CFLAGS) $(AVRO_CFLAGS) $(CURL_CFLAGS) $(JSON_CFLAGS) $(WARNINGS)
LDFLAGS= $(PG_LDFLAGS) $(KAFKA_LDFLAGS) $(AVRO_LDFLAGS) $(CURL_LDFLAGS) $(JSON_LDFLAGS)
CC=gcc
OBJECTS=$(SOURCES:.c=.o)

.PHONY: all clean

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $^ $(STATICLIB) -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJECTS) $(EXECUTABLE)