SOURCES=bottledwater.c
EXECUTABLE=bottledwater
STATICLIB=../client/libbottledwater.a

WARNINGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security
CFLAGS=-c -g -I../client -I`pg_config --includedir` `pkg-config --cflags avro-c` $(WARNINGS)
# TODO -lrt needed on Linux for clock_gettime, but not on OSX?
LDFLAGS=-lrdkafka -lz -lpthread -L`pg_config --libdir` -lpq `pkg-config --libs avro-c`
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
