SOURCES=pg_cdc.c replication.c protocol.c protocol_client.c
EXECUTABLE=pg_cdc

WARNINGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security
CFLAGS=-c -I`pg_config --includedir` `pkg-config --cflags avro-c` $(WARNINGS)
LDFLAGS=-L`pg_config --libdir` -lpq `pkg-config --libs avro-c`
CC=gcc
OBJECTS=$(SOURCES:.c=.o)

.PHONY: all clean

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJECTS) $(EXECUTABLE)
