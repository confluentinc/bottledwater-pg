SOURCES=test.c
EXECUTABLE=test

CFLAGS=-c -Wall -I`pg_config --includedir` `pkg-config --cflags avro-c`
LDFLAGS=-L`pg_config --libdir` -lpq `pkg-config --libs avro-c`
CC=gcc
OBJECTS=$(SOURCES:.c=.o)

all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@
