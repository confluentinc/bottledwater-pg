SOURCES=test.c
EXECUTABLE=test

CFLAGS=-c -Wall -I`pg_config --includedir`
LDFLAGS=-L`pg_config --libdir` -lpq
CC=gcc
OBJECTS=$(SOURCES:.c=.o)

all: $(SOURCES) $(EXECUTABLE)
	
$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.c.o:
	$(CC) $(CFLAGS) $< -o $@
