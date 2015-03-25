SOURCES=replication.c protocol.c protocol_client.c connect.c
EXEC_SRC=bottledwater.c
EXECUTABLE=bottledwater
STATICLIB=libbottledwater.a

WARNINGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Wendif-labels -Wmissing-format-attribute -Wformat-security
CFLAGS=-c -I`pg_config --includedir` `pkg-config --cflags avro-c` $(WARNINGS)
LDFLAGS=-L`pg_config --libdir` -lpq `pkg-config --libs avro-c`
CC=gcc
AR=ar
OBJECTS=$(SOURCES:.c=.o)
EXEC_OBJ=$(EXEC_SRC:.c=.o)

.PHONY: all clean

all: $(SOURCES) $(EXECUTABLE) $(STATICLIB)

$(EXECUTABLE): $(OBJECTS) $(EXEC_OBJ)
	$(CC) $(LDFLAGS) $^ -o $@

$(STATICLIB): $(OBJECTS)
	$(AR) rcs $@ $^

.c.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(OBJECTS) $(EXEC_OBJ) $(EXECUTABLE) $(STATICLIB)
