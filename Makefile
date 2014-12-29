PROGRAM = test

AVRO_CFLAGS = $(shell pkg-config --cflags avro-c)
AVRO_LDFLAGS = $(shell pkg-config --libs avro-c)
PQ_LDFLAGS = -lpq

PG_CPPFLAGS += $(AVRO_CFLAGS)
PG_LIBS += $(AVRO_LDFLAGS) $(PQ_LDFLAGS)

OBJS = test.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
