EXTENSION = crabka_compute
MODULE_big = crabka_compute
OBJS = crabka.o
DATA = sql/crabka_compute--0.1.0.sql
PGFILEDESC = "crabka_compute - Crabka pageserver smgr hook"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_CPPFLAGS += -I../include
SHLIB_LINK += -L../../target/release -lcrabka_compute_client

include $(PGXS)
