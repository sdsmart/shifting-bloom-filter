#-------------------------------------------------------------------------
#
# Makefile for cms
#
#-------------------------------------------------------------------------

MODULE_big = shbf
OBJS =		\
			shbf.o \
			MurmurHash3.o \
			$(NULL)

EXTENSION = shbf
DATA =		\
			shbf.sql \
			$(NULL)

PG_CPPFLAGS += -fPIC
shbf.o: override CFLAGS += -std=c99
MurmurHash3.o: override CFLAGS += -std=c99

SHLIB_LINK	+= -lstdc++

ifndef PG_CONFIG
PG_CONFIG = pg_config
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
