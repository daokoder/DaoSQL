
CC = gcc
INCLUDE = -I. -I.. -I/usr/local/dao/include
LIB = -L. -L.. -lDaoSQL -L/usr/lib -lsqlite3
CFLAG = -c -Wall -O2 -fPIC
LFLAG =
TARGET = ../DaoSQLite.so

UNAME := $(shell uname)

ifeq ($(UNAME), Linux)
  CFLAG += -DUNIX
  LFLAGLIB = -s -fPIC -shared
endif
ifeq ($(UNAME), Darwin)
  TARGET = ../DaoSQLite.dylib
  CFLAG += -DUNIX -DMAC_OSX
  LFLAGLIB = -s -fPIC -dynamiclib -undefined dynamic_lookup
endif

all: $(TARGET)

daoSQLite.o: daoSQLite.c
	$(CC) $(CFLAG) $(INCLUDE) daoSQLite.c
	
$(TARGET): daoSQLite.o
	$(CC) $(LFLAGLIB) $(LIB) daoSQLite.o -o $(TARGET)

bind: clean

clean:
	rm *.o
