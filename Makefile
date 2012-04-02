
CC = gcc
INCLUDE = -I. -I/usr/local/dao/include
LIB = -L. -L/usr/lib
CFLAG = -c -fPIC
LFLAG =

TARGET = libDaoSQL.so

UNAME = $(shell uname)

ifeq ($(UNAME), Linux)
  CFLAG += -DUNIX
  LFLAG += -fPIC -shared -Wl,-soname,libDaoSQL.so
endif

ifeq ($(UNAME), Darwin)
  TARGET	= libDaoSQL.dylib
  CFLAG += -DUNIX -DMAC_OSX
  LFLAG += -fPIC -dynamiclib -undefined dynamic_lookup -install_name libDaoSQL.dylib
endif

all: $(TARGET) backends

daoSQL.o: daoSQL.c daoSQL.h
	$(CC) $(CFLAG) $(INCLUDE) daoSQL.c

$(TARGET): daoSQL.o
	$(CC) $(LFLAG) $(LIB) daoSQL.o -o $(TARGET)

backends:
	cd DaoSQLite && $(MAKE)
	cd DaoMySQL && $(MAKE)

bind: clean

clean:
	rm *.o
	cd DaoSQLite && $(MAKE) clean
	cd DaoMySQL && $(MAKE) clean

.PHONY: backends clean
