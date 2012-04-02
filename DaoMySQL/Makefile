
CC = gcc
INCLUDE = -I. -I.. -I/usr/local/dao/include
LIB = -L. -L.. -lDaoSQL -L/usr/lib
CFLAG = -c -fPIC
LFLAG =

INCLUDE += -I/Users/fulimin/Downloads/mysql-connector-c-6.0.2-osx10.5-x86-64bit/include
#LIB += -L/Users/fulimin/Downloads/mysql-connector-c-6.0.2-osx10.5-x86-64bit/lib

TARGET = DaoMySQL.so

UNAME = $(shell uname)

ifeq ($(UNAME), Linux)
  CFLAG += -DUNIX
  INCLUDE += -I/usr/include/mysql
  LFLAG += -fPIC -shared -Wl,-soname,libDaoMySQL.so
  LIB += -L/usr/lib/mysql -lmysqlclient
endif

ifeq ($(UNAME), Darwin)
  TARGET	= DaoMySQL.dylib
  INCLUDE += -I/usr/local/mysql/include
  CFLAG += -DUNIX -DMAC_OSX
  LFLAG += -fPIC -dynamiclib -undefined dynamic_lookup -install_name libDaoMySQL.dylib
  LIB += -L/usr/local/mysql/lib -lmysqlclient
endif

all: $(TARGET)

daoMySQL.o: daoMySQL.c daoMySQL.h
	$(CC) $(CFLAG) $(INCLUDE) daoMySQL.c

$(TARGET): daoMySQL.o
	$(CC) $(LFLAG) $(LIB) daoMySQL.o -o $(TARGET)

bind: clean

clean:
	rm *.o
