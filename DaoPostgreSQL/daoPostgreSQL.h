/*
// DaoSQL
// Database handling with mapping class instances to database table records.
// Copyright (C) 2013-2015, Limin Fu (http://fulimin.org).
*/

#include<stdint.h>
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"
#include"daoSQL.h"
#include"libpq-fe.h"

typedef struct DaoPostgreSQLDB DaoPostgreSQLDB;
typedef struct DaoPostgreSQLHD DaoPostgreSQLHD;

struct DaoPostgreSQLDB
{
	DaoSQLDatabase base;

	PGconn  *conn;
	DMap    *stmts;
	daoint   rows;
};

DaoPostgreSQLDB* DaoPostgreSQLDB_New();

struct DaoPostgreSQLHD
{
	DaoSQLHandle  base;
	DaoPostgreSQLDB  *model;

	DString     *name;
	PGresult    *res;
	Oid          paramTypes[ MAX_PARAM_COUNT ];
	int          paramFormats[ MAX_PARAM_COUNT ];
	int          paramLengths[ MAX_PARAM_COUNT ];
	const char  *paramValues[ MAX_PARAM_COUNT ];
	uint32_t     paramInts32[ MAX_PARAM_COUNT ];
	uint64_t     paramInts64[ MAX_PARAM_COUNT ];
};
