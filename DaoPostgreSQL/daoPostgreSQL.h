/*
// DaoPostgreSQL:
// Database handling with mapping class instances to database table records.
// Copyright (C) 2013, Limin Fu (daokoder@gmail.com).
*/

#include<stdint.h>
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"

#include"mysql.h"
#include"libpq-fe.h"

#include"daoSQL.h"

typedef struct DaoPostgreSQLDB DaoPostgreSQLDB;
typedef struct DaoPostgreSQLHD DaoPostgreSQLHD;

struct DaoPostgreSQLDB
{
	DaoSQLDatabase base;

	PGconn  *conn;
};

DaoPostgreSQLDB* DaoPostgreSQLDB_New();

struct DaoPostgreSQLHD
{
	DaoSQLHandle  base;
	DaoPostgreSQLDB  *model;

	MYSQL_STMT  *stmt;
	DString     *name;
	PGresult    *res;
	Oid          paramTypes[ MAX_PARAM_COUNT ];
	int          paramFormats[ MAX_PARAM_COUNT ];
	int          paramLengths[ MAX_PARAM_COUNT ];
	const char  *paramValues[ MAX_PARAM_COUNT ];
	uint32_t     paramInts32[ MAX_PARAM_COUNT ];
	uint64_t     paramInts64[ MAX_PARAM_COUNT ];
	MYSQL_BIND   parbind[ MAX_PARAM_COUNT ];
	MYSQL_BIND   resbind[ MAX_PARAM_COUNT ];
};
