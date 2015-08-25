/*
// DaoSQL
// Database handling with mapping class instances to database table records.
// Copyright (C) 2008-2015, Limin Fu (http://fulimin.org).
*/
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"

#include"mysql.h"

#include"daoSQL.h"

typedef struct DaoMySQLDB DaoMySQLDB;
typedef struct DaoMySQLHD DaoMySQLHD;

struct DaoMySQLDB
{
	DaoSQLDatabase base;

	MYSQL       *mysql;
	MYSQL_STMT  *stmt;
	DMap        *stmts;
	DString     *error;
};

DaoMySQLDB* DaoMySQLDB_New();

struct DaoMySQLHD
{
	DaoSQLHandle  base;
	DaoMySQLDB   *model;

	MYSQL_STMT  *stmt;
	MYSQL_RES   *res;
	MYSQL_BIND   parbind[ MAX_PARAM_COUNT ];
	MYSQL_BIND   resbind[ MAX_PARAM_COUNT ];
};
