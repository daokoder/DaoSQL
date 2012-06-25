
/* DaoSQLiteDB:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
 */
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"

#include<sqlite3.h>

#include"daoSQL.h"

typedef struct DaoSQLiteDB DaoSQLiteDB;
typedef struct DaoSQLiteHD DaoSQLiteHD;

struct DaoSQLiteDB
{
	DaoSQLDatabase base;

	sqlite3      *db;
	sqlite3_stmt *stmt;
};

DaoSQLiteDB* DaoSQLiteDB_New();

struct DaoSQLiteHD
{
	DaoSQLHandle  base;
	DaoSQLiteDB  *model;

	sqlite3_stmt *stmt;
};
