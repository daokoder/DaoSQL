
/* DaoDataModel:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
 */
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"

#include<sqlite3.h>

#include"daoSQL.h"

typedef struct DaoDataModel DaoDataModel;
typedef struct DaoHandle DaoHandle;

struct DaoDataModel
{
	DaoSQLDatabase base;

	sqlite3      *db;
	sqlite3_stmt *stmt;
};

DaoDataModel* DaoDataModel_New();

struct DaoHandle
{
	DaoSQLHandle  base;
	DaoDataModel  *model;

	sqlite3_stmt *stmt;
};
