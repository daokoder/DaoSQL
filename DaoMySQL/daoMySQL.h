
/* DaoDataModel:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2011, Limin Fu (phoolimin@gmail.com).
 */
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoNamespace.h"

#include"mysql.h"

#include"daoSQL.h"

typedef struct DaoDataModel DaoDataModel;
typedef struct DaoHandle DaoHandle;

struct DaoDataModel
{
	DaoSQLDatabase base;

	MYSQL  *mysql;
	MYSQL_STMT  *stmt;
};

DaoDataModel* DaoDataModel_New();

struct DaoHandle
{
	DaoSQLHandle  base;
	DaoDataModel  *model;

	MYSQL_STMT  *stmt;
	MYSQL_RES   *res;
	MYSQL_BIND   parbind[ MAX_PARAM_COUNT ];
	MYSQL_BIND   resbind[ MAX_PARAM_COUNT ];
};
