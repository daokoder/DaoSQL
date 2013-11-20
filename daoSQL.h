/* DaoSQL:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
 */

#ifndef __DAO_SQL__
#define __DAO_SQL__

#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoValue.h"
#include"daoNamespace.h"

#define MAX_PARAM_COUNT 64
#define MIN_DATA_SIZE 64
#define MAX_DATA_SIZE 1024

typedef struct DaoSQLDatabase DaoSQLDatabase;
typedef struct DaoSQLHandle  DaoSQLHandle;

enum{ DAO_SQLITE, DAO_MYSQL, DAO_POSTGRESQL };

struct DaoSQLDatabase
{
	int       type;
	DString  *name;
	DString  *host;
	DString  *user;
	DString  *password;
	DMap     *dataClass; /* DMap<DaoClass*,int> */
};
void DaoSQLDatabase_Init( DaoSQLDatabase *self, int type );
void DaoSQLDatabase_Clear( DaoSQLDatabase *self );

struct DaoSQLHandle
{
	DaoSQLDatabase  *database;

	DArray   *classList;
	DArray   *countList;
	DString  *sqlSource;
	DString  *buffer;
	int  boolCount;
	int  setCount;
	int  prepared;
	int  executed;
	unsigned long  reslen;

	DString  *pardata[ MAX_PARAM_COUNT ];
	DString  *resdata[ MAX_PARAM_COUNT ];
};
void DaoSQLHandle_Init( DaoSQLHandle *self, DaoSQLDatabase *db );
void DaoSQLHandle_Clear( DaoSQLHandle *self );

extern DaoTypeBase DaoSQLDatabase_Typer;
extern DaoTypeBase DaoSQLHandle_Typer;

void DaoSQLDatabase_CreateTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql );
int DaoSQLHandle_PrepareInsert( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareDelete( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareSelect( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareUpdate( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );

#endif
