/*
// DaoSQL
// Database handling with mapping class instances to database table records.
// Copyright (C) 2008-2015, Limin Fu (http://fulimin.org).
*/

#ifndef __DAO_SQL__
#define __DAO_SQL__

#include<stdint.h>
#include"dao.h"
#include"daoStdtype.h"
#include"daoType.h"
#include"daoClass.h"
#include"daoObject.h"
#include"daoValue.h"
#include"daoNamespace.h"

#define DAO_HAS_TIME
#define DAO_HAS_CRYPTO
#define DAO_HAS_DECIMAL
#include"dao_api.h"

#define MAX_PARAM_COUNT 64
#define MIN_DATA_SIZE 64
#define MAX_DATA_SIZE 1024

typedef struct DaoSQLDatabase DaoSQLDatabase;
typedef struct DaoSQLHandle   DaoSQLHandle;

enum{ DAO_SQLITE, DAO_MYSQL, DAO_POSTGRESQL };

struct DaoSQLDatabase
{
	DAO_CSTRUCT_COMMON;

	int       etype;
	DString  *name;
	DString  *host;
	DString  *user;
	DString  *password;
	DMap     *dataClass; /* DMap<DaoClass*,int> */
};
void DaoSQLDatabase_Init( DaoSQLDatabase *self, DaoType *type, int etype );
void DaoSQLDatabase_Clear( DaoSQLDatabase *self );

struct DaoSQLHandle
{
	DAO_CSTRUCT_COMMON;

	DaoSQLDatabase  *database;

	DList    *classList;
	DList    *countList;
	DString  *sqlSource;
	DString  *buffer;
	int  paramCount;
	int  boolCount;
	int  setCount;
	int  prepared;
	int  executed;
	int  stopQuery;
	unsigned long  reslen;

	DaoType  *partypes[ MAX_PARAM_COUNT ];
	DString  *pardata[ MAX_PARAM_COUNT ];
	DString  *resdata[ MAX_PARAM_COUNT ];
};
void DaoSQLHandle_Init( DaoSQLHandle *self, DaoType *type, DaoSQLDatabase *db );
void DaoSQLHandle_Clear( DaoSQLHandle *self );

extern DaoTypeCore daoSQLDatabaseCore;
extern DaoTypeCore daoSQLHandleCore;

void DaoSQLDatabase_CreateTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql );
void DaoSQLDatabase_DeleteTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql );
int DaoSQLHandle_PrepareInsert( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareDelete( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareSelect( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );
int DaoSQLHandle_PrepareUpdate( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N );


void DString_AppendSQL( DString *self, DString *content, int escape, const char *quote );
DString* DaoSQLDatabase_TableName( DaoClass *klass );

int32_t DaoSQL_EncodeDate( DaoTuple *tuple );
int64_t DaoSQL_EncodeTimestamp( DaoTuple *tuple );
void DaoSQL_DecodeDate( DaoTuple *tuple, int32_t date_days );
void DaoSQL_DecodeTimestamp( DaoTuple *tuple, int64_t time_msec );

extern DaoType *dao_sql_type_bigint;
extern DaoType *dao_sql_type_integer_primary_key;
extern DaoType *dao_sql_type_integer_primary_key_auto_increment;
extern DaoType *dao_sql_type_real;
extern DaoType *dao_sql_type_float;
extern DaoType *dao_sql_type_double;
extern DaoType *dao_sql_type_decimal;
extern DaoType *dao_sql_type_date;
extern DaoType *dao_sql_type_timestamp;
extern DaoType *dao_type_datetime;

#endif
