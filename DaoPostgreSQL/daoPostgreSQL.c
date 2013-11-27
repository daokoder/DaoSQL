/*
// DaoPostgreSQL:
// Database handling with mapping class instances to database table records.
// Copyright (C) 2013, Limin Fu (daokoder@gmail.com).
*/

#include"stdlib.h"
#include"string.h"
#include"daoPostgreSQL.h"

#ifdef MAC_OSX

  #include <libkern/OSByteOrder.h>
 
  #define htobe16(x) OSSwapHostToBigInt16(x)
  #define htole16(x) OSSwapHostToLittleInt16(x)
  #define be16toh(x) OSSwapBigToHostInt16(x)
  #define le16toh(x) OSSwapLittleToHostInt16(x)
 
  #define htobe32(x) OSSwapHostToBigInt32(x)
  #define htole32(x) OSSwapHostToLittleInt32(x)
  #define be32toh(x) OSSwapBigToHostInt32(x)
  #define le32toh(x) OSSwapLittleToHostInt32(x)
 
  #define htobe64(x) OSSwapHostToBigInt64(x)
  #define htole64(x) OSSwapHostToLittleInt64(x)
  #define be64toh(x) OSSwapBigToHostInt64(x)
  #define le64toh(x) OSSwapLittleToHostInt64(x)
 
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
#elif defined(__BSD__)
  #include <sys/types.h>
  #include <sys/endian.h>
#else
  #include <endian.h>
#endif

DaoPostgreSQLDB* DaoPostgreSQLDB_New()
{
	DaoPostgreSQLDB *self = malloc( sizeof(DaoPostgreSQLDB) );
	DaoSQLDatabase_Init( (DaoSQLDatabase*) self, DAO_POSTGRESQL );
	self->conn = NULL;
	return self;
}
void DaoPostgreSQLDB_Delete( DaoPostgreSQLDB *self )
{
	DaoSQLDatabase_Clear( (DaoSQLDatabase*) self );
	if( self->conn ) PQfinish( self->conn );
	free( self );
}

static void DaoPostgreSQLDB_DataModel( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLDB_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_Sort( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoPostgreSQLDB_DataModel,"SQLDatabase<PostgreSQL>( name : string, host='', user='', pwd='' )=>SQLDatabase<PostgreSQL>"},
	{ DaoPostgreSQLDB_CreateTable,  "CreateTable( self:SQLDatabase<PostgreSQL>, klass )" },
//	{ DaoPostgreSQLDB_AlterTable,  "AlterTable( self:SQLDatabase<PostgreSQL>, klass )" },
	{ DaoPostgreSQLDB_Insert,  "Insert( self:SQLDatabase<PostgreSQL>, object )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLDB_DeleteRow, "Delete( self:SQLDatabase<PostgreSQL>, object )=>SQLHandle<PostgreSQL>"},
	{ DaoPostgreSQLDB_Select, "Select( self:SQLDatabase<PostgreSQL>, object,...)=>SQLHandle<PostgreSQL>"},
	{ DaoPostgreSQLDB_Update, "Update( self:SQLDatabase<PostgreSQL>, object,...)=>SQLHandle<PostgreSQL>"},
	{ DaoPostgreSQLDB_Query,  "Query( self:SQLDatabase<PostgreSQL>, sql : string ) => int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoPostgreSQLDB_Typer = 
{ "SQLDatabase<PostgreSQL>", NULL, NULL, modelMeths, 
	{ & DaoSQLDatabase_Typer, 0 }, {0}, 
	(FuncPtrDel) DaoPostgreSQLDB_Delete, NULL };

static DaoType *dao_type_postgresql_database = NULL;

DaoPostgreSQLHD* DaoPostgreSQLHD_New( DaoPostgreSQLDB *model )
{
	int i;
	char buf[100];
	DaoPostgreSQLHD *self = malloc( sizeof(DaoPostgreSQLHD) );
	DaoSQLHandle_Init( (DaoSQLHandle*) self, (DaoSQLDatabase*) model );
	self->model = model;
	self->res = NULL;
	self->name = DString_New(1);
	sprintf( buf, "PQ_STMT_%p", self );
	DString_SetMBS( self->name, buf );
	return self;
}
void DaoPostgreSQLHD_Delete( DaoPostgreSQLHD *self )
{
	int i;
	/* it appears to close all other stmt associated with the connection too! */
	/* mysql_stmt_close( self->stmt ); */
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		DString_Delete( self->base.pardata[i] );
		DString_Delete( self->base.resdata[i] );
	}
	DString_Delete( self->name );
	DString_Delete( self->base.sqlSource );
	DString_Delete( self->base.buffer );
	DArray_Delete( self->base.classList );
	DArray_Delete( self->base.countList );
	free( self );
}
static void DaoPostgreSQLHD_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_Bind( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N );

static void DaoPostgreSQLHD_HStoreSet( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_HStoreDelete( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_HStoreContain( DaoProcess *proc, DaoValue *p[], int N );

static void DaoPostgreSQLHD_Add( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_EQ( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_NE( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_GT( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_GE( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_LT( DaoProcess *proc, DaoValue *p[], int N );
static void DaoPostgreSQLHD_LE( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoPostgreSQLHD_Insert, "Insert( self :SQLHandle<PostgreSQL>, object ) => int" },
	{ DaoPostgreSQLHD_Bind, "Bind( self :SQLHandle<PostgreSQL>, value, index=0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_Query, "Query( self :SQLHandle<PostgreSQL>, ... ) [=>enum<continue,done>] => int" },
	{ DaoPostgreSQLHD_QueryOnce, "QueryOnce( self :SQLHandle<PostgreSQL>, ... ) => int" },

	{ DaoPostgreSQLHD_HStoreSet, "HstoreSet( self :SQLHandle<PostgreSQL>, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreSet, "HStoreSet( self :SQLHandle<PostgreSQL>, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, field :string, key :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, field :string, key :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_HStoreSet, "HstoreSet( self :SQLHandle<PostgreSQL>, table :class, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreSet, "HStoreSet( self :SQLHandle<PostgreSQL>, table :class, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, table :class, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, table :class, field :string, key :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreDelete, "HStoreDelete( self :SQLHandle<PostgreSQL>, table :class, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, table :class, field :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, table :class, field :string, key :string )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_HStoreContain, "HStoreContain( self :SQLHandle<PostgreSQL>, table :class, field :string, pairs :map<string,string> )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<int>, value :int = 0 )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<float>, value :float = 0 )=>SQLHandle<PostgreSQL>" },

	{ DaoPostgreSQLHD_Add, "Add( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_EQ, "EQ( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_NE, "NE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GT, "GT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_GE, "GE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LT, "LT( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_LE, "LE( self :SQLHandle<PostgreSQL>, table :class, field :string,  key :string, cast :type<double>, value :double = 0 )=>SQLHandle<PostgreSQL>" },

	// TODO: desc, use enum symbol!
	{ DaoPostgreSQLHD_Sort,  "Sort( self :@SQLHandle, field :string, key :string, desc=0 )=>@SQLHandle" },
	{ DaoPostgreSQLHD_Sort,  "Sort( self :@SQLHandle, field :string, key :string, cast :type<int>|type<float>|type<double>, desc=0 )=>@SQLHandle" },

	{ DaoPostgreSQLHD_Sort,  "Sort( self :@SQLHandle, table :class, field :string, key :string, desc=0 )=>@SQLHandle" },
	{ DaoPostgreSQLHD_Sort,  "Sort( self :@SQLHandle, table :class, field :string, key :string, cast :type<int>|type<float>|type<double>, desc=0 )=>@SQLHandle" },

	{ NULL, NULL }
};

static DaoTypeBase DaoPostgreSQLHD_Typer = 
{ "SQLHandle<PostgreSQL>", NULL, NULL, handleMeths, 
	{ & DaoSQLHandle_Typer, 0 }, {0},
	(FuncPtrDel) DaoPostgreSQLHD_Delete, NULL };

static DaoType *dao_type_postgresql_handle = NULL;

static void DaoPostgreSQLDB_DataModel( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = DaoPostgreSQLDB_New();
	DString_Assign( model->base.name, p[0]->xString.data );
	if( N >1 ) DString_Assign( model->base.host, p[1]->xString.data );
	if( N >2 ) DString_Assign( model->base.user, p[2]->xString.data );
	if( N >3 ) DString_Assign( model->base.password, p[3]->xString.data );
	DString_ToMBS( model->base.name );
	DString_ToMBS( model->base.host );
	DString_ToMBS( model->base.user );
	DString_ToMBS( model->base.password );
	DaoProcess_PutCdata( proc, model, dao_type_postgresql_database );
	// TODO: port!
	model->conn = PQsetdbLogin( model->base.host->mbs, NULL, NULL, NULL,
			model->base.name->mbs, model->base.user->mbs, model->base.password->mbs );
	if( PQstatus( model->conn ) != CONNECTION_OK ){
		DaoProcess_RaiseException( proc, DAO_ERROR, PQerrorMessage( model->conn ) );
		PQfinish( model->conn );
		model->conn = NULL;
		return;
	}
}
static void DaoPostgreSQLDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New(1);
	PGresult *res;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql );
	res = PQexec( model->conn, sql->mbs );
	if( PQresultStatus( res ) != PGRES_COMMAND_OK )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
	DString_Delete( sql );
	PQclear( res );
}
static void DaoPostgreSQLDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.data;
	PGresult *res;
	DString_ToMBS( sql );
	res = PQexec( model->conn, sql->mbs );
	if( PQresultStatus( res ) != PGRES_COMMAND_OK ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
	}
	DaoProcess_PutInteger( proc,PQresultStatus( res ) == PGRES_COMMAND_OK );
	PQclear( res );
}
static void DString_AppendKeyValues( DString *self, DaoMap *keyvalues )
{
	DNode *it;
	for(it=DaoMap_First(keyvalues); it; it=DaoMap_Next(keyvalues,it)){
		if( self->size ) DString_AppendChar( self, ',' );
		DString_AppendSQL( self, it->key.pValue->xString.data, 1, "\"" );
		DString_AppendMBS( self, "=>" );
		DString_AppendSQL( self, it->value.pValue->xString.data, 1, "\"" );
	}
}
static void DaoPostgreSQLHD_BindValue( DaoPostgreSQLHD *self, DaoValue *value, int index )
{
	DString *mbstring;

	self->paramLengths[index] = 0;
	self->paramFormats[index] = 0;
	switch( value->type ){
	case DAO_NONE :
		self->paramValues[index] = NULL;
		break;
	case DAO_INTEGER :
		self->paramFormats[index] = 1;
		self->paramLengths[index] = sizeof(uint32_t);
		self->paramValues[index] = (char*) & self->paramInts32[index];
		self->paramInts32[index] = htonl((uint32_t) value->xInteger.value );
		break;
	case DAO_FLOAT   :
		self->paramFormats[index] = 1;
		self->paramLengths[index] = sizeof(uint32_t);
		self->paramValues[index] = (char*) & self->paramInts32[index];
		self->paramInts32[index] = htobe32(*(uint32_t*) &value->xFloat.value );
		break;
	case DAO_DOUBLE  :
		self->paramFormats[index] = 1;
		self->paramLengths[index] = sizeof(uint64_t);
		self->paramValues[index] = (char*) & self->paramInts64[index];
		self->paramInts64[index] = htobe64(*(uint64_t*) &value->xDouble.value );
		break;
	case DAO_STRING  :
		mbstring = value->xString.data;
		DString_ToMBS( mbstring );
		DString_SetDataMBS( self->base.pardata[index], mbstring->mbs, mbstring->size );
		self->paramValues[index] = self->base.pardata[index]->mbs;
		self->paramLengths[index] = mbstring->size;
		break;
	case DAO_MAP :
		mbstring = self->base.pardata[index];
		DString_Reset( mbstring, 0 );
		DString_AppendKeyValues( mbstring, (DaoMap*) value );
		self->paramValues[index] = mbstring->mbs;
		self->paramLengths[index] = mbstring->size;
		break;
	default : break;
	}
}
static void DaoPostgreSQLDB_InsertObject( DaoProcess *proc, DaoPostgreSQLHD *handle, DaoObject *object )
{
	DaoPostgreSQLDB *model = handle->model;
	DaoClass *klass = object->defClass;
	DaoVariable **vars = klass->instvars->items.pVar;
	DString *mbstring;
	int i, k = -1;
	for(i=1,k=0; i<klass->objDataName->size; i++){
		char *tpname = vars[i]->dtype->name->mbs;
		DaoValue *value = object->objValues[i];
		if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ) continue;
		DaoPostgreSQLHD_BindValue( handle, value, k++ );
	}
	if( handle->res ) PQclear( handle->res );
	handle->res = PQexecPrepared( model->conn, handle->name->mbs, k, handle->paramValues,
			handle->paramLengths, handle->paramFormats, 1 );
	if( PQresultStatus( handle->res ) != PGRES_COMMAND_OK ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
	}

	//printf( "k = %i %i\n", k, mysql_insert_id( handle->model->conn ) );
	//if( k >=0 ) object->objValues[k]->xInteger.value = mysql_insert_id( handle->model->conn );
}
#define VOIDOID         2278
#define INT4OID         23
#define FLOAT4OID       700
#define FLOAT8OID       701
#define BYTEAOID        17
#define BPCHAROID       1042
#define VARCHAROID      1043
#define TEXTOID         25
static void DaoPostgreSQLHD_PrepareBindings( DaoPostgreSQLHD *self )
{
	int k;
	for(k=0; k<self->base.paramCount; ++k){
		DaoType *type = self->base.partypes[k];
		self->paramTypes[k] = 0;
		switch( type->tid ){
		case DAO_NONE :
			self->paramTypes[k] = VOIDOID; // ???
			break;
		case DAO_INTEGER :
			self->paramTypes[k] = INT4OID;
			break;
		case DAO_FLOAT :
			self->paramTypes[k] = FLOAT4OID;
			break;
		case DAO_DOUBLE :
			self->paramTypes[k] = FLOAT8OID;
			break;
		case DAO_STRING :
			self->paramTypes[k] = BYTEAOID; // ???
			if( strstr( type->name->mbs, "CHAR" ) == type->name->mbs ){
				self->paramTypes[k] = BPCHAROID;
			}else if( strstr( type->name->mbs, "VARCHAR" ) == type->name->mbs ){
				self->paramTypes[k] = VARCHAROID;
			}else if( strcmp( type->name->mbs, "TEXT" ) == 0 ){
				self->paramTypes[k] = TEXTOID;
			}
			break;
		case DAO_MAP :
			self->paramTypes[k] = TEXTOID;
			break;
		}
	}
}
static void DaoPostgreSQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoPostgreSQLHD *handle = DaoPostgreSQLHD_New( model );
	DaoObject *object = (DaoObject*) p[1];
	DaoClass *klass = object->defClass;
	DString *str = handle->base.sqlSource;
	int i;

	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_postgresql_handle, handle ) );
	if( DaoSQLHandle_PrepareInsert( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->mbs );
	if( p[1]->type == DAO_LIST ){
		/* Already checked by DaoSQLHandle_PrepareInsert(): */
		klass = p[1]->xList.items.items.pValue[0]->xObject.defClass;
	}
	DaoPostgreSQLHD_PrepareBindings( handle );
	handle->res = PQprepare( model->conn, handle->name->mbs, str->mbs, handle->base.paramCount, handle->paramTypes );
	if( PQresultStatus( handle->res ) != PGRES_COMMAND_OK ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
		return;
	}
	if( p[1]->type == DAO_LIST ){
		for( i=0; i<p[1]->xList.items.size; i++ )
			DaoPostgreSQLDB_InsertObject( proc, handle, p[1]->xList.items.items.pObject[i] );
	}else{
		DaoPostgreSQLDB_InsertObject( proc, handle, object );
	}
}
static void DaoPostgreSQLDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoPostgreSQLHD *handle = DaoPostgreSQLHD_New( model );

	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_postgresql_handle, handle ) );
	if( DaoSQLHandle_PrepareDelete( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoPostgreSQLDB_Select( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoPostgreSQLHD *handle = DaoPostgreSQLHD_New( model );

	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_postgresql_handle, handle ) );
	if( DaoSQLHandle_PrepareSelect( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->mbs );
}
static void DaoPostgreSQLDB_Update( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoPostgreSQLHD *handle = DaoPostgreSQLHD_New( model );

	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_postgresql_handle, handle ) );
	if( DaoSQLHandle_PrepareUpdate( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoPostgreSQLHD_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoValue *value;
	int i;
	DaoProcess_PutValue( proc, p[0] );
	if( p[1]->type == DAO_OBJECT ){
		DaoPostgreSQLDB_InsertObject( proc, handle, (DaoObject*) p[1] );
	}else if( p[1]->type == DAO_LIST ){
		for(i=0; i<p[1]->xList.items.size; i++){
			value = p[1]->xList.items.items.pValue[i];
			if( value->type != DAO_OBJECT 
					|| value->xObject.defClass != handle->base.classList->items.pClass[0] ){
				DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
				return;
			}
			DaoPostgreSQLDB_InsertObject( proc, handle, (DaoObject*) value );
		}
	}
}
static void DaoPostgreSQLHD_Bind( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoValue *value = p[1];
	int index = p[2]->xInteger.value;
	DaoProcess_PutValue( proc, p[0] );
	if( handle->base.prepared ==0 ){
		DaoPostgreSQLDB *db = handle->model;
		DString *sql = handle->base.sqlSource;
		if( handle->res ) PQclear( handle->res );
		handle->res = PQprepare( db->conn, handle->name->mbs, sql->mbs, handle->base.paramCount, handle->paramTypes );
		if( PQresultStatus( handle->res ) != PGRES_COMMAND_OK ){
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( db->conn ) );
			return;
		}
		handle->base.prepared = 1;
	}
	if( index >= MAX_PARAM_COUNT ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	DaoPostgreSQLHD_BindValue( handle, value, index );
}
static int DaoPostgreSQLHD_Execute( DaoProcess *proc, DaoValue *p[], int N, int status[2] )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoPostgreSQLDB *model = handle->model;
	int i, ret;

	//printf( "%s\n", handle->base.sqlSource->mbs );

	if( handle->base.prepared ==0 ){
		DaoPostgreSQLDB *db = handle->model;
		DString *sql = handle->base.sqlSource;
		if( handle->res ) PQclear( handle->res );
		handle->res = PQprepare( db->conn, handle->name->mbs, sql->mbs, handle->base.paramCount, handle->paramTypes );
		if( PQresultStatus( handle->res ) != PGRES_COMMAND_OK ){
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( db->conn ) );
			return 0;
		}
		handle->base.prepared = 1;
	}
	if( handle->res ) PQclear( handle->res );
	handle->res = PQexecPrepared( model->conn, handle->name->mbs, handle->base.paramCount,
			handle->paramValues, handle->paramLengths, handle->paramFormats, 1 );
	ret = PQresultStatus( handle->res );
	if( ret != status[0] && ret != status[1] ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
		return 0;
	}
	for(i=1; i<N; i++){
		if( p[i]->type == DAO_OBJECT ){
			if( p[i]->xObject.defClass == handle->base.classList->items.pClass[i-1] ) continue;
		}
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "need class instance(s)" );
		return 0;
	}
	return ret;
}
static void DaoPostgreSQLHD_Retrieve( DaoProcess *proc, DaoValue *p[], int N, daoint row )
{
	char *pdata;
	uint32_t ivalue32;
	uint64_t ivalue64;
	daoint i, j, k, m, len;
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoPostgreSQLDB *model = handle->model;
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	DaoMap *keyvalues;
	DNode *it;

	for(i=1,k=0; i<N; i++){
		object = (DaoObject*) p[i];
		klass = object->defClass;
		for(j=1, m = handle->base.countList->items.pInt[i-1]; j<m; j++){
			type = klass->instvars->items.pVar[j]->dtype;
			value = object->objValues[j];
			pdata = PQgetvalue( handle->res, row, k++ );
			if( pdata == NULL ) continue;
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
			case DAO_INTEGER :
				value->xInteger.value = ntohl(*((uint32_t *) pdata));
				break;
			case DAO_FLOAT   :
				ivalue32 = be32toh( *(uint32_t*) pdata );
				value->xFloat.value = *(float*) & ivalue32;
				break;
			case DAO_DOUBLE  :
				ivalue64 = be64toh( *(uint64_t*) pdata );
				value->xDouble.value = *(double*) & ivalue64;
				break;
			case DAO_STRING  :
				len = PQgetlength( handle->res, row, k-1 );
				DString_SetDataMBS( value->xString.data, pdata, len );
				break;
			case DAO_MAP :
				k --;
				keyvalues = (DaoMap*) value;
				for(it=DaoMap_First(keyvalues); it; it=DaoMap_Next(keyvalues,it)){
					pdata = PQgetvalue( handle->res, row, k++ );
					if( pdata == NULL ) continue;
					len = PQgetlength( handle->res, row, k-1 );
					DString_SetDataMBS( it->value.pValue->xString.data, pdata, len );
				}
				break;
			default : break;
			}
		}
	}
}

static int status_ok[2] = { PGRES_COMMAND_OK, PGRES_TUPLES_OK };

static void DaoPostgreSQLHD_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	daoint i, j, k, m, entry, row;
	daoint *count = DaoProcess_PutInteger( proc, 0 );
	DaoVmCode *sect = DaoGetSectionCode( proc->activeCode );
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoPostgreSQLDB *model = handle->model;
	DaoObject *object;
	DaoClass  *klass;
	DaoValue *value;

	if( DaoPostgreSQLHD_Execute( proc, p, N, status_ok ) != PGRES_TUPLES_OK ) return;

	if( sect == NULL ) return;
	if( DaoProcess_PushSectionFrame( proc ) == NULL ) return;
	entry = proc->topFrame->entry;
	DaoProcess_AcquireCV( proc );
	for(row=0; row < PQntuples( handle->res ); ++row){

		DaoPostgreSQLHD_Retrieve( proc, p, N, row );

		proc->topFrame->entry = entry;
		DaoProcess_Execute( proc );
		if( proc->status == DAO_PROCESS_ABORTED ) break;
		*count += 1;
		value = proc->stackValues[0];
		if( value == NULL || value->type != DAO_ENUM || value->xEnum.value != 0 ) break;
	}
	DaoProcess_ReleaseCV( proc );
	DaoProcess_PopFrame( proc );
}
static void DaoPostgreSQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	if( DaoPostgreSQLHD_Execute( proc, p, N, status_ok ) != PGRES_TUPLES_OK ) return;
	if( PQntuples( handle->res ) ) DaoPostgreSQLHD_Retrieve( proc, p, N, 0 );
}


static void DaoPostgreSQLHD_HStore( DaoProcess *proc, DaoValue *p[], int N, const char *op, int update )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *fname, *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *value = p[2];
	DaoClass *klass;
	DaoType **type2;
	DaoType *type;
	int status = 0;

	DaoProcess_PutValue( proc, p[0] );
	if( handle->classList->size == 0 ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	fname = DString_New(1);
	klass = handle->classList->items.pClass[0];
	if( handle->setCount ) DString_AppendMBS( handle->sqlSource, ", " );
	DString_Assign( fname, field->xString.data );

	type2 = DaoClass_GetDataType( klass, fname, & status, NULL );
	type = type2 ? *type2 : NULL;

	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		klass = (DaoClass*) p[1];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Assign( fname, tabname );
		DString_AppendMBS( fname, "." );
		DString_Append( fname, field->xString.data );
	}
	DString_Append( handle->sqlSource, fname );
	if( update ){
		DString_AppendMBS( handle->sqlSource, "=" );
		DString_Append( handle->sqlSource, fname );
	}
	DString_AppendMBS( handle->sqlSource, op );

	if( N >2 ){
		DString *mbstring = field->xString.data;
		DString_Reset( mbstring, 0 );
		if( value->type == DAO_MAP ){
			DString_AppendKeyValues( mbstring, (DaoMap*) value );
			DString_AppendChar( handle->sqlSource, '\'' );
			DString_Append( handle->sqlSource, mbstring );
			DString_AppendChar( handle->sqlSource, '\'' );
		}else{
			DaoValue_GetString( value, field->xString.data );
			DString_AppendSQL( handle->sqlSource, field->xString.data, value->type == DAO_STRING, "\'" );
		}
	}else{
		char buf[20];
		handle->partypes[handle->paramCount++] = type;
		sprintf( buf, "$%i", handle->paramCount );
		DString_AppendMBS( handle->sqlSource, buf );
		DString_AppendMBS( handle->sqlSource, "::hstore" );
	}
	DString_Delete( fname );
}
static void DaoPostgreSQLHD_HStoreSet( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoPostgreSQLHD_HStore( proc, p, N, " || ", 1 );
	handle->setCount ++;
}
static void DaoPostgreSQLHD_HStoreDelete( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoPostgreSQLHD_HStore( proc, p, N, " - ", 1 );
	handle->setCount ++;
}
static void DaoPostgreSQLHD_HStoreContain( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoPostgreSQLHD_HStore( proc, p, N, " @> ", 0 );
	handle->boolCount ++;
}

static void DaoPostgreSQLHD_Add( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *fname, *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *key = p[2];
	DaoType *cast = (DaoType*) p[3];
	DaoValue *value = p[4];
	DaoClass *klass;
	int status = 0;

	DaoProcess_PutValue( proc, p[0] );
	if( handle->classList->size == 0 ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	fname = DString_New(1);
	klass = handle->classList->items.pClass[0];
	if( handle->setCount ) DString_AppendMBS( handle->sqlSource, ", " );
	DString_Assign( fname, field->xString.data );

	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		key = p[3];
		cast = (DaoType*) p[4];
		value = p[5];
		klass = (DaoClass*) p[1];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Assign( fname, tabname );
		DString_AppendMBS( fname, "." );
		DString_Append( fname, field->xString.data );
	}
	DString_Append( handle->sqlSource, fname );
	DString_AppendMBS( handle->sqlSource, "=" );
	DString_Append( handle->sqlSource, fname );
	DString_AppendMBS( handle->sqlSource, " || (\'" );
	DString_AppendSQL( handle->sqlSource, key->xString.data, 1, "\"" );
	DString_AppendMBS( handle->sqlSource, "=>\' || " );
	DString_AppendMBS( handle->sqlSource, "CAST( CAST(" );
	DString_Append( handle->sqlSource, fname );
	DString_AppendMBS( handle->sqlSource, "->" );
	DString_AppendSQL( handle->sqlSource, key->xString.data, 1, "\'" );
	DString_AppendMBS( handle->sqlSource, " AS " );
	switch( cast->tid ){
	case DAO_INTEGER : DString_AppendMBS( handle->sqlSource, " INTEGER) " ); break;
	case DAO_FLOAT   : DString_AppendMBS( handle->sqlSource, " FLOAT) " ); break;
	case DAO_DOUBLE  : DString_AppendMBS( handle->sqlSource, " DOUBLE) " ); break;
	}
	DString_AppendMBS( handle->sqlSource, "+" );

	if( N >2 ){
		DString *mbstring = field->xString.data;
		DString_Reset( mbstring, 0 );
		if( value->type == DAO_MAP ){
			DString_AppendKeyValues( mbstring, (DaoMap*) value );
			DString_AppendChar( handle->sqlSource, '\'' );
			DString_Append( handle->sqlSource, mbstring );
			DString_AppendChar( handle->sqlSource, '\'' );
		}else{
			DaoValue_GetString( value, field->xString.data );
			DString_AppendSQL( handle->sqlSource, field->xString.data, value->type == DAO_STRING, "\'" );
		}
	}else{
		char buf[20];
		handle->partypes[handle->paramCount++] = cast;
		sprintf( buf, "$%i", handle->paramCount );
		DString_AppendMBS( handle->sqlSource, buf );
	}
	DString_AppendMBS( handle->sqlSource, " AS TEXT))::hstore" );
	handle->setCount ++;
	DString_Delete( fname );
	//fprintf( stderr, "%s\n", handle->sqlSource->mbs );
}
static void DaoPostgreSQLHD_Operator( DaoProcess *proc, DaoValue *p[], int N, char *op )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *mbstring, *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *key = p[2];
	DaoType *cast = (DaoType*) p[3];
	DaoValue *value = p[4];
	DaoClass *klass;
	int status = 0;

	DaoProcess_PutValue( proc, p[0] );
	if( handle->classList->size == 0 ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	klass = handle->classList->items.pClass[0];
	if( handle->boolCount ) DString_AppendMBS( handle->sqlSource, " AND " );
	DString_AppendMBS( handle->sqlSource, "CAST( " );

	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		key = p[3];
		cast = (DaoType*) p[4];
		value = p[5];
		klass = (DaoClass*) p[1];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( handle->sqlSource, tabname );
		DString_AppendMBS( handle->sqlSource, "." );
	}
	DString_Append( handle->sqlSource, field->xString.data );
	DString_AppendMBS( handle->sqlSource, "->" );
	DString_AppendSQL( handle->sqlSource, key->xString.data, 1, "\'" );
	DString_AppendMBS( handle->sqlSource, " AS " );
	switch( cast->tid ){
	case DAO_INTEGER : DString_AppendMBS( handle->sqlSource, " INTEGER) " ); break;
	case DAO_FLOAT   : DString_AppendMBS( handle->sqlSource, " FLOAT) " ); break;
	case DAO_DOUBLE  : DString_AppendMBS( handle->sqlSource, " DOUBLE) " ); break;
	}
	DString_AppendMBS( handle->sqlSource, op );
	if( N >2 ){
		DString *mbstring = field->xString.data;
		DString_Reset( mbstring, 0 );
		if( value->type == DAO_MAP ){
			DString_AppendKeyValues( mbstring, (DaoMap*) value );
			DString_AppendChar( handle->sqlSource, '\'' );
			DString_Append( handle->sqlSource, mbstring );
			DString_AppendChar( handle->sqlSource, '\'' );
		}else{
			DaoValue_GetString( value, field->xString.data );
			DString_AppendSQL( handle->sqlSource, field->xString.data, value->type == DAO_STRING, "\'" );
		}
	}else{
		char buf[20];
		handle->partypes[handle->paramCount++] = cast;
		sprintf( buf, "$%i", handle->paramCount );
		DString_AppendMBS( handle->sqlSource, buf );
	}
	handle->boolCount ++;
	//fprintf( stderr, "%s\n", handle->sqlSource->mbs );
}
static void DaoPostgreSQLHD_EQ( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, "=" );
}
static void DaoPostgreSQLHD_NE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, "!=" );
}
static void DaoPostgreSQLHD_GT( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, ">" );
}
static void DaoPostgreSQLHD_GE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, ">=" );
}
static void DaoPostgreSQLHD_LT( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, "<" );
}
static void DaoPostgreSQLHD_LE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD_Operator( proc, p, N, "<=" );
}

static void DaoPostgreSQLHD_Sort( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handle = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *tabname = NULL;
	DString *field = DaoValue_TryGetString( p[1] );
	DString *key = DaoValue_TryGetString( p[2] );
	DaoType *cast = DaoValue_CastType( p[3] );
	int desc = DaoValue_TryGetInteger( cast == NULL ? p[3] : p[4] );

	if( p[1]->type == DAO_CLASS ){
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		field = DaoValue_TryGetString( p[2] );
		key = DaoValue_TryGetString( p[3] );
		cast = DaoValue_CastType( p[4] );
		desc = DaoValue_TryGetInteger( cast == NULL ? p[4] : p[5] );
	}

	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handle->sqlSource, " ORDER BY " );
	if( cast != NULL ) DString_AppendMBS( handle->sqlSource, "CAST( " );
	if( tabname != NULL ){
		DString_Append( handle->sqlSource, tabname );
		DString_AppendMBS( handle->sqlSource, "." );
	}
	DString_Append( handle->sqlSource, field );
	DString_AppendMBS( handle->sqlSource, "->" );
	DString_AppendSQL( handle->sqlSource, key, 1, "\'" );
	if( cast != NULL ){
		DString_AppendMBS( handle->sqlSource, " AS" );
		switch( cast->tid ){
		case DAO_INTEGER : DString_AppendMBS( handle->sqlSource, " INTEGER) " ); break;
		case DAO_FLOAT   : DString_AppendMBS( handle->sqlSource, " FLOAT) " ); break;
		case DAO_DOUBLE  : DString_AppendMBS( handle->sqlSource, " DOUBLE) " ); break;
		}
	}
	if( desc ){
		DString_AppendMBS( handle->sqlSource, "DESC " );
	}else{
		DString_AppendMBS( handle->sqlSource, "ASC " );
	}
	fprintf( stderr, "%s\n", handle->sqlSource->mbs );
}


int DaoOnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoVmSpace_LinkModule( vms, ns, "DaoSQL" );
	DaoNamespace_TypeDefine( ns, "int", "PostgreSQL" );
	DaoNamespace_TypeDefine( ns, "map<string,string>", "HSTORE" );
	DaoNamespace_TypeDefine( ns, "tuple<...>", "JSON" );
	dao_type_postgresql_database = DaoNamespace_WrapType( ns, & DaoPostgreSQLDB_Typer, 1 );
	dao_type_postgresql_handle = DaoNamespace_WrapType( ns, & DaoPostgreSQLHD_Typer, 1 );
	return 0;
}
