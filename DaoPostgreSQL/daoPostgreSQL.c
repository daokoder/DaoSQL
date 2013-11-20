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
	memset( self->parbind, 0, MAX_PARAM_COUNT*sizeof(MYSQL_BIND) );
	memset( self->resbind, 0, MAX_PARAM_COUNT*sizeof(MYSQL_BIND) );
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		self->parbind[i].buffer = self->base.pardata[i]->mbs;
		self->resbind[i].buffer = self->base.resdata[i]->mbs;
		self->resbind[i].buffer_length = self->base.resdata[i]->size;
	}
	self->resbind[0].length = & self->base.reslen;
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
static void DaoPostgreSQLHD_Done( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoPostgreSQLHD_Insert, "Insert( self:SQLHandle<PostgreSQL>, object ) => int" },
	{ DaoPostgreSQLHD_Bind, "Bind( self:SQLHandle<PostgreSQL>, value, index=0 )=>SQLHandle<PostgreSQL>" },
	{ DaoPostgreSQLHD_Query, "Query( self:SQLHandle<PostgreSQL>, ... ) => int" },
	{ DaoPostgreSQLHD_QueryOnce, "QueryOnce( self:SQLHandle<PostgreSQL>, ... ) => int" },
	{ DaoPostgreSQLHD_Done,  "Done( self:SQLHandle<PostgreSQL> )" },
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
	if( PQresultStatus(res) != PGRES_COMMAND_OK )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
	DString_Delete( sql );
	PQclear(res);
}
static void DaoPostgreSQLDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.data;
	PGresult *res;
	DString_ToMBS( sql );
	res = PQexec( model->conn, sql->mbs );
	if( PQresultStatus(res) != PGRES_COMMAND_OK ){
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
		return;
	}
	DaoProcess_PutInteger( proc, 1 );
}
static void DaoPostgreSQLDB_InsertObject( DaoProcess *proc, DaoPostgreSQLHD *handle, DaoObject *object )
{
	PGresult *res;
	DaoPostgreSQLDB *model = handle->model;
	DaoClass *klass = object->defClass;
	DaoVariable **vars = klass->instvars->items.pVar;
	DString *mbstring;
	int i, k = -1;
	for(i=1,k=0; i<klass->objDataName->size; i++){
		char *tpname = vars[i]->dtype->name->mbs;
		DaoValue *value = object->objValues[i];
		if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ) continue;
		handle->paramLengths[k] = 0;
		handle->paramFormats[k] = 0;
		switch( value->type ){
			case DAO_NONE :
				handle->paramValues[k] = NULL;
				break;
			case DAO_INTEGER :
				handle->paramFormats[k] = 1;
				handle->paramLengths[k] = sizeof(uint32_t);
				handle->paramValues[k] = (char*) & handle->paramInts32[k];
				handle->paramInts32[k] = htonl((uint32_t) value->xInteger.value );
				break;
			case DAO_FLOAT   :
				handle->paramFormats[k] = 1;
				handle->paramLengths[k] = sizeof(uint32_t);
				handle->paramValues[k] = (char*) & handle->paramInts32[k];
				handle->paramInts32[k] = htobe32(*(uint32_t*) &value->xFloat.value );
				break;
			case DAO_DOUBLE  :
				handle->paramFormats[k] = 1;
				handle->paramLengths[k] = sizeof(uint64_t);
				handle->paramValues[k] = (char*) & handle->paramInts64[k];
				handle->paramInts64[k] = htobe64(*(uint64_t*) &value->xDouble.value );
				break;
			case DAO_STRING  :
				mbstring = value->xString.data;
				DString_ToMBS( mbstring );
				DString_SetDataMBS( handle->base.pardata[k], mbstring->mbs, mbstring->size );
				handle->paramValues[k] = handle->base.pardata[k]->mbs;
				handle->paramLengths[k] = mbstring->size;
				break;
			default : break;
		}
		k += 1;
	}
	res = PQexecPrepared( model->conn, handle->name->mbs, k, handle->paramValues,
			handle->paramLengths, handle->paramFormats, 1 );
	if( PQresultStatus(res) != PGRES_COMMAND_OK ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, PQerrorMessage( model->conn ) );
		return;
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
static void DaoPostgreSQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLDB *model = (DaoPostgreSQLDB*) p[0]->xCdata.data;
	DaoPostgreSQLHD *handle = DaoPostgreSQLHD_New( model );
	DaoObject *object = (DaoObject*) p[1];
	DaoClass *klass = object->defClass;
	DString *str = handle->base.sqlSource;
	PGresult *res;
	int i, k;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_postgresql_handle, handle ) );
	if( DaoSQLHandle_PrepareInsert( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->mbs );
	if( p[1]->type == DAO_LIST ){
		/* Already checked by DaoSQLHandle_PrepareInsert(): */
		klass = p[1]->xList.items.items.pValue[0]->xObject.defClass;
	}
	for(i=1,k=0; i<klass->objDataName->size; i++){
		DaoType *type = klass->instvars->items.pVar[i]->dtype;
		if( strcmp( type->name->mbs, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ) continue;
		handle->paramTypes[k] = 0;
		switch( type->tid ){
		case DAO_NONE :
			handle->paramTypes[k] = VOIDOID; // ???
			break;
		case DAO_INTEGER :
			handle->paramTypes[k] = INT4OID;
			break;
		case DAO_FLOAT :
			handle->paramTypes[k] = FLOAT4OID;
			break;
		case DAO_DOUBLE :
			handle->paramTypes[k] = FLOAT8OID;
			break;
		case DAO_STRING :
			handle->paramTypes[k] = BYTEAOID; // ???
			if( strstr( type->name->mbs, "CHAR" ) == type->name->mbs ){
				handle->paramTypes[k] = BPCHAROID;
			}else if( strstr( type->name->mbs, "VARCHAR" ) == type->name->mbs ){
				handle->paramTypes[k] = VARCHAROID;
			}else if( strcmp( type->name->mbs, "TEXT" ) == 0 ){
				handle->paramTypes[k] = TEXTOID;
			}
			break;
		}
		k += 1;
	}
	res = PQprepare( model->conn, handle->name->mbs, str->mbs, k, handle->paramTypes );
	if( PQresultStatus(res) != PGRES_COMMAND_OK ){
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
		DString *sql = handle->base.sqlSource;
		if( mysql_stmt_prepare( handle->stmt, sql->mbs, sql->size ) )
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
		handle->base.prepared = 1;
	}
	handle->base.executed = 0;
	if( index >= MAX_PARAM_COUNT ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	switch( value->type ){
		case DAO_NONE :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_NULL;
			break;
		case DAO_INTEGER :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_LONG;
			*(long*)handle->base.pardata[index]->mbs = value->xInteger.value;
			break;
		case DAO_FLOAT :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_DOUBLE;
			*(double*)handle->base.pardata[index]->mbs = value->xFloat.value;
			break;
		case DAO_DOUBLE :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_DOUBLE;
			*(double*)handle->base.pardata[index]->mbs = value->xDouble.value;
			break;
		case DAO_STRING :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_STRING;
			DString_SetMBS( handle->base.pardata[index], DString_GetMBS( value->xString.data ) );
			handle->parbind[ index ].buffer = handle->base.pardata[index]->mbs;
			handle->parbind[ index ].buffer_length = handle->base.pardata[index]->size;
			break;
		default : break;
	}
}
//#define USE_RES
static void DaoPostgreSQLHD_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	MYSQL_ROW row;
	char *field;
	unsigned long *lens;
	daoint *res = DaoProcess_PutInteger( proc, 1 );
	int i, j, k = 0, rc = 0;
	int pitch, offset;
	int m;
	//printf( "%s\n", handle->base.sqlSource->mbs );
	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		mysql_stmt_free_result( handle->stmt );
		if( mysql_stmt_prepare( handle->stmt, sql->mbs, sql->size ) )
			goto RaiseException;
		handle->base.prepared = 1;
		handle->base.executed = 0;
	}
	if( handle->base.executed ==0 ){
#ifdef USE_RES
		if( handle->res ) mysql_free_result( handle->res );
#endif
		mysql_stmt_bind_param( handle->stmt, handle->parbind );
		if( mysql_stmt_execute( handle->stmt ) ) goto RaiseException;
#ifdef USE_RES
		handle->res = mysql_use_result( handle->model->conn );
#endif
		handle->base.executed = 1;
	}
#ifdef USE_RES
	m = mysql_num_fields( handle->res );
	row = mysql_fetch_row( handle->res );
	if( row == NULL ){
		*res = 0;
		return;
	}
	lens = mysql_fetch_lengths( handle->res );
	for(i=1; i<N; i++){
		if( p[i]->type != DAO_OBJECT ) goto RaiseException2;
		object = (DaoObject*) p[i];
		klass = object->myClass;
		if( klass != handle->base.classList->items.pClass[i-1] ) goto RaiseException2;
		m = handle->base.countList->items.pInt[i-1];
		for(j=1; j<m; j++){
			type = klass->instvars->items.pVar[j]->dtype;
			value = object->objValues[j];
			field = row[k];
			k ++;
			if( field == NULL ) continue;
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
				case DAO_INTEGER :
					value->xInteger.value = strtol( field, NULL, 10 );
					break;
				case DAO_FLOAT   :
					value->xFloat.value = strtod( field, NULL );
					break;
				case DAO_DOUBLE  :
					value->xDouble.value = strtod( field, NULL );
					break;
				case DAO_STRING  :
					DString_SetBytes( value->xString.data, field, lens[k-1] );
					break;
				default : break;
			}
		}
	}
#else
	if( mysql_stmt_field_count( handle->stmt ) ){
		if( mysql_stmt_bind_result( handle->stmt, handle->resbind ) ) goto RaiseException;
		rc = mysql_stmt_fetch( handle->stmt );
		if( rc > 0 && rc != MYSQL_NO_DATA && rc != MYSQL_DATA_TRUNCATED ) goto RaiseException;
		if( rc == MYSQL_NO_DATA ){
			*res = 0;
			return;
		}
	}else{
		*res = 0;
		return;
	}
	for(i=1; i<N; i++){
		if( p[i]->type != DAO_OBJECT ) goto RaiseException2;
		object = (DaoObject*) p[i];
		klass = object->defClass;
		if( klass != handle->base.classList->items.pClass[i-1] ) goto RaiseException2;
		m = handle->base.countList->items.pInt[i-1];
		for(j=1; j<m; j++){
			type = klass->instvars->items.pVar[j]->dtype;
			value = object->objValues[j];
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
				case DAO_INTEGER :
					handle->resbind[0].buffer_type = MYSQL_TYPE_LONG;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					value->xInteger.value =  *(int*)handle->base.resdata[0]->mbs;
					break;
				case DAO_FLOAT   :
					handle->resbind[0].buffer_type = MYSQL_TYPE_DOUBLE;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					value->xFloat.value =  *(double*)handle->base.resdata[0]->mbs;
					break;
				case DAO_DOUBLE  :
					handle->resbind[0].buffer_type = MYSQL_TYPE_DOUBLE;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					value->xDouble.value =  *(double*)handle->base.resdata[0]->mbs;
					break;
				case DAO_STRING  :
					handle->resbind[0].buffer_type = MYSQL_TYPE_STRING;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					pitch = handle->base.reslen >= MAX_DATA_SIZE ? MAX_DATA_SIZE : handle->base.reslen;
					DString_Clear( value->xString.data );
					DString_AppendDataMBS( value->xString.data, handle->base.resdata[0]->mbs, pitch );
					offset = MAX_DATA_SIZE;
					while( offset < handle->base.reslen ){
						rc = mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, offset );
						pitch = offset;
						offset += MAX_DATA_SIZE;
						pitch = handle->base.reslen >= offset ? MAX_DATA_SIZE : handle->base.reslen - pitch;
						DString_AppendDataMBS( value->xString.data, handle->base.resdata[0]->mbs, pitch );
					}
					break;
				default : break;
			}
			k ++;
		}
	}
#endif
	return;
RaiseException :
	DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
	*res = 0;
	return;
RaiseException2 :
	DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "need class instance(s)" );
	*res = 0;
}
static void DaoPostgreSQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	DaoPostgreSQLHD_Query( proc, p, N );
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}
static void DaoPostgreSQLHD_Done( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoPostgreSQLHD *handle = (DaoPostgreSQLHD*) p[0]->xCdata.data;
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}

int DaoOnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoVmSpace_LinkModule( vms, ns, "DaoSQL" );
	DaoNamespace_TypeDefine( ns, "int", "PostgreSQL" );
	DaoNamespace_TypeDefine( ns, "map<string,string>", "HSTORE" );
	dao_type_postgresql_database = DaoNamespace_WrapType( ns, & DaoPostgreSQLDB_Typer, 1 );
	dao_type_postgresql_handle = DaoNamespace_WrapType( ns, & DaoPostgreSQLHD_Typer, 1 );
	return 0;
}
