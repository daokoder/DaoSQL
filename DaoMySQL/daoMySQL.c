
/* DaoMySQL:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2011, Limin Fu (phoolimin@gmail.com).
 */
#include"stdlib.h"
#include"string.h"
#include"daoMySQL.h"

DaoMySQLDB* DaoMySQLDB_New()
{
	DaoMySQLDB *self = malloc( sizeof(DaoMySQLDB) );
	DaoSQLDatabase_Init( (DaoSQLDatabase*) self, DAO_MYSQL );
	self->mysql = mysql_init( NULL );
	self->stmt = mysql_stmt_init( self->mysql );
	return self;
}
void DaoMySQLDB_Delete( DaoMySQLDB *self )
{
	DaoSQLDatabase_Clear( (DaoSQLDatabase*) self );
	mysql_stmt_close( self->stmt );
	mysql_close( self->mysql );
	free( self );
}

static void DaoMySQLDB_DataModel( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_DeleteTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Query( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoMySQLDB_DataModel,"Database<MySQL>( name : string, host='', user='', pwd='' ) => Database<MySQL>"},
	{ DaoMySQLDB_CreateTable,  "CreateTable( self: Database<MySQL>, klass )" },
	{ DaoMySQLDB_DeleteTable,  "DeleteTable( self: Database<MySQL>, klass )" },
//	{ DaoMySQLDB_AlterTable,  "AlterTable( self: Database<MySQL>, klass )" },
	{ DaoMySQLDB_Insert,  "Insert( self: Database<MySQL>, object: @T, ...: @T ) => Handle<MySQL>" },
	{ DaoMySQLDB_DeleteRow, "Delete( self: Database<MySQL>, object ) => Handle<MySQL>"},
	{ DaoMySQLDB_Select, "Select( self: Database<MySQL>, object, ... ) => Handle<MySQL>"},
	{ DaoMySQLDB_Update, "Update( self: Database<MySQL>, object, ... ) => Handle<MySQL>"},
	{ DaoMySQLDB_Query,  "Query( self: Database<MySQL>, sql: string ) => bool" },
	{ NULL, NULL }
};

static DaoTypeBase DaoMySQLDB_Typer = 
{ "Database<MySQL>", NULL, NULL, modelMeths, 
	{ & DaoSQLDatabase_Typer, 0 }, {0}, 
	(FuncPtrDel) DaoMySQLDB_Delete, NULL };

static DaoType *dao_type_mysql_database = NULL;

DaoMySQLHD* DaoMySQLHD_New( DaoMySQLDB *model )
{
	DaoMySQLHD *self = malloc( sizeof(DaoMySQLHD) );
	int i;
	DaoSQLHandle_Init( (DaoSQLHandle*) self, (DaoSQLDatabase*) model );
	self->model = model;
	self->res = NULL;
	self->stmt = mysql_stmt_init( model->mysql );
	memset( self->parbind, 0, MAX_PARAM_COUNT*sizeof(MYSQL_BIND) );
	memset( self->resbind, 0, MAX_PARAM_COUNT*sizeof(MYSQL_BIND) );
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		self->parbind[i].buffer = self->base.pardata[i]->chars;
		self->resbind[i].buffer = self->base.resdata[i]->chars;
		self->resbind[i].buffer_length = self->base.resdata[i]->size;
	}
	self->resbind[0].length = & self->base.reslen;
	return self;
}
void DaoMySQLHD_Delete( DaoMySQLHD *self )
{
	int i;
	/* it appears to close all other stmt associated with the connection too! */
	/* mysql_stmt_close( self->stmt ); */
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		DString_Delete( self->base.pardata[i] );
		DString_Delete( self->base.resdata[i] );
	}
	DString_Delete( self->base.sqlSource );
	DString_Delete( self->base.buffer );
	DList_Delete( self->base.classList );
	DList_Delete( self->base.countList );
	free( self );
}
static void DaoMySQLHD_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_Bind( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoMySQLHD_Insert, "Insert( self: Handle<MySQL>, object: @T, ... : @T ) => Handle<MySQL>" },
	{ DaoMySQLHD_Bind, "Bind( self: Handle<MySQL>, value, index=0 ) => Handle<MySQL>" },
	{ DaoMySQLHD_Query, "Query( self: Handle<MySQL>, ... ) [] => bool" },
	{ DaoMySQLHD_QueryOnce, "QueryOnce( self: Handle<MySQL>, ... ) => bool" },
	{ NULL, NULL }
};

static DaoTypeBase DaoMySQLHD_Typer = 
{ "Handle<MySQL>", NULL, NULL, handleMeths, 
	{ & DaoSQLHandle_Typer, 0 }, {0},
	(FuncPtrDel) DaoMySQLHD_Delete, NULL };

static DaoType *dao_type_mysql_handle = NULL;

static void DaoMySQLDB_DataModel( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = DaoMySQLDB_New();
	DString_Assign( model->base.name, p[0]->xString.value );
	if( N >1 ) DString_Assign( model->base.host, p[1]->xString.value );
	if( N >2 ) DString_Assign( model->base.user, p[2]->xString.value );
	if( N >3 ) DString_Assign( model->base.password, p[3]->xString.value );
	DaoProcess_PutCdata( proc, model, dao_type_mysql_database );
	if( mysql_real_connect( model->mysql, model->base.host->chars, 
				model->base.user->chars, model->base.password->chars, 
				NULL, 0, NULL, 0 ) ==NULL ){
		DaoProcess_RaiseError( proc, NULL, mysql_error( model->mysql ) );
		mysql_close( model->mysql );
		model->mysql = NULL;
		return;
	}
	if( mysql_select_db( model->mysql, model->base.name->chars ) ){
		DString_InsertChars( p[0]->xString.value, "create database ", 0, 0, 0 );
		mysql_query( model->mysql, p[0]->xString.value->chars );
		if( mysql_select_db( model->mysql, model->base.name->chars ) )
			DaoProcess_RaiseError( proc, NULL, mysql_error( model->mysql ) );
	}
}
static void DaoMySQLDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New();
	MYSQL_STMT *stmt;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql );
	stmt = mysql_stmt_init( model->mysql );
	if( mysql_stmt_prepare( stmt, sql->chars, sql->size ) || mysql_stmt_execute( stmt ) )
		DaoProcess_RaiseError( proc, "Param", mysql_error( model->mysql ) );
	DString_Delete( sql );
	mysql_stmt_close( stmt );
}
static void DaoMySQLDB_DeleteTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New();
	MYSQL_STMT *stmt;
	DaoSQLDatabase_DeleteTable( (DaoSQLDatabase*) model, klass, sql );
	stmt = mysql_stmt_init( model->mysql );
	if( mysql_stmt_prepare( stmt, sql->chars, sql->size ) || mysql_stmt_execute( stmt ) )
		DaoProcess_RaiseError( proc, "Param", mysql_error( model->mysql ) );
	DString_Delete( sql );
	mysql_stmt_close( stmt );
}
static void DaoMySQLDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.value;
	if( mysql_query( model->mysql, sql->chars ) ){
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseError( proc, "Param", mysql_error( model->mysql ) );
		return;
	}
	DaoProcess_PutBoolean( proc, 1 );
}
static void DaoMySQLHD_BindValue( DaoMySQLHD *self, DaoValue *value, int index, DaoProcess *proc )
{
	DaoType *type = self->base.partypes[index];
	MYSQL_BIND *bind = self->parbind + index;
	char *pbuf = self->base.pardata[index]->chars;

	//printf( "%i: %s\n", index, type->name->chars );
	switch( value->type ){
	case DAO_NONE :
		bind->buffer_type = MYSQL_TYPE_NULL;
		break;
	case DAO_INTEGER :
		if( type == dao_sql_type_bigint ){
			bind->buffer_type = MYSQL_TYPE_LONGLONG;
			*(long long*) pbuf = value->xInteger.value;
		}else{
			bind->buffer_type = MYSQL_TYPE_LONG;
			*(long*) pbuf = value->xInteger.value;
		}
		break;
	case DAO_FLOAT   :
		bind->buffer_type = MYSQL_TYPE_DOUBLE;
		*(double*) pbuf = value->xFloat.value;
		break;
	case DAO_STRING  :
		DString_SetBytes( self->base.pardata[index], value->xString.value->chars, value->xString.value->size );
		bind->buffer_type = MYSQL_TYPE_STRING;
		bind->buffer = self->base.pardata[index]->chars;
		bind->buffer_length = self->base.pardata[index]->size;
		break;
	case DAO_TUPLE :
		if( type == dao_sql_type_date ){
			int days = DaoSQL_EncodeDate( (DaoTuple*) value );
			bind->buffer_type = MYSQL_TYPE_LONG;
			*(long*) pbuf = days;
		}else if( type == dao_sql_type_timestamp ){
			int64_t msecs = DaoSQL_EncodeTimestamp( (DaoTuple*) value );
			bind->buffer_type = MYSQL_TYPE_LONGLONG;
			*(long long*) pbuf = msecs;
		}
		break;
	default : break;
	}
}
static void DaoMySQLDB_InsertObject( DaoProcess *proc, DaoMySQLHD *handle, DaoObject *object )
{
	DaoClass  *klass = object->defClass;
	DaoVariable **vars = klass->instvars->items.pVar;
	DaoValue  *value;
	MYSQL_BIND *bind;
	char *pbuf, *tpname;
	int i, k = -1;
	for(i=1; i<klass->objDataName->size; i++){
		DaoType *type = DaoType_GetBaseType( vars[i]->dtype );
		tpname = type->name->chars;
		value = object->objValues[i];
		bind = handle->parbind + (i-1);
		if( strcmp( tpname, "INTEGER_PRIMARY_KEY_AUTO_INCREMENT" ) == 0 ){
			bind->buffer_type = MYSQL_TYPE_NULL;
			k = i;
			continue;
		}
		DaoMySQLHD_BindValue( handle, value, i-1, proc );
	}
	if( mysql_stmt_bind_param( handle->stmt, handle->parbind ) )
		DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );
	if( mysql_stmt_execute( handle->stmt ) )
		DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );

	//printf( "k = %i %i\n", k, mysql_insert_id( handle->model->mysql ) );
	if( k >=0 ) object->objValues[k]->xInteger.value = mysql_insert_id( handle->model->mysql );
}
static void DaoMySQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoMySQLHD *handle = DaoMySQLHD_New( model );
	DString *str = handle->base.sqlSource;
	int i;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareInsert( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->chars );
	if( mysql_stmt_prepare( handle->stmt, str->chars, str->size ) ){
		DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );
		return;
	}
	for(i=1; i<N; ++i) DaoMySQLDB_InsertObject( proc, handle, (DaoObject*) p[i] );
}
static void DaoMySQLDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoMySQLHD *handle = DaoMySQLHD_New( model );
	int i;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareDelete( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoMySQLDB_Select( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoMySQLHD *handle = DaoMySQLHD_New( model );
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareSelect( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->chars );
}
static void DaoMySQLDB_Update( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoMySQLHD *handle = DaoMySQLHD_New( model );
	DaoClass *klass;
	int i, j;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareUpdate( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoMySQLHD_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	int i;
	DaoProcess_PutValue( proc, p[0] );
	for(i=1; i<N; ++i){
		if( p[i]->type != DAO_OBJECT || p[i]->xObject.defClass != p[1]->xObject.defClass ){
			DaoProcess_RaiseError( proc, "Param", "" );
			return;
		}
		DaoMySQLDB_InsertObject( proc, handle, (DaoObject*) p[i] );
	}
}
static void DaoMySQLHD_Bind( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	DaoValue *value = p[1];
	int index = p[2]->xInteger.value;
	DaoProcess_PutValue( proc, p[0] );
	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		if( mysql_stmt_prepare( handle->stmt, sql->chars, sql->size ) )
			DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );
		handle->base.prepared = 1;
	}
	handle->base.executed = 0;
	if( index >= MAX_PARAM_COUNT ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	DaoMySQLHD_BindValue( handle, value, index, proc );
}
static int DaoMySQLHD_Execute( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	int i;
	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		mysql_stmt_free_result( handle->stmt );
		if( mysql_stmt_prepare( handle->stmt, sql->chars, sql->size ) ) goto RaiseException;
		handle->base.prepared = 1;
		handle->base.executed = 0;
	}
	if( handle->base.executed ==0 ){
		mysql_stmt_bind_param( handle->stmt, handle->parbind );
		if( mysql_stmt_execute( handle->stmt ) ) goto RaiseException;
		handle->base.executed = 1;
	}
	for(i=1; i<N; i++){
		if( p[i]->type == DAO_OBJECT ){
			if( p[i]->xObject.defClass == handle->base.classList->items.pClass[i-1] ) continue;
		}
		DaoProcess_RaiseError( proc, "Param", "need class instance(s)" );
		return 0;
	}
	return 1;
RaiseException :
	DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );
	return 0;
}
static int DaoMySQLHD_Retrieve( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	char *field;
	unsigned long *lens;
	daoint i, j, m, k = 0, rc = 0;
	daoint pitch, offset;

	if( mysql_stmt_field_count( handle->stmt ) ){
		if( mysql_stmt_bind_result( handle->stmt, handle->resbind ) ) goto RaiseException;
		rc = mysql_stmt_fetch( handle->stmt );
		if( rc > 0 && rc != MYSQL_NO_DATA && rc != MYSQL_DATA_TRUNCATED ) goto RaiseException;
		if( rc == MYSQL_NO_DATA ) return 0;
	}else{
		return 0;
	}
	for(i=1; i<N; i++){
		object = (DaoObject*) p[i];
		klass = object->defClass;
		m = handle->base.countList->items.pInt[i-1];
		for(j=1; j<m; j++){
			MYSQL_FIELD *field = & handle->stmt->fields[k];
			type = DaoType_GetBaseType( klass->instvars->items.pVar[j]->dtype );
			value = object->objValues[j];
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
			case DAO_INTEGER :
				if( field->type == MYSQL_TYPE_LONGLONG ){
					handle->resbind[0].buffer_type = MYSQL_TYPE_LONGLONG;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					value->xInteger.value =  *(long long*)handle->base.resdata[0]->chars;
				}else{
					handle->resbind[0].buffer_type = MYSQL_TYPE_LONG;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					value->xInteger.value =  *(int*)handle->base.resdata[0]->chars;
				}
				break;
			case DAO_FLOAT   :
				handle->resbind[0].buffer_type = MYSQL_TYPE_DOUBLE;
				mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
				value->xFloat.value =  *(double*)handle->base.resdata[0]->chars;
				break;
			case DAO_STRING  :
				handle->resbind[0].buffer_type = MYSQL_TYPE_STRING;
				mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
				pitch = handle->base.reslen >= MAX_DATA_SIZE ? MAX_DATA_SIZE : handle->base.reslen;
				DString_Clear( value->xString.value );
				DString_AppendBytes( value->xString.value, handle->base.resdata[0]->chars, pitch );
				offset = MAX_DATA_SIZE;
				while( offset < handle->base.reslen ){
					rc = mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, offset );
					pitch = offset;
					offset += MAX_DATA_SIZE;
					pitch = handle->base.reslen >= offset ? MAX_DATA_SIZE : handle->base.reslen - pitch;
					DString_AppendBytes( value->xString.value, handle->base.resdata[0]->chars, pitch );
				}
				break;
			case DAO_TUPLE :
				if( type == dao_sql_type_date ){
					DaoTuple *tuple = (DaoTuple*) value;
					handle->resbind[0].buffer_type = MYSQL_TYPE_LONG;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					DaoSQL_DecodeDate( tuple, *(long*)handle->base.resdata[0]->chars );
					break;
				}else if( type == dao_sql_type_timestamp ){
					DaoTuple *tuple = (DaoTuple*) value;
					handle->resbind[0].buffer_type = MYSQL_TYPE_LONGLONG;
					mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
					DaoSQL_DecodeTimestamp( tuple, *(long long*)handle->base.resdata[0]->chars );
					break;
				}
				break;
			default : break;
			}
			k ++;
		}
	}
	return 1;
RaiseException :
	DaoProcess_RaiseError( proc, "Param", mysql_stmt_error( handle->stmt ) );
	return 0;
}
static void DaoMySQLHD_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	char *field;
	unsigned long *lens;
	DaoVmCode *sect;
	DaoValue *params[DAO_MAX_PARAM+1];
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	dao_integer *res = DaoProcess_PutBoolean( proc, 0 );
	int i, j, k = 0, rc = 0;
	int pitch, offset;
	int entry;

	if( DaoMySQLHD_Execute( proc, p, N ) == 0 ) return;

	sect = DaoProcess_InitCodeSection( proc, 1 );
	if( sect == NULL ) return;
	entry = proc->topFrame->entry;
	memcpy( params, p, N*sizeof(DaoValue*) );
	handle->base.stopQuery = 0;
	while(1){
		if( DaoMySQLHD_Retrieve( proc, params, N ) == 0 ) break;

		proc->topFrame->entry = entry;
		DaoProcess_Execute( proc );
		if( proc->status == DAO_PROCESS_ABORTED ) break;
		*res += 1;
		if( handle->base.stopQuery ) break;
	}
	DaoProcess_PopFrame( proc );
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}
static void DaoMySQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	dao_integer *res = DaoProcess_PutBoolean( proc, 0 );
	if( DaoMySQLHD_Execute( proc, p, N ) == 0 ) return;
	*res = DaoMySQLHD_Retrieve( proc, p, N );
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}

int DaoMysql_OnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoMap *engines;
	DaoNamespace *sqlns = DaoVmSpace_LinkModule( vms, ns, "sql" );
	sqlns = DaoNamespace_GetNamespace( sqlns, "SQL" );
	dao_type_mysql_database = DaoNamespace_WrapType( sqlns, & DaoMySQLDB_Typer, 1 );
	dao_type_mysql_handle = DaoNamespace_WrapType( sqlns, & DaoMySQLHD_Typer, 1 );
	engines = (DaoMap*) DaoNamespace_FindData( sqlns, "Engines" );
	DaoMap_InsertChars( engines, "MySQL", (DaoValue*) dao_type_mysql_database );
	return 0;
}
