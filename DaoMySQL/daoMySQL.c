
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
static void DaoMySQLDB_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLDB_Query( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoMySQLDB_DataModel,"SQLDatabase<MySQL>( name : string, host='', user='', pwd='' )=>SQLDatabase<MySQL>"},
	{ DaoMySQLDB_CreateTable,  "CreateTable( self:SQLDatabase<MySQL>, klass )" },
//	{ DaoMySQLDB_AlterTable,  "AlterTable( self:SQLDatabase<MySQL>, klass )" },
	{ DaoMySQLDB_Insert,  "Insert( self:SQLDatabase<MySQL>, object :@T, ... :@T )=>SQLHandle<MySQL>" },
	{ DaoMySQLDB_DeleteRow, "Delete( self:SQLDatabase<MySQL>, object )=>SQLHandle<MySQL>"},
	{ DaoMySQLDB_Select, "Select( self:SQLDatabase<MySQL>, object,...)=>SQLHandle<MySQL>"},
	{ DaoMySQLDB_Update, "Update( self:SQLDatabase<MySQL>, object,...)=>SQLHandle<MySQL>"},
	{ DaoMySQLDB_Query,  "Query( self:SQLDatabase<MySQL>, sql : string ) => int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoMySQLDB_Typer = 
{ "SQLDatabase<MySQL>", NULL, NULL, modelMeths, 
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
		self->parbind[i].buffer = self->base.pardata[i]->bytes;
		self->resbind[i].buffer = self->base.resdata[i]->bytes;
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
	DArray_Delete( self->base.classList );
	DArray_Delete( self->base.countList );
	free( self );
}
static void DaoMySQLHD_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_Bind( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoMySQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoMySQLHD_Insert, "Insert( self:SQLHandle<MySQL>, object :@T, ... :@T ) => int" },
	{ DaoMySQLHD_Bind, "Bind( self:SQLHandle<MySQL>, value, index=0 )=>SQLHandle<MySQL>" },
	{ DaoMySQLHD_Query, "Query( self:SQLHandle<MySQL>, ... ) [] => int" },
	{ DaoMySQLHD_QueryOnce, "QueryOnce( self:SQLHandle<MySQL>, ... ) => int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoMySQLHD_Typer = 
{ "SQLHandle<MySQL>", NULL, NULL, handleMeths, 
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
	if( mysql_real_connect( model->mysql, model->base.host->bytes, 
				model->base.user->bytes, model->base.password->bytes, 
				NULL, 0, NULL, 0 ) ==NULL ){
		DaoProcess_RaiseException( proc, DAO_ERROR, mysql_error( model->mysql ) );
		mysql_close( model->mysql );
		model->mysql = NULL;
		return;
	}
	if( mysql_select_db( model->mysql, model->base.name->bytes ) ){
		DString_InsertChars( p[0]->xString.value, "create database ", 0, 0, 0 );
		mysql_query( model->mysql, p[0]->xString.value->bytes );
		if( mysql_select_db( model->mysql, model->base.name->bytes ) )
			DaoProcess_RaiseException( proc, DAO_ERROR, mysql_error( model->mysql ) );
	}
}
static void DaoMySQLDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New(1);
	MYSQL_STMT *stmt;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql );
	stmt = mysql_stmt_init( model->mysql );
	if( mysql_stmt_prepare( stmt, sql->bytes, sql->size ) || mysql_stmt_execute( stmt ) )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_error( model->mysql ) );
	DString_Delete( sql );
	mysql_stmt_close( stmt );
}
static void DaoMySQLDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLDB *model = (DaoMySQLDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.value;
	if( mysql_query( model->mysql, sql->bytes ) ){
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_error( model->mysql ) );
		return;
	}
	DaoProcess_PutInteger( proc, 1 );
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
		tpname = vars[i]->dtype->name->bytes;
		value = object->objValues[i];
		bind = handle->parbind + (i-1);
		pbuf = handle->base.pardata[i-1]->bytes;
		if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ) k = i;
		switch( value->type ){
			case DAO_NONE :
				bind->buffer_type = MYSQL_TYPE_NULL;
				break;
			case DAO_INTEGER :
				if( i == k ){
					bind->buffer_type = MYSQL_TYPE_NULL;
				}else{
					bind->buffer_type = MYSQL_TYPE_LONG;
					*(long*) pbuf = value->xInteger.value;
				}
				break;
			case DAO_FLOAT   :
				bind->buffer_type = MYSQL_TYPE_DOUBLE;
				*(double*) pbuf = value->xFloat.value;
				break;
			case DAO_DOUBLE  :
				bind->buffer_type = MYSQL_TYPE_DOUBLE;
				*(double*) pbuf = value->xDouble.value;
				break;
			case DAO_STRING  :
				DString_SetBytes( handle->base.pardata[i-1], value->xString.value->bytes, value->xString.value->size );
				bind->buffer_type = MYSQL_TYPE_STRING;
				bind->buffer = handle->base.pardata[i-1]->bytes;
				bind->buffer_length = handle->base.pardata[i-1]->size;
				break;
			default : break;
		}
	}
	if( mysql_stmt_bind_param( handle->stmt, handle->parbind ) )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
	if( mysql_stmt_execute( handle->stmt ) )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );

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
	//printf( "%s\n", handle->base.sqlSource->bytes );
	if( mysql_stmt_prepare( handle->stmt, str->bytes, str->size ) ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
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
	//printf( "%s\n", handle->base.sqlSource->bytes );
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
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
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
		if( mysql_stmt_prepare( handle->stmt, sql->bytes, sql->size ) )
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
			*(long*)handle->base.pardata[index]->bytes = value->xInteger.value;
			break;
		case DAO_FLOAT :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_DOUBLE;
			*(double*)handle->base.pardata[index]->bytes = value->xFloat.value;
			break;
		case DAO_DOUBLE :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_DOUBLE;
			*(double*)handle->base.pardata[index]->bytes = value->xDouble.value;
			break;
		case DAO_STRING :
			handle->parbind[ index ].buffer_type = MYSQL_TYPE_STRING;
			DString_SetChars( handle->base.pardata[index], DString_GetData( value->xString.value ) );
			handle->parbind[ index ].buffer = handle->base.pardata[index]->bytes;
			handle->parbind[ index ].buffer_length = handle->base.pardata[index]->size;
			break;
		default : break;
	}
}
static int DaoMySQLHD_Execute( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	int i;
	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		mysql_stmt_free_result( handle->stmt );
		if( mysql_stmt_prepare( handle->stmt, sql->bytes, sql->size ) ) goto RaiseException;
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
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "need class instance(s)" );
		return 0;
	}
	return 1;
RaiseException :
	DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
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
				value->xInteger.value =  *(int*)handle->base.resdata[0]->bytes;
				break;
			case DAO_FLOAT   :
				handle->resbind[0].buffer_type = MYSQL_TYPE_DOUBLE;
				mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
				value->xFloat.value =  *(double*)handle->base.resdata[0]->bytes;
				break;
			case DAO_DOUBLE  :
				handle->resbind[0].buffer_type = MYSQL_TYPE_DOUBLE;
				mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
				value->xDouble.value =  *(double*)handle->base.resdata[0]->bytes;
				break;
			case DAO_STRING  :
				handle->resbind[0].buffer_type = MYSQL_TYPE_STRING;
				mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, 0 );
				pitch = handle->base.reslen >= MAX_DATA_SIZE ? MAX_DATA_SIZE : handle->base.reslen;
				DString_Clear( value->xString.value );
				DString_AppendBytes( value->xString.value, handle->base.resdata[0]->bytes, pitch );
				offset = MAX_DATA_SIZE;
				while( offset < handle->base.reslen ){
					rc = mysql_stmt_fetch_column( handle->stmt, handle->resbind, k, offset );
					pitch = offset;
					offset += MAX_DATA_SIZE;
					pitch = handle->base.reslen >= offset ? MAX_DATA_SIZE : handle->base.reslen - pitch;
					DString_AppendBytes( value->xString.value, handle->base.resdata[0]->bytes, pitch );
				}
				break;
			default : break;
			}
			k ++;
		}
	}
	return 1;
RaiseException :
	DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
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
	DaoVmCode *sect = DaoGetSectionCode( proc->activeCode );
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	daoint *res = DaoProcess_PutInteger( proc, 0 );
	int i, j, k = 0, rc = 0;
	int pitch, offset;
	int entry;

	if( DaoMySQLHD_Execute( proc, p, N ) == 0 ) return;

	if( sect == NULL ) return;
	if( DaoProcess_PushSectionFrame( proc ) == NULL ) return;
	entry = proc->topFrame->entry;
	DaoProcess_AcquireCV( proc );

	while(1){
		if( DaoMySQLHD_Retrieve( proc, p, N ) == 0 ) break;

		proc->topFrame->entry = entry;
		DaoProcess_Execute( proc );
		if( proc->status == DAO_PROCESS_ABORTED ) break;
		*res += 1;
		value = proc->stackValues[0];
		if( value == NULL || value->type != DAO_ENUM || value->xEnum.value != 0 ) break;
	}

	DaoProcess_ReleaseCV( proc );
	DaoProcess_PopFrame( proc );
}
static void DaoMySQLHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoMySQLHD *handle = (DaoMySQLHD*) p[0]->xCdata.data;
	daoint *res = DaoProcess_PutInteger( proc, 0 );
	if( DaoMySQLHD_Execute( proc, p, N ) == 0 ) return;
	*res = DaoMySQLHD_Retrieve( proc, p, N );
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}

int DaoOnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoVmSpace_LinkModule( vms, ns, "sql" );
	DaoNamespace_TypeDefine( ns, "int", "MySQL" );
	dao_type_mysql_database = DaoNamespace_WrapType( ns, & DaoMySQLDB_Typer, 1 );
	dao_type_mysql_handle = DaoNamespace_WrapType( ns, & DaoMySQLHD_Typer, 1 );
	return 0;
}
