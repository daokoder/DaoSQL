
/* DaoDataModel:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2011, Limin Fu (phoolimin@gmail.com).
 */
#include"stdlib.h"
#include"string.h"
#include"daoMySQL.h"

DaoDataModel* DaoDataModel_New()
{
	DaoDataModel *self = malloc( sizeof(DaoDataModel) );
	DaoSQLDatabase_Init( (DaoSQLDatabase*) self );
	self->mysql = mysql_init( NULL );
	self->stmt = mysql_stmt_init( self->mysql );
	return self;
}
void DaoDataModel_Delete( DaoDataModel *self )
{
	DaoSQLDatabase_Clear( (DaoSQLDatabase*) self );
	mysql_stmt_close( self->stmt );
	mysql_close( self->mysql );
	free( self );
}

static void DaoDataModel_DataModel( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_CreateTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoDataModel_Query( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoDataModel_DataModel,"SQLDatabase<MySQL>( name : string, host='', user='', pwd='' )=>SQLDatabase<MySQL>"},
	{ DaoDataModel_CreateTable,  "CreateTable( self:SQLDatabase<MySQL>, klass )" },
//	{ DaoDataModel_AlterTable,  "AlterTable( self:SQLDatabase<MySQL>, klass )" },
	{ DaoDataModel_Insert,  "Insert( self:SQLDatabase<MySQL>, object )=>SQLHandle<MySQL>" },
	{ DaoDataModel_DeleteRow, "Delete( self:SQLDatabase<MySQL>, object )=>SQLHandle<MySQL>"},
	{ DaoDataModel_Select, "Select( self:SQLDatabase<MySQL>, object,...)=>SQLHandle<MySQL>"},
	{ DaoDataModel_Update, "Update( self:SQLDatabase<MySQL>, object,...)=>SQLHandle<MySQL>"},
	{ DaoDataModel_Query,  "Query( self:SQLDatabase<MySQL>, sql : string ) => int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoDataModel_Typer = 
{ "SQLDatabase<MySQL>", NULL, NULL, modelMeths, 
	{ & DaoSQLDatabase_Typer, 0 }, {0}, 
	(FuncPtrDel) DaoDataModel_Delete, NULL };

static DaoType *dao_type_mysql_datamodel = NULL;

DaoHandle* DaoHandle_New( DaoDataModel *model )
{
	DaoHandle *self = malloc( sizeof(DaoHandle) );
	int i;
	DaoSQLHandle_Init( (DaoSQLHandle*) self );
	self->model = model;
	self->res = NULL;
	self->stmt = mysql_stmt_init( model->mysql );
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
void DaoHandle_Delete( DaoHandle *self )
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
static void DaoHandle_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoHandle_Bind( DaoProcess *proc, DaoValue *p[], int N );
static void DaoHandle_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoHandle_QueryOnce( DaoProcess *proc, DaoValue *p[], int N );
static void DaoHandle_Done( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoHandle_Insert, "Insert( self:SQLHandle<MySQL>, object ) => int" },
	{ DaoHandle_Bind, "Bind( self:SQLHandle<MySQL>, value, index=0 )=>SQLHandle<MySQL>" },
	{ DaoHandle_Query, "Query( self:SQLHandle<MySQL>, ... ) => int" },
	{ DaoHandle_QueryOnce, "QueryOnce( self:SQLHandle<MySQL>, ... ) => int" },
	{ DaoHandle_Done,  "Done( self:SQLHandle<MySQL> )" },
	{ NULL, NULL }
};

static DaoTypeBase DaoHandle_Typer = 
{ "SQLHandle<MySQL>", NULL, NULL, handleMeths, 
	{ & DaoSQLHandle_Typer, 0 }, {0},
	(FuncPtrDel) DaoHandle_Delete, NULL };

static DaoType *dao_type_mysql_handle = NULL;

static void DaoDataModel_DataModel( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = DaoDataModel_New();
	DString_Assign( model->base.name, p[0]->xString.data );
	if( N >1 ) DString_Assign( model->base.host, p[1]->xString.data );
	if( N >2 ) DString_Assign( model->base.user, p[2]->xString.data );
	if( N >3 ) DString_Assign( model->base.password, p[3]->xString.data );
	DString_ToMBS( model->base.name );
	DString_ToMBS( model->base.host );
	DString_ToMBS( model->base.user );
	DString_ToMBS( model->base.password );
	DaoProcess_PutCdata( proc, model, dao_type_mysql_datamodel );
	if( mysql_real_connect( model->mysql, model->base.host->mbs, 
				model->base.user->mbs, model->base.password->mbs, 
				NULL, 0, NULL, 0 ) ==NULL ){
		DaoProcess_RaiseException( proc, DAO_ERROR, mysql_error( model->mysql ) );
		mysql_close( model->mysql );
		model->mysql = NULL;
		return;
	}
	if( mysql_select_db( model->mysql, model->base.name->mbs ) ){
		DString_InsertMBS( p[0]->xString.data, "create database ", 0, 0, 0 );
		mysql_query( model->mysql, p[0]->xString.data->mbs );
		if( mysql_select_db( model->mysql, model->base.name->mbs ) )
			DaoProcess_RaiseException( proc, DAO_ERROR, mysql_error( model->mysql ) );
	}
}
static void DaoDataModel_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DaoType **types;
	DString **names;
	DString *sql = DString_New(1);
	DString *tabname = NULL;
	DNode *node;
	MYSQL_STMT *stmt;
	char *tpname;
	int i;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql, DAO_MYSQL );
	stmt = mysql_stmt_init( model->mysql );
	if( mysql_stmt_prepare( stmt, sql->mbs, sql->size ) || mysql_stmt_execute( stmt ) )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_error( model->mysql ) );
	DString_Delete( sql );
	mysql_stmt_close( stmt );
}
static void DaoDataModel_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.data;
	DString_ToMBS( sql );
	if( mysql_query( model->mysql, sql->mbs ) ){
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_error( model->mysql ) );
		return;
	}
	DaoProcess_PutInteger( proc, 1 );
}
static void DaoDataModel_InsertObject( DaoProcess *proc, DaoHandle *handle, DaoObject *object )
{
	DaoClass  *klass = object->defClass;
	DaoType **types = klass->objDataType->items.pType;
	DaoValue  *value;
	MYSQL_BIND *bind;
	char *pbuf, *tpname;
	int i, k = -1;
	for(i=1; i<klass->objDataName->size; i++){
		tpname = types[i]->name->mbs;
		value = object->objValues[i];
		bind = handle->parbind + (i-1);
		pbuf = handle->base.pardata[i-1]->mbs;
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
				DString_ToMBS( value->xString.data );
				DString_SetDataMBS( handle->base.pardata[i-1], value->xString.data->mbs, value->xString.data->size );
				bind->buffer_type = MYSQL_TYPE_STRING;
				bind->buffer = handle->base.pardata[i-1]->mbs;
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
static void DaoDataModel_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DaoHandle *handle = DaoHandle_New( model );
	DaoObject *object = (DaoObject*) p[1];
	DString *str = handle->base.sqlSource;
	DString *tabname = NULL;
	int i;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareInsert( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->mbs );
	if( mysql_stmt_prepare( handle->stmt, str->mbs, str->size ) ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, mysql_stmt_error( handle->stmt ) );
		return;
	}
	if( p[1]->type == DAO_LIST ){
		for( i=0; i<p[1]->xList.items.size; i++ )
			DaoDataModel_InsertObject( proc, handle, p[1]->xList.items.items.pObject[i] );
	}else{
		DaoDataModel_InsertObject( proc, handle, object );
	}
}
static void DaoDataModel_DeleteRow( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DaoHandle *handle = DaoHandle_New( model );
	int i;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareDelete( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoDataModel_Select( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DaoHandle *handle = DaoHandle_New( model );
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareSelect( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->mbs );
}
static void DaoDataModel_Update( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoDataModel *model = (DaoDataModel*) p[0]->xCdata.data;
	DaoHandle *handle = DaoHandle_New( model );
	DaoClass *klass;
	DString *tabname = NULL;
	int i, j;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_mysql_handle, handle ) );
	if( DaoSQLHandle_PrepareUpdate( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoHandle_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoHandle *handle = (DaoHandle*) p[0]->xCdata.data;
	DaoValue *value;
	int i;
	DaoProcess_PutValue( proc, p[0] );
	if( p[1]->type == DAO_OBJECT ){
		DaoDataModel_InsertObject( proc, handle, (DaoObject*) p[1] );
	}else if( p[1]->type == DAO_LIST ){
		for(i=0; i<p[1]->xList.items.size; i++){
			value = p[1]->xList.items.items.pValue[i];
			if( value->type != DAO_OBJECT 
					|| value->xObject.defClass != handle->base.classList->items.pClass[0] ){
				DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
				return;
			}
			DaoDataModel_InsertObject( proc, handle, (DaoObject*) value );
		}
	}
}
static void DaoHandle_Bind( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoHandle *handle = (DaoHandle*) p[0]->xCdata.data;
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
static void DaoHandle_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoHandle *handle = (DaoHandle*) p[0]->xCdata.data;
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
		handle->res = mysql_use_result( handle->model->mysql );
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
			type = klass->objDataType->items.pType[j];
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
			type = klass->objDataType->items.pType[j];
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
static void DaoHandle_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoHandle *handle = (DaoHandle*) p[0]->xCdata.data;
	DaoHandle_Query( proc, p, N );
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}
static void DaoHandle_Done( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoHandle *handle = (DaoHandle*) p[0]->xCdata.data;
	if( handle->base.executed ) mysql_stmt_free_result( handle->stmt );
}

int DaoOnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoVmSpace_LinkModule( vms, ns, "DaoSQL" );
	DaoNamespace_TypeDefine( ns, "int", "MySQL" );
	dao_type_mysql_datamodel = DaoNamespace_WrapType( ns, & DaoDataModel_Typer, 1 );
	dao_type_mysql_handle = DaoNamespace_WrapType( ns, & DaoHandle_Typer, 1 );
	return 0;
}
