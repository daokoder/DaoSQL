/*
// DaoSQL
// Database handling with mapping class instances to database table records.
// Copyright (C) 2008-2015, Limin Fu (http://fulimin.org).
*/
#include"stdlib.h"
#include"string.h"
#include"daoSQLite.h"


DaoSQLiteDB* DaoSQLiteDB_New()
{
	DaoSQLiteDB *self = malloc( sizeof(DaoSQLiteDB) );
	DaoSQLDatabase_Init( (DaoSQLDatabase*) self, DAO_SQLITE );
	self->db = NULL;
	self->stmt = NULL;
	return self;
}
void DaoSQLiteDB_Delete( DaoSQLiteDB *self )
{
	DaoSQLDatabase_Clear( (DaoSQLDatabase*) self );
	if( self->stmt ) sqlite3_finalize( self->stmt );
	if( self->db ) sqlite3_close( self->db );
	free( self );
}

static void DaoSQLiteDB_DataModel( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_DeleteTable( DaoProcess *proc, DaoValue *p[], int N );
//static void DaoSQLiteDB_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Query( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoSQLiteDB_DataModel,"Database<SQLite>( name: string, host='', user='', pwd='' ) => Database<SQLite>"},
	{ DaoSQLiteDB_CreateTable,  "CreateTable( self: Database<SQLite>, klass )" },
	{ DaoSQLiteDB_DeleteTable,  "DeleteTable( self: Database<SQLite>, klass )" },
//	{ DaoSQLiteDB_AlterTable,  "AlterTable( self:SQLDatabase<SQLite>, klass )" },
	{ DaoSQLiteDB_Insert,  "Insert( self: Database<SQLite>, object: @T, ... : @T ) => Handle<SQLite>" },
	{ DaoSQLiteDB_DeleteRow,"Delete( self: Database<SQLite>, object ) => Handle<SQLite>" },
	{ DaoSQLiteDB_Select, "Select( self: Database<SQLite>, object, ... ) => Handle<SQLite>" },
	{ DaoSQLiteDB_Update, "Update( self: Database<SQLite>, object, ... ) => Handle<SQLite>" },
	{ DaoSQLiteDB_Query,  "Query( self: Database<SQLite>, sql: string ) => bool" },
	{ NULL, NULL }
};

static DaoTypeBase DaoSQLiteDB_Typer = 
{ "Database<SQLite>", NULL, NULL, modelMeths, 
	{ & DaoSQLDatabase_Typer, 0 }, {0}, 
	(FuncPtrDel) DaoSQLiteDB_Delete, NULL };

static DaoType *dao_type_sqlite3_database = NULL;

DaoSQLiteHD* DaoSQLiteHD_New( DaoSQLiteDB *model )
{
	DaoSQLiteHD *self = malloc( sizeof(DaoSQLiteHD) );
	DaoSQLHandle_Init( (DaoSQLHandle*) self, (DaoSQLDatabase*) model );
	self->model = model;
	self->stmt = NULL;
	return self;
}
void DaoSQLiteHD_Delete( DaoSQLiteHD *self )
{
	DaoSQLHandle_Clear( (DaoSQLHandle*) self );
	if( self->stmt ) sqlite3_finalize( self->stmt );
	free( self );
}
static void DaoSQLiteHD_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteHD_Bind( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteHD_Query( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handleMeths[]=
{
	{ DaoSQLiteHD_Insert, "Insert( self: Handle<SQLite>, object: @T, ... : @T ) => Handle<SQLite>" },
	{ DaoSQLiteHD_Bind,   "Bind( self: Handle<SQLite>, value, index=0 ) => Handle<SQLite>" },
	{ DaoSQLiteHD_Query,  "Query( self: Handle<SQLite>, ... ) [] => bool" },
	{ DaoSQLiteHD_QueryOnce, "QueryOnce( self: Handle<SQLite>, ... ) => bool" },
	{ NULL, NULL }
};

static DaoTypeBase DaoSQLiteHD_Typer = 
{ "Handle<SQLite>", NULL, NULL, handleMeths, 
	{ & DaoSQLHandle_Typer, 0 }, {0},
	(FuncPtrDel) DaoSQLiteHD_Delete, NULL };

static DaoType *dao_type_sqlite3_handle = NULL;

static void DaoSQLiteDB_DataModel( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = DaoSQLiteDB_New();
	DString_Assign( model->base.name, p[0]->xString.value );
	DaoProcess_PutCdata( proc, model, dao_type_sqlite3_database );
	if( sqlite3_open( model->base.name->chars, & model->db ) ){
		DaoProcess_RaiseError( proc, NULL, sqlite3_errmsg( model->db ) );
		sqlite3_close( model->db );
		model->db = NULL;
	}
}
static void DaoSQLiteDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New();
	sqlite3_stmt *stmt = NULL;
	int rc = 0;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql );
	rc = sqlite3_prepare_v2( model->db, sql->chars, sql->size, & stmt, NULL );
	if( rc ) DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
	if( rc == 0 ){
		rc = sqlite3_step( stmt );
		if( rc > SQLITE_OK && rc < SQLITE_ROW )
			DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
	}
	DString_Delete( sql );
	sqlite3_finalize( stmt );
}
static void DaoSQLiteDB_DeleteTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New();
	sqlite3_stmt *stmt = NULL;
	int rc = 0;
	DaoSQLDatabase_DeleteTable( (DaoSQLDatabase*) model, klass, sql );
	rc = sqlite3_prepare_v2( model->db, sql->chars, sql->size, & stmt, NULL );
	if( rc ) DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
	if( rc == 0 ){
		rc = sqlite3_step( stmt );
		if( rc > SQLITE_OK && rc < SQLITE_ROW )
			DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
	}
	DString_Delete( sql );
	sqlite3_finalize( stmt );
}
static void DaoSQLiteDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.value;
	sqlite3_stmt *stmt = NULL;
	int k = 0;
	if( sqlite3_prepare_v2( model->db, sql->chars, sql->size, & stmt, NULL ) ){
		sqlite3_finalize( stmt );
		DaoProcess_PutBoolean( proc, 0 );
		DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
		return;
	}
	k = sqlite3_step( stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW ){
		sqlite3_finalize( stmt );
		DaoProcess_PutBoolean( proc, 0 );
		DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
		return;
	}
	sqlite3_finalize( stmt );
	DaoProcess_PutBoolean( proc, 1 );
}
static void DaoSQLiteHD_BindValue( DaoSQLiteHD *self, DaoValue *value, int index, DaoProcess *proc )
{
	DaoType *type = self->base.partypes[index-1];
	sqlite3 *db = self->model->db;
	sqlite3_stmt *stmt = self->stmt;
	int k = SQLITE_MISUSE;
	switch( value->type ){
	case DAO_NONE :
		k = sqlite3_bind_null( stmt, index );
		break;
	case DAO_INTEGER :
		if( type == dao_sql_type_bigint ){
			k = sqlite3_bind_int64( stmt, index, value->xInteger.value );
		}else{
			k = sqlite3_bind_int( stmt, index, value->xInteger.value );
		}
		break;
	case DAO_FLOAT   :
		k = sqlite3_bind_double( stmt, index, value->xFloat.value );
		break;
	case DAO_STRING  :
		k = sqlite3_bind_text( stmt, index, value->xString.value->chars, value->xString.value->size, SQLITE_TRANSIENT );
		break;
	case DAO_CINVALUE :
		if( value->xCinValue.cintype->vatype == dao_sql_type_date ){
			DaoTime *time = (DaoTime*) value->xCinValue.value;
			int days = _DTime_ToDay( time->time );
			k = sqlite3_bind_int( stmt, index, days );
		}else if( value->xCinValue.cintype->vatype == dao_sql_type_timestamp ){
			DaoTime *time = (DaoTime*) value->xCinValue.value;
			int64_t msecs = _DTime_ToMicroSeconds( time->time );
			k = sqlite3_bind_int64( stmt, index, msecs );
		}
		break;
	case DAO_CPOD :
		if( value->xCpod.ctype == dao_type_datetime ){
			DaoTime *time = (DaoTime*) value;
			int64_t msecs = _DTime_ToMicroSeconds( time->time );
			k = sqlite3_bind_int64( stmt, index, msecs );
		}
		break;
	default : break;
	}
	if( k ) DaoProcess_RaiseError( proc, NULL, sqlite3_errmsg( db ) );
}
static void DaoSQLiteDB_InsertObject( DaoProcess *proc, DaoSQLiteHD *handle, DaoObject *object )
{
	DaoValue *value;
	DaoClass *klass = object->defClass;
	DaoVariable **vars = klass->instvars->items.pVar;
	sqlite3 *db = handle->model->db;
	sqlite3_stmt *stmt = handle->stmt;
	char *tpname;
	int i, k, key = 0;

	sqlite3_reset( handle->stmt );
	for(i=1; i<klass->objDataName->size; i++){
		DaoType *type = DaoType_GetBaseType( vars[i]->dtype );
		tpname = type->name->chars;
		value = object->objValues[i];
		//fprintf( stderr, "%3i: %s %s\n", i, klass->objDataName->items.pString[i]->chars, tpname );
		if( strstr( tpname, "INTEGER_PRIMARY_KEY" ) == tpname ){
			key = i;
			sqlite3_bind_null( stmt, i );
			continue;
		}
		DaoSQLiteHD_BindValue( handle, value, i, proc );
	}
	k = sqlite3_step( handle->stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW )
		DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( db ) );

	//printf( "k = %i %i\n", k, sqlite3_insert_id( handle->model->db ) );
	if( key > 0 ) object->objValues[key]->xInteger.value = sqlite3_last_insert_rowid( db );
	sqlite3_reset( handle->stmt );
}
static void DaoSQLiteDB_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoSQLiteHD *handle = DaoSQLiteHD_New( model );
	DString *str = handle->base.sqlSource;
	int i;
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_sqlite3_handle, handle ) );
	if( DaoSQLHandle_PrepareInsert( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	if( sqlite3_prepare_v2( model->db, str->chars, str->size, & handle->stmt, NULL ) ){
		DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( model->db ) );
		return;
	}
	if( N == 2 && p[1]->type == DAO_CLASS ) return;
	for(i=1; i<N; ++i) DaoSQLiteDB_InsertObject( proc, handle, (DaoObject*) p[i] );
}
static void DaoSQLiteDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoSQLiteHD *handle = DaoSQLiteHD_New( model );
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_sqlite3_handle, handle ) );
	if( DaoSQLHandle_PrepareDelete( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoSQLiteDB_Select( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoSQLiteHD *handle = DaoSQLiteHD_New( model );
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_sqlite3_handle, handle ) );
	if( DaoSQLHandle_PrepareSelect( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
	//printf( "%s\n", handle->base.sqlSource->chars );
}
static void DaoSQLiteDB_Update( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoSQLiteHD *handle = DaoSQLiteHD_New( model );
	DaoProcess_PutValue( proc, (DaoValue*)DaoCdata_New( dao_type_sqlite3_handle, handle ) );
	if( DaoSQLHandle_PrepareUpdate( (DaoSQLHandle*) handle, proc, p, N ) ==0 ) return;
}
static void DaoSQLiteHD_Insert( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	int i;
	DaoProcess_PutValue( proc, p[0] );
	for(i=1; i<N; ++i){
		if( p[i]->type != DAO_OBJECT || p[i]->xObject.defClass != p[1]->xObject.defClass ){
			DaoProcess_RaiseError( proc, "Param", "" );
			return;
		}
		DaoSQLiteDB_InsertObject( proc, handle, (DaoObject*) p[i] );
	}
}
static void DaoSQLiteHD_Bind( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	DaoValue *value = p[1];
	sqlite3 *db = handle->model->db;
	sqlite3_stmt *stmt = handle->stmt;
	int k, index = p[2]->xInteger.value + 1; /* 1-based index for sqlite; */

	DaoProcess_PutValue( proc, p[0] );
	if( handle->base.executed ) sqlite3_reset( handle->stmt );
	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		if( sqlite3_prepare_v2( db, sql->chars, sql->size, & handle->stmt, NULL ) )
			DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( db ) );
		handle->base.prepared = 1;
	}
	stmt = handle->stmt;
	handle->base.executed = 0;
	if( index >= MAX_PARAM_COUNT ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	DaoSQLiteHD_BindValue( handle, value, index, proc );
}
static int DaoSQLiteHD_TryPrepare( DaoProcess *proc, DaoValue *p[], int N )
{
	int i;
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;

	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		if( sqlite3_prepare_v2( handle->model->db, sql->chars, sql->size, & handle->stmt, NULL ) ){
			DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( handle->model->db ) );
			return 0;
		}
		handle->base.prepared = 1;
		handle->base.executed = 0;
	}

	for(i=1; i<N; i++){
		if( p[i]->type == DAO_OBJECT ){
			if( p[i]->xObject.defClass == handle->base.classList->items.pClass[i-1] ) continue;
		}
		DaoProcess_RaiseError( proc, "Param", "need class instance(s)" );
		return 0;
	}
	return 1;
}
static void DaoSQLiteHD_Retrieve( DaoProcess *proc, DaoValue *p[], int N )
{
	daoint i, j, k, m, count;
	const unsigned char *txt;
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;

	for(i=1,k=0; i<N; i++){
		object = (DaoObject*) p[i];
		klass = object->defClass;
		m = handle->base.countList->items.pInt[i-1];
		for(j=1; j<m; j++){
			type = DaoType_GetBaseType( klass->instvars->items.pVar[j]->dtype );
			value = object->objValues[j];
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
			case DAO_INTEGER :
				if( type == dao_sql_type_bigint ){
					value->xInteger.value = sqlite3_column_int64( handle->stmt, k );
				}else{
					value->xInteger.value = sqlite3_column_int( handle->stmt, k );
				}
				break;
			case DAO_FLOAT   :
				value->xFloat.value = sqlite3_column_double( handle->stmt, k );
				break;
			case DAO_STRING  :
				DString_Clear( value->xString.value );
				txt = sqlite3_column_text( handle->stmt, k );
				count = sqlite3_column_bytes( handle->stmt, k );
				if( txt ) DString_AppendBytes( value->xString.value, (const char*)txt, count );
				break;
			case DAO_CINVALUE :
				if( value->xCinValue.cintype->vatype == dao_sql_type_date ){
					DaoTime *time = (DaoTime*) value->xCinValue.value;
					time->time = _DTime_FromDay( sqlite3_column_int( handle->stmt, k ) );
				}else if( value->xCinValue.cintype->vatype == dao_sql_type_timestamp ){
					DaoTime *time = (DaoTime*) value->xCinValue.value;
					time->time = _DTime_FromMicroSeconds( sqlite3_column_int64( handle->stmt, k ) );
				}
				break;
			case DAO_CPOD :
				if( value->xCpod.ctype == dao_type_datetime ){
					DaoTime *time = (DaoTime*) value;
					time->time = _DTime_FromMicroSeconds( sqlite3_column_int64( handle->stmt, k ) );
				}
				break;
			default : break;
			}
			k ++;
		}
	}
}
static void DaoSQLiteHD_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoVmCode *sect;
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	DaoValue *params[DAO_MAX_PARAM+1];
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	dao_integer *res = DaoProcess_PutBoolean( proc, 0 );
	const unsigned char *txt;
	daoint i, j, k = 0;
	daoint m, count, entry;

	if( DaoSQLiteHD_TryPrepare( proc, p, N ) == 0 ) return;

	sect = DaoProcess_InitCodeSection( proc, 1 );
	if( sect == NULL ) return;
	entry = proc->topFrame->entry;
	memcpy( params, p, N*sizeof(DaoValue*) );
	handle->base.stopQuery = 0;
	while(1){
		k = sqlite3_step( handle->stmt );
		handle->base.executed = 1;
		if( k > SQLITE_OK && k < SQLITE_ROW ){
			DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( handle->model->db ) );
			break;
		}
		if( sqlite3_data_count( handle->stmt ) ==0 ) break;

		DaoSQLiteHD_Retrieve( proc, params, N );

		proc->topFrame->entry = entry;
		DaoProcess_Execute( proc );
		if( proc->status == DAO_PROCESS_ABORTED ) break;
		*res += 1;
		if( handle->base.stopQuery ) break;
	}
	DaoProcess_PopFrame( proc );
	sqlite3_reset( handle->stmt );
}
static void DaoSQLiteHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	dao_integer *res = DaoProcess_PutBoolean( proc, 0 );
	int k;

	if( DaoSQLiteHD_TryPrepare( proc, p, N ) == 0 ) return;

	k = sqlite3_step( handle->stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW ){
		DaoProcess_RaiseError( proc, "Param", sqlite3_errmsg( handle->model->db ) );
	}else if( sqlite3_data_count( handle->stmt ) ){
		DaoSQLiteHD_Retrieve( proc, p, N );
		*res = 1;
	}
	sqlite3_reset( handle->stmt );
}

int DaoSqlite_OnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoMap *engines;
	DaoNamespace *sqlns = DaoVmSpace_LinkModule( vms, ns, "sql" );
	sqlns = DaoNamespace_GetNamespace( sqlns, "SQL" );
	dao_type_sqlite3_database = DaoNamespace_WrapType( sqlns, & DaoSQLiteDB_Typer, DAO_CDATA, 0 );
	dao_type_sqlite3_handle = DaoNamespace_WrapType( sqlns, & DaoSQLiteHD_Typer, DAO_CDATA, 0 );
	engines = (DaoMap*) DaoNamespace_FindData( sqlns, "Engines" );
	DaoMap_InsertChars( engines, "SQLite", (DaoValue*) dao_type_sqlite3_database );
	return 0;
}
