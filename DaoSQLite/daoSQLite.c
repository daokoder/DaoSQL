
/* DaoSQLite:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
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
//static void DaoSQLiteDB_AlterTable( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Insert( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_DeleteRow( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Select( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Update( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLiteDB_Query( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoSQLiteDB_DataModel,"SQLDatabase<SQLite>( name :string, host='', user='', pwd='' )=>SQLDatabase<SQLite>"},
	{ DaoSQLiteDB_CreateTable,  "CreateTable( self :SQLDatabase<SQLite>, klass )" },
//	{ DaoSQLiteDB_AlterTable,  "AlterTable( self:SQLDatabase<SQLite>, klass )" },
	{ DaoSQLiteDB_Insert,  "Insert( self :SQLDatabase<SQLite>, object :@T, ... :@T )=>SQLHandle<SQLite>" },
	{ DaoSQLiteDB_DeleteRow,"Delete( self :SQLDatabase<SQLite>, object )=>SQLHandle<SQLite>" },
	{ DaoSQLiteDB_Select, "Select( self :SQLDatabase<SQLite>, object, ... )=>SQLHandle<SQLite>" },
	{ DaoSQLiteDB_Update, "Update( self :SQLDatabase<SQLite>, object, ... )=>SQLHandle<SQLite>" },
	{ DaoSQLiteDB_Query,  "Query( self :SQLDatabase<SQLite>, sql :string )=>int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoSQLiteDB_Typer = 
{ "SQLDatabase<SQLite>", NULL, NULL, modelMeths, 
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
	{ DaoSQLiteHD_Insert, "Insert( self :SQLHandle<SQLite>, object :@T, ... :@T ) => int" },
	{ DaoSQLiteHD_Bind,   "Bind( self :SQLHandle<SQLite>, value, index=0 )=>SQLHandle<SQLite>" },
	{ DaoSQLiteHD_Query,  "Query( self :SQLHandle<SQLite>, ... ) [=>enum<continue,done>] => int" },
	{ DaoSQLiteHD_QueryOnce, "QueryOnce( self :SQLHandle<SQLite>, ... ) => int" },
	{ NULL, NULL }
};

static DaoTypeBase DaoSQLiteHD_Typer = 
{ "SQLHandle<SQLite>", NULL, NULL, handleMeths, 
	{ & DaoSQLHandle_Typer, 0 }, {0},
	(FuncPtrDel) DaoSQLiteHD_Delete, NULL };

static DaoType *dao_type_sqlite3_handle = NULL;

static void DaoSQLiteDB_DataModel( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = DaoSQLiteDB_New();
	DString_Assign( model->base.name, p[0]->xString.data );
	DString_ToMBS( model->base.name );
	DaoProcess_PutCdata( proc, model, dao_type_sqlite3_database );
	if( sqlite3_open( model->base.name->mbs, & model->db ) ){
		DaoProcess_RaiseException( proc, DAO_ERROR, sqlite3_errmsg( model->db ) );
		sqlite3_close( model->db );
		model->db = NULL;
	}
}
static void DaoSQLiteDB_CreateTable( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DaoClass *klass = (DaoClass*) p[1];
	DString *sql = DString_New(1);
	sqlite3_stmt *stmt = NULL;
	int rc = 0;
	DaoSQLDatabase_CreateTable( (DaoSQLDatabase*) model, klass, sql );
	rc = sqlite3_prepare_v2( model->db, sql->mbs, sql->size, & stmt, NULL );
	if( rc ) DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( model->db ) );
	if( rc == 0 ){
		rc = sqlite3_step( stmt );
		if( rc > SQLITE_OK && rc < SQLITE_ROW )
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( model->db ) );
	}
	DString_Delete( sql );
	sqlite3_finalize( stmt );
}
static void DaoSQLiteDB_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteDB *model = (DaoSQLiteDB*) p[0]->xCdata.data;
	DString *sql = p[1]->xString.data;
	sqlite3_stmt *stmt = NULL;
	int k = 0;
	DString_ToMBS( sql );
	if( sqlite3_prepare_v2( model->db, sql->mbs, sql->size, & stmt, NULL ) ){
		sqlite3_finalize( stmt );
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( model->db ) );
		return;
	}
	k = sqlite3_step( stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW ){
		sqlite3_finalize( stmt );
		DaoProcess_PutInteger( proc, 0 );
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( model->db ) );
		return;
	}
	sqlite3_finalize( stmt );
	DaoProcess_PutInteger( proc, 1 );
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
	for(i=1; i<klass->objDataName->size; i++){
		tpname = vars[i]->dtype->name->mbs;
		value = object->objValues[i];
		//fprintf( stderr, "%3i: %s %s\n", i, klass->objDataName->items.pString[i]->mbs, tpname );
		if( strstr( tpname, "INT_PRIMARY_KEY" ) == tpname ){
			key = i;
			sqlite3_bind_null( stmt, i );
			continue;
		}
		k = SQLITE_MISUSE;
		switch( value->type ){
		case DAO_NONE :
			k = sqlite3_bind_null( stmt, i );
			break;
		case DAO_INTEGER :
			k = sqlite3_bind_int( stmt, i, value->xInteger.value );
			break;
		case DAO_FLOAT   :
			k = sqlite3_bind_double( stmt, i, value->xFloat.value );
			break;
		case DAO_DOUBLE  :
			k = sqlite3_bind_double( stmt, i, value->xDouble.value );
			break;
		case DAO_STRING  :
			DString_ToMBS( value->xString.data );
			k = sqlite3_bind_text( stmt, i, value->xString.data->mbs, value->xString.data->size, SQLITE_TRANSIENT );
			break;
		default : break;
		}
		if( k ) DaoProcess_RaiseException( proc, DAO_ERROR, sqlite3_errmsg( db ) );
	}
	k = sqlite3_step( handle->stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW )
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( db ) );

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
	if( sqlite3_prepare_v2( model->db, str->mbs, str->size, & handle->stmt, NULL ) ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( model->db ) );
		return;
	}
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
	//printf( "%s\n", handle->base.sqlSource->mbs );
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
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
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
		if( sqlite3_prepare_v2( db, sql->mbs, sql->size, & handle->stmt, NULL ) )
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( db ) );
		handle->base.prepared = 1;
	}
	stmt = handle->stmt;
	handle->base.executed = 0;
	if( index >= MAX_PARAM_COUNT ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	k = SQLITE_MISUSE;
	switch( value->type ){
		case DAO_NONE :
			k = sqlite3_bind_null( stmt, index );
			break;
		case DAO_INTEGER :
			k = sqlite3_bind_int( stmt, index, value->xInteger.value );
			break;
		case DAO_FLOAT :
			k = sqlite3_bind_double( stmt, index, value->xFloat.value );
			break;
		case DAO_DOUBLE :
			k = sqlite3_bind_double( stmt, index, value->xDouble.value );
			break;
		case DAO_STRING :
			DString_ToMBS( value->xString.data );
			k = sqlite3_bind_text( stmt, index, value->xString.data->mbs, value->xString.data->size, SQLITE_TRANSIENT );
			break;
		default : break;
	}
	if( k ) DaoProcess_RaiseException( proc, DAO_ERROR, sqlite3_errmsg( db ) );
}
static int DaoSQLiteHD_TryPrepare( DaoProcess *proc, DaoValue *p[], int N )
{
	int i;
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;

	if( handle->base.prepared ==0 ){
		DString *sql = handle->base.sqlSource;
		if( sqlite3_prepare_v2( handle->model->db, sql->mbs, sql->size, & handle->stmt, NULL ) ){
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( handle->model->db ) );
			return 0;
		}
		handle->base.prepared = 1;
		handle->base.executed = 0;
	}

	for(i=1; i<N; i++){
		if( p[i]->type == DAO_OBJECT ){
			if( p[i]->xObject.defClass == handle->base.classList->items.pClass[i-1] ) continue;
		}
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "need class instance(s)" );
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
			type = klass->instvars->items.pVar[j]->dtype;
			value = object->objValues[j];
			if( value == NULL || value->type != type->tid ){
				DaoValue_Move( type->value, & object->objValues[j], type );
				value = object->objValues[j];
			}
			switch( type->tid ){
			case DAO_INTEGER :
				if( sizeof(daoint) == 4 ){
					value->xInteger.value = sqlite3_column_int( handle->stmt, k );
				}else{
					value->xInteger.value = sqlite3_column_int64( handle->stmt, k );
				}
				break;
			case DAO_FLOAT   :
				value->xFloat.value = sqlite3_column_double( handle->stmt, k );
				break;
			case DAO_DOUBLE  :
				value->xDouble.value = sqlite3_column_double( handle->stmt, k );
				break;
			case DAO_STRING  :
				DString_Clear( value->xString.data );
				txt = sqlite3_column_text( handle->stmt, k );
				count = sqlite3_column_bytes( handle->stmt, k );
				if( txt ) DString_AppendDataMBS( value->xString.data, (const char*)txt, count );
				break;
			default : break;
			}
			k ++;
		}
	}
}
static void DaoSQLiteHD_Query( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoObject *object;
	DaoClass  *klass;
	DaoType *type;
	DaoValue *value;
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	DaoVmCode *sect = DaoGetSectionCode( proc->activeCode );
	daoint *res = DaoProcess_PutInteger( proc, 0 );
	const unsigned char *txt;
	daoint i, j, k = 0;
	daoint m, count, entry;

	if( DaoSQLiteHD_TryPrepare( proc, p, N ) == 0 ) return;

	if( sect == NULL ) return;
	if( DaoProcess_PushSectionFrame( proc ) == NULL ) return;
	entry = proc->topFrame->entry;
	DaoProcess_AcquireCV( proc );

	while(1){
		k = sqlite3_step( handle->stmt );
		handle->base.executed = 1;
		if( k > SQLITE_OK && k < SQLITE_ROW ){
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( handle->model->db ) );
			break;
		}
		if( sqlite3_data_count( handle->stmt ) ==0 ) break;

		DaoSQLiteHD_Retrieve( proc, p, N );

		proc->topFrame->entry = entry;
		DaoProcess_Execute( proc );
		if( proc->status == DAO_PROCESS_ABORTED ) break;
		*res += 1;
		value = proc->stackValues[0];
		if( value == NULL || value->type != DAO_ENUM || value->xEnum.value != 0 ) break;
	}

	DaoProcess_ReleaseCV( proc );
	DaoProcess_PopFrame( proc );
	sqlite3_reset( handle->stmt );
}
static void DaoSQLiteHD_QueryOnce( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLiteHD *handle = (DaoSQLiteHD*) p[0]->xCdata.data;
	daoint *res = DaoProcess_PutInteger( proc, 0 );
	int k;

	if( DaoSQLiteHD_TryPrepare( proc, p, N ) == 0 ) return;

	k = sqlite3_step( handle->stmt );
	if( k > SQLITE_OK && k < SQLITE_ROW ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, sqlite3_errmsg( handle->model->db ) );
	}else if( sqlite3_data_count( handle->stmt ) ){
		DaoSQLiteHD_Retrieve( proc, p, N );
		*res = 1;
	}
	sqlite3_reset( handle->stmt );
}

int DaoOnLoad( DaoVmSpace *vms, DaoNamespace *ns )
{
	DaoVmSpace_LinkModule( vms, ns, "DaoSQL" );
	DaoNamespace_TypeDefine( ns, "int", "SQLite" );
	dao_type_sqlite3_database = DaoNamespace_WrapType( ns, & DaoSQLiteDB_Typer, 1 );
	dao_type_sqlite3_handle = DaoNamespace_WrapType( ns, & DaoSQLiteHD_Typer, 1 );
	return 0;
}
