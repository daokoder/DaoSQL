
/* DaoSQL:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
 */
#include"stdlib.h"
#include"string.h"
#include"daoSQL.h"

void DaoSQLDatabase_Init( DaoSQLDatabase *self )
{
	self->dataClass = DMap_New(0,0);
	self->name = DString_New(1);
	self->host = DString_New(1);
	self->user = DString_New(1);
	self->password = DString_New(1);
	DString_SetMBS( self->name, "unnamed" );
	DString_SetMBS( self->host, "localhost" );
	DString_SetMBS( self->user, "root" );
}
void DaoSQLDatabase_Clear( DaoSQLDatabase *self )
{
	DString_Delete( self->name );
	DString_Delete( self->host );
	DString_Delete( self->user );
	DString_Delete( self->password );
	DMap_Delete( self->dataClass );
}

static void DaoSQLDatabase_SetName( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_SetHost( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_SetUser( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_SetPassword( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_GetName( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_GetHost( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_GetUser( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLDatabase_GetPassword( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem modelMeths[]=
{
	{ DaoSQLDatabase_SetName,      "SetName( self :SQLDatabase, name :string )" },
	{ DaoSQLDatabase_GetName,      "GetName( self :SQLDatabase) => string" },
	{ DaoSQLDatabase_SetHost,      "SetHost( self :SQLDatabase, host :string )" },
	{ DaoSQLDatabase_GetHost,      "GetHost( self :SQLDatabase) => string" },
	{ DaoSQLDatabase_SetUser,      "SetUser( self :SQLDatabase, user :string )" },
	{ DaoSQLDatabase_GetUser,      "GetUser( self :SQLDatabase) => string" },
	{ DaoSQLDatabase_SetPassword,  "SetPassword( self :SQLDatabase, pwd :string )" },
	{ DaoSQLDatabase_GetPassword,  "GetPassword( self :SQLDatabase) => string" },
	{ NULL, NULL }
};

DaoTypeBase DaoSQLDatabase_Typer = 
{ "SQLDatabase<>", NULL, NULL, modelMeths, {0}, {0}, NULL, NULL };

static void DaoSQLDatabase_SetName( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->name, p[1]->xString.data );
}
static void DaoSQLDatabase_SetHost( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->host, p[1]->xString.data );
}
static void DaoSQLDatabase_SetUser( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->user, p[1]->xString.data );
}
static void DaoSQLDatabase_SetPassword( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->password, p[1]->xString.data );
}
static void DaoSQLDatabase_GetName( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DaoProcess_PutString( proc, self->name );
}
static void DaoSQLDatabase_GetHost( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DaoProcess_PutString( proc, self->host );
}
static void DaoSQLDatabase_GetUser( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DaoProcess_PutString( proc, self->user );
}
static void DaoSQLDatabase_GetPassword( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DaoProcess_PutString( proc, self->password );
}

void DaoSQLHandle_Init( DaoSQLHandle *self )
{
	int i;
	self->sqlSource = DString_New(1);
	self->buffer = DString_New(1);
	self->classList = DArray_New(0);
	self->countList = DArray_New(0);
	self->setCount = 0;
	self->boolCount = 0;
	self->prepared = 0;
	self->executed = 0;
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		self->pardata[i] = DString_New(1);
		self->resdata[i] = DString_New(1);
		DString_Resize( self->pardata[i], MIN_DATA_SIZE );
		DString_Resize( self->resdata[i], MIN_DATA_SIZE );
		if( i ==0 ) DString_Resize( self->resdata[i], MAX_DATA_SIZE );
	}
}
void DaoSQLHandle_Clear( DaoSQLHandle *self )
{
	int i;
	/* it appears to close all other stmt associated with the connection too! */
	/* mysql_stmt_close( self->stmt ); */
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		DString_Delete( self->pardata[i] );
		DString_Delete( self->resdata[i] );
	}
	DString_Delete( self->sqlSource );
	DString_Delete( self->buffer );
	DArray_Delete( self->classList );
	DArray_Delete( self->countList );
}
static void DaoSQLHandle_SQLString( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Set( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Where( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Add( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_EQ( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_NE( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_GT( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_GE( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_LT( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_LE( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_IN( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_OR( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_And( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Not( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_LBrace( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_RBrace( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Match( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Sort( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Range( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handlerMeths[]=
{
	{ DaoSQLHandle_SQLString, "sqlstring( self :@SQLHandle )=>string" },
	{ DaoSQLHandle_Where,  "Where( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Set, "Set( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_Add, "Add( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_EQ, "EQ( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_NE, "NE( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_GT, "GT( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_GE, "GE( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_LT, "LT( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_LE, "LE( self :@SQLHandle, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_Set,"Set( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_Add,"Add( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_EQ, "EQ( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_NE, "NE( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_GT, "GT( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_GE, "GE( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_LT, "LT( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_LE, "LE( self :@SQLHandle, table :class, field :string, value:=none )=>@SQLHandle" },
	{ DaoSQLHandle_IN, "In( self :@SQLHandle, field :string, values={} )=>@SQLHandle" },
	{ DaoSQLHandle_IN, "In( self :@SQLHandle, table :class, field :string, values={} )=>@SQLHandle" },
	{ DaoSQLHandle_OR,"Or( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_And, "And( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Not,     "Not( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_LBrace,  "LBrace( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_RBrace,  "RBrace( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Match,   "Match( self :@SQLHandle, table1 :class, table2 :class, field1='', field2='' )=>@SQLHandle" },
	{ DaoSQLHandle_Sort,  "Sort( self :@SQLHandle, field :string, desc=0 )=>@SQLHandle" },
	{ DaoSQLHandle_Sort,  "Sort( self :@SQLHandle, table :class, field :string, desc=0 )=>@SQLHandle" },
	{ DaoSQLHandle_Range, "Range( self :@SQLHandle, limit :int, offset=0 )=>@SQLHandle" },
	{ NULL, NULL }
};

DaoTypeBase DaoSQLHandle_Typer = 
{ "SQLHandle<>", NULL, NULL, handlerMeths, {0}, {0}, NULL, NULL };

static DString* DaoSQLDatabase_TableName( DaoClass *klass )
{
	DString *name = DString_New(1);
	DNode *node;
	int id=0, st=0;
	DString_SetMBS( name, "__TABLE_NAME__" );
	node = DMap_Find( klass->lookupTable, name );
	DString_Delete( name );
	if( node ) st = LOOKUP_ST( node->value.pInt );
	if( node ) id = LOOKUP_ID( node->value.pInt );
	if( node && st == DAO_CLASS_CONSTANT && klass->constants->items.pConst[id]->value->type == DAO_STRING )
		return klass->constants->items.pConst[id]->value->xString.data;
	return klass->className;
}
void DaoSQLDatabase_CreateTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql, int tp )
{
	DaoVariable **vars;
	DString **names;
	DString *tabname = NULL;
	DNode *node;
	char *tpname;
	int i;
	if( tp == DAO_SQLITE ){
		DString_SetMBS( sql, "__SQLITE_TABLE_PROPERTY__" );
	}else if( tp == DAO_MYSQL ){
		DString_SetMBS( sql, "__MYSQL_TABLE_PROPERTY__" );
	}
	node = DMap_Find( klass->lookupTable, sql );
	DString_Clear( sql );
	names = klass->objDataName->items.pString;
	vars = klass->instvars->items.pVar;
	tabname = DaoSQLDatabase_TableName( klass );
	DString_AppendMBS( sql, "CREATE TABLE " );
	DString_Append( sql, tabname );
	DString_AppendMBS( sql, "(" );
	for(i=1; i<klass->objDataName->size; i++){
		if( i >1 ) DString_AppendMBS( sql, "," );
		tpname = vars[i]->dtype->name->mbs;
		DString_AppendMBS( sql, "\n" );
		DString_Append( sql, names[i] );
		DString_AppendMBS( sql, "  " );
		if( strcmp( tpname, "INT_PRIMARY_KEY" ) ==0 ){
			DString_AppendMBS( sql, "INTEGER PRIMARY KEY" );
		}else if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ){
			if( tp == DAO_SQLITE )
				DString_AppendMBS( sql, "INTEGER PRIMARY KEY AUTOINCREMENT" );
			else
				DString_AppendMBS( sql, "INTEGER PRIMARY KEY AUTO_INCREMENT" );
		}else if( strstr( tpname, "CHAR" ) == tpname ){
			DString_AppendMBS( sql, "CHAR(" );
			DString_AppendMBS( sql, tpname+4 );
			DString_AppendMBS( sql, ")" );
		}else if( strstr( tpname, "VARCHAR" ) == tpname ){
			DString_AppendMBS( sql, "VARCHAR(" );
			DString_AppendMBS( sql, tpname+7 );
			DString_AppendMBS( sql, ")" );
		}else{
			DString_AppendMBS( sql, tpname );
		}
	}
	if( node && LOOKUP_ST( node->value.pInt ) == DAO_CLASS_CONSTANT ){
		DaoValue *val = klass->constants->items.pConst[ LOOKUP_ID( node->value.pInt ) ]->value;
		if( val->type == DAO_STRING ){
			DString_AppendMBS( sql, ",\n" );
			DString_Append( sql, val->xString.data );
		}
	}
	DString_AppendMBS( sql, "\n);\n" );
}
int DaoSQLHandle_PrepareInsert( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoObject *object;
	DaoClass  *klass;
	DString *str = self->sqlSource;
	DString *tabname = NULL;
	int i;
	if( p[1]->type != DAO_LIST && p[1]->type != DAO_OBJECT ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return 0;
	}
	if( p[1]->type == DAO_LIST ){
		if( p[1]->xList.items.size ==0 ) return 0;
		for( i=0; i<p[1]->xList.items.size; i++ ){
			if( p[1]->xList.items.items.pValue[i]->type != DAO_OBJECT ){
				DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
				return 0;
			}
		}
		klass = p[1]->xList.items.items.pValue[0]->xObject.defClass;
		for( i=0; i<p[1]->xList.items.size; i++ ){
			if( p[1]->xList.items.items.pValue[i]->type != DAO_OBJECT
					|| p[1]->xList.items.items.pValue[0]->xObject.defClass != klass ){
				DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
				return 0;
			}
		}
	}else{
		object = (DaoObject*) p[1];
		klass = object->defClass;
	}
	tabname = DaoSQLDatabase_TableName( klass );
	DArray_PushBack( self->classList, klass );
	DString_AppendMBS( self->sqlSource, "INSERT INTO " );
	DString_Append( self->sqlSource, tabname );
	DString_AppendMBS( self->sqlSource, "(" );
	for(i=1; i<klass->objDataName->size; i++){
		if( i >1 ) DString_AppendMBS( self->sqlSource, "," );
		DString_Append( self->sqlSource, klass->objDataName->items.pString[i] );
	}
	DString_AppendMBS( self->sqlSource, ") VALUES(" );
	for(i=1; i<klass->objDataName->size; i++){
		if( i >1 ) DString_AppendMBS( self->sqlSource, "," );
		DString_AppendMBS( self->sqlSource, "?" );
	}
	DString_AppendMBS( self->sqlSource, ");" );
	//printf( "%s\n", self->sqlSource->mbs );
	return 1;
}
int DaoSQLHandle_PrepareDelete( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *str = self->sqlSource;
	DString *tabname = NULL;
	int i;
	if( p[1]->type != DAO_CLASS ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return 0;
	}
	klass = (DaoClass*) p[1];
	tabname = DaoSQLDatabase_TableName( klass );
	DArray_PushBack( self->classList, klass );
	DString_AppendMBS( self->sqlSource, "DELETE FROM " );
	DString_Append( self->sqlSource, tabname );
	DString_AppendMBS( self->sqlSource, " " );
	return 1;
}
int DaoSQLHandle_PrepareSelect( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *tabname = NULL;
	int i, j, m, nclass = 0;
	DString_AppendMBS( self->sqlSource, "SELECT " );
	for(i=1; i<N; i++) if( p[i]->type == DAO_CLASS ) nclass ++;
	for(i=1; i<N; i++){
		if( p[i]->type != DAO_CLASS ){
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
			return 0;
		}
		klass = (DaoClass*) p[i];
		tabname = DaoSQLDatabase_TableName( klass );
		m = klass->objDataName->size;
		if( i+1 < N && p[i+1]->type == DAO_INTEGER ){
			i ++;
			if( p[i]->xInteger.value + 1 < m ) m = p[i]->xInteger.value + 1;
		}
		DArray_PushBack( self->classList, klass );
		DArray_PushBack( self->countList, (void*)(size_t) m );
		if( m == 1 ) continue;
		if( self->classList->size >1 ) DString_AppendMBS( self->sqlSource, "," );
		for(j=1; j<m; j++){
			if( j >1 ) DString_AppendMBS( self->sqlSource, "," );
			if( nclass >1 ){
				DString_Append( self->sqlSource, tabname );
				DString_AppendMBS( self->sqlSource, "." );
			}
			DString_Append( self->sqlSource, klass->objDataName->items.pString[j] );
		}
	}
	DString_AppendMBS( self->sqlSource, " FROM " );
	for(i=1; i<N; i++){
		if( p[i]->type == DAO_INTEGER ) continue;
		if( i+1<N && p[i+1]->type == DAO_INTEGER && p[i+1]->xInteger.value ==0 ) continue;
		klass = (DaoClass*) p[i];
		tabname = DaoSQLDatabase_TableName( klass );
		if( i >1 ) DString_AppendMBS( self->sqlSource, "," );
		DString_Append( self->sqlSource, tabname );
	}
	//printf( "%s\n", self->sqlSource->mbs );
	return 1;
}
int DaoSQLHandle_PrepareUpdate( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *tabname = NULL;
	int i, j;
	DString_AppendMBS( self->sqlSource, "UPDATE " );
	for(i=1; i<N; i++){
		if( i >1 ) DString_AppendMBS( self->sqlSource, "," );
		klass = (DaoClass*) p[i];
		DArray_PushBack( self->classList, klass );
		tabname = DaoSQLDatabase_TableName( klass );
		DString_Append( self->sqlSource, tabname );
	}
	DString_AppendMBS( self->sqlSource, " SET " );
	return 1;
}
static void DaoSQLHandle_SQLString( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutString( proc, handler->sqlSource );
}
static void DaoSQLHandle_SetAdd( DaoProcess *proc, DaoValue *p[], int N, int add )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *fname = DString_New(1);
	DString *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *value = p[2];
	DaoProcess_PutValue( proc, p[0] );
	if( handler->setCount ) DString_AppendMBS( handler->sqlSource, ", " );
	DString_Assign( fname, field->xString.data );
	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( fname, tabname );
		DString_AppendMBS( fname, "." );
	}
	DString_Append( handler->sqlSource, fname );
	DString_AppendMBS( handler->sqlSource, "=" );
	if( add ){
		DString_Append( handler->sqlSource, fname );
		DString_AppendMBS( handler->sqlSource, "+ " );
	}
	if( N >2 ){
		DaoValue_GetString( value, field->xString.data );
		if( value->type == DAO_STRING ) DString_AppendMBS( handler->sqlSource, "\"" );
		DString_Append( handler->sqlSource, field->xString.data );
		if( value->type == DAO_STRING ) DString_AppendMBS( handler->sqlSource, "\"" );
	}else{
		DString_AppendMBS( handler->sqlSource, "?" );
	}
	handler->setCount ++;
	DString_Delete( fname );
}
static void DaoSQLHandle_Set( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_SetAdd( proc, p, N, 0 );
}
static void DaoSQLHandle_Add( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_SetAdd( proc, p, N, 1 );
}
static void DaoSQLHandle_Where( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " WHERE " );
}
static void DaoSQLHandle_Operator( DaoProcess *proc, DaoValue *p[], int N, char *op )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *value = p[2];
	DaoProcess_PutValue( proc, p[0] );
	if( handler->boolCount ) DString_AppendMBS( handler->sqlSource, " AND " );
	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( handler->sqlSource, tabname );
		DString_AppendMBS( handler->sqlSource, "." );
	}
	DString_Append( handler->sqlSource, field->xString.data );
	DString_AppendMBS( handler->sqlSource, op );

	if( value->type ){
		DaoValue_GetString( value, field->xString.data );
		if( value->type == DAO_STRING ) DString_AppendMBS( handler->sqlSource, "\"" );
		DString_Append( handler->sqlSource, field->xString.data );
		if( value->type == DAO_STRING ) DString_AppendMBS( handler->sqlSource, "\"" );
	}else{
		DString_AppendMBS( handler->sqlSource, "?" );
	}
	handler->boolCount ++;
	//fprintf( stderr, "%s\n", handler->sqlSource->mbs );
}
static void DaoSQLHandle_EQ( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, "=" );
}
static void DaoSQLHandle_NE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, "!=" );
}
static void DaoSQLHandle_GT( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, ">" );
}
static void DaoSQLHandle_GE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, ">=" );
}
static void DaoSQLHandle_LT( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, "<" );
}
static void DaoSQLHandle_LE( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle_Operator( proc, p, N, "<=" );
}
static void DaoSQLHandle_IN( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *table = NULL;
	DArray *values;
	DaoValue *val;
	int i, fid=1, vid = 2;
	DaoProcess_PutValue( proc, p[0] );
	if( p[1]->type == DAO_CLASS ){
		fid = 2;  vid = 3;
		table = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
	}
	if( p[vid]->type != DAO_LIST || p[vid]->xList.items.size ==0 ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "need non-empty list" );
		return;
	}
	values = & p[vid]->xList.items;
	DString_AppendMBS( handler->sqlSource, " " );
	if( table ){
		DString_Append( handler->sqlSource, table );
		DString_AppendMBS( handler->sqlSource, "." );
	}
	DString_Append( handler->sqlSource, p[fid]->xString.data );
	DString_AppendMBS( handler->sqlSource, " IN (" );
	for(i=0; i<values->size; i++){
		if( i ) DString_AppendMBS( handler->sqlSource, ", " );
		val = values->items.pValue[i];
		if( val->type ==0 ){
			DString_AppendMBS( handler->sqlSource, "NULL" );
		}else if( val->type <= DAO_DOUBLE ){
			DaoValue_GetString( val, handler->buffer );
			DString_Append( handler->sqlSource, handler->buffer );
		}else if( val->type == DAO_STRING ){
			DString_AppendMBS( handler->sqlSource, "'" );
			DString_Append( handler->sqlSource, val->xString.data );
			DString_AppendMBS( handler->sqlSource, "'" );
		}else{
			DaoProcess_RaiseException( proc, DAO_ERROR, "invalid value in list" );
			break;
		}
	}
	DString_AppendMBS( handler->sqlSource, " ) " );
	handler->boolCount ++;
	//printf( "%s\n", handler->sqlSource->mbs );
}
static void DaoSQLHandle_OR( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " OR " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_And( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " AND " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_Not( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " NOT " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_LBrace( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, "(" );
	handler->boolCount = 0;
}
static void DaoSQLHandle_RBrace( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, ")" );
}
static void DaoSQLHandle_Match( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoClass *c1, *c2;
	DString *tabname1 = NULL;
	DString *tabname2 = NULL;
	int i, k=0;
	DaoProcess_PutValue( proc, p[0] );
	if( p[1]->type != DAO_CLASS || p[2]->type != DAO_CLASS ){
		DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
		return;
	}
	c1 = (DaoClass*) p[1];
	c2 = (DaoClass*) p[2];
	tabname1 = DaoSQLDatabase_TableName( c1 );
	tabname2 = DaoSQLDatabase_TableName( c2 );
	if( handler->boolCount ) DString_AppendMBS( handler->sqlSource, " AND " );
	handler->boolCount ++;
	if( N == 3 ){
		for(i=1; i<c1->objDataName->size; i++){
			DNode *node = DMap_Find( c2->lookupTable, c1->objDataName->items.pVoid[i] );
			if( node && LOOKUP_ST( node->value.pInt ) == DAO_OBJECT_VARIABLE ){
				if( k >0 ) DString_AppendMBS( handler->sqlSource, " AND " );
				DString_Append( handler->sqlSource, tabname1 );
				DString_AppendMBS( handler->sqlSource, "." );
				DString_Append( handler->sqlSource, c1->objDataName->items.pString[i] );
				DString_AppendMBS( handler->sqlSource, "=" );
				DString_Append( handler->sqlSource, tabname2 );
				DString_AppendMBS( handler->sqlSource, "." );
				DString_Append( handler->sqlSource, c1->objDataName->items.pString[i] );
				k ++;
			}
		}
	}else{
		DString *field1 = p[3]->xString.data;
		DString *field2 = p[3]->xString.data;
		DNode *node1, *node2;
		if( N > 4 ) field2 = p[4]->xString.data;
		DString_ToMBS( field1 ); /* c1->lookupTable is a hash with MBS keys; */
		DString_ToMBS( field2 );
		node1 = DMap_Find( c1->lookupTable, field1 );
		node2 = DMap_Find( c2->lookupTable, field2 );
		if( node1 && LOOKUP_ST( node1->value.pInt ) == DAO_OBJECT_VARIABLE 
				&& node2 && LOOKUP_ST( node2->value.pInt ) == DAO_OBJECT_VARIABLE ){
			DString_Append( handler->sqlSource, tabname1 );
			DString_AppendMBS( handler->sqlSource, "." );
			DString_Append( handler->sqlSource, field1 );
			DString_AppendMBS( handler->sqlSource, "=" );
			DString_Append( handler->sqlSource, tabname2 );
			DString_AppendMBS( handler->sqlSource, "." );
			DString_Append( handler->sqlSource, field2 );
		}else{
			DaoProcess_RaiseException( proc, DAO_ERROR_PARAM, "" );
			return;
		}
	}
}
static void DaoSQLHandle_Sort( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	int desc = 0;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " ORDER BY " );
	if( p[2]->type == DAO_INTEGER ){
		desc = p[2]->xInteger.value;
		DString_Append( handler->sqlSource, p[1]->xString.data );
	}else{
		if( p[1]->type == DAO_CLASS ){
			DString *tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
			DString_Append( handler->sqlSource, tabname );
			DString_AppendMBS( handler->sqlSource, "." );
		}
		DString_Append( handler->sqlSource, p[2]->xString.data );
		desc = p[3]->xInteger.value;
	}
	if( desc ){
		DString_AppendMBS( handler->sqlSource, " DESC " );
	}else{
		DString_AppendMBS( handler->sqlSource, " ASC " );
	}
}
static void DaoSQLHandle_Range( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendMBS( handler->sqlSource, " LIMIT " );
	DaoValue_GetString( p[1], handler->buffer );
	DString_Append( handler->sqlSource, handler->buffer );
	if( N >2 ){
		DString_AppendMBS( handler->sqlSource, " OFFSET " );
		DaoValue_GetString( p[2], handler->buffer );
		DString_Append( handler->sqlSource, handler->buffer );
	}
}

int DaoOnLoad( DaoVmSpace * vms, DaoNamespace *ns )
{
	char *lang = getenv( "DAO_HELP_LANG" );
	DaoTypeBase *typers[] = { & DaoSQLDatabase_Typer, & DaoSQLHandle_Typer, NULL };

	DaoNamespace_TypeDefine( ns, "int", "INT" );
	DaoNamespace_TypeDefine( ns, "int", "TINYINT" );
	DaoNamespace_TypeDefine( ns, "int", "SMALLINT" );
	DaoNamespace_TypeDefine( ns, "int", "MEDIUMINT" );
	DaoNamespace_TypeDefine( ns, "int", "INT_PRIMARY_KEY" );
	DaoNamespace_TypeDefine( ns, "int", "INT_PRIMARY_KEY_AUTO_INCREMENT" );
	DaoNamespace_TypeDefine( ns, "double", "BIGINT" );
	DaoNamespace_TypeDefine( ns, "string", "TEXT" );
	DaoNamespace_TypeDefine( ns, "string", "MEDIUMTEXT" );
	DaoNamespace_TypeDefine( ns, "string", "LONGTEXT" );
	DaoNamespace_TypeDefine( ns, "string", "BLOB" );
	DaoNamespace_TypeDefine( ns, "string", "MEDIUMBLOB" );
	DaoNamespace_TypeDefine( ns, "string", "LONGBLOB" );
	DaoNamespace_TypeDefine( ns, "string", "CHAR10" );
	DaoNamespace_TypeDefine( ns, "string", "CHAR20" );
	DaoNamespace_TypeDefine( ns, "string", "CHAR50" );
	DaoNamespace_TypeDefine( ns, "string", "CHAR100" );
	DaoNamespace_TypeDefine( ns, "string", "CHAR200" );
	DaoNamespace_TypeDefine( ns, "string", "VARCHAR10" );
	DaoNamespace_TypeDefine( ns, "string", "VARCHAR20" );
	DaoNamespace_TypeDefine( ns, "string", "VARCHAR50" );
	DaoNamespace_TypeDefine( ns, "string", "VARCHAR100" );
	DaoNamespace_TypeDefine( ns, "string", "VARCHAR200" );

	DaoNamespace_WrapTypes( ns, typers );

	if( lang ){
		char fname[100] = "help_module_official_sql_";
		strcat( fname, lang );
		DaoVmSpace_Load( vms, fname );
	}
	return 0;
}
