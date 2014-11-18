
/* DaoSQL:
 * Database handling with mapping class instances to database table records.
 * Copyright (C) 2008-2012, Limin Fu (phoolimin@gmail.com).
 */
#include"stdlib.h"
#include"string.h"
#include"daoSQL.h"

void DaoSQLDatabase_Init( DaoSQLDatabase *self, int type )
{
	self->type = type;
	self->dataClass = DMap_New(0,0);
	self->name = DString_New(1);
	self->host = DString_New(1);
	self->user = DString_New(1);
	self->password = DString_New(1);
	DString_SetChars( self->name, "unnamed" );
	DString_SetChars( self->host, "localhost" );
	DString_SetChars( self->user, "root" );
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
	DString_Assign( self->name, p[1]->xString.value );
}
static void DaoSQLDatabase_SetHost( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->host, p[1]->xString.value );
}
static void DaoSQLDatabase_SetUser( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->user, p[1]->xString.value );
}
static void DaoSQLDatabase_SetPassword( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLDatabase *self = (DaoSQLDatabase*) DaoValue_TryGetCdata( p[0] );
	DString_Assign( self->password, p[1]->xString.value );
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

void DaoSQLHandle_Init( DaoSQLHandle *self, DaoSQLDatabase *db )
{
	int i;
	self->database = db;
	self->sqlSource = DString_New(1);
	self->buffer = DString_New(1);
	self->classList = DList_New(0);
	self->countList = DList_New(0);
	self->setCount = 0;
	self->boolCount = 0;
	self->prepared = 0;
	self->executed = 0;
	self->paramCount = 0;
	self->stopQuery = 0;
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
	DList_Delete( self->classList );
	DList_Delete( self->countList );
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
static void DaoSQLHandle_Stop( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handlerMeths[]=
{
	{ DaoSQLHandle_SQLString, "sqlstring( self :@SQLHandle )=>string" },
	{ DaoSQLHandle_Where,  "Where( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Set, "Set( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_Add, "Add( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_EQ, "EQ( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_NE, "NE( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_GT, "GT( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_GE, "GE( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_LT, "LT( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_LE, "LE( self :@SQLHandle, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_Set,"Set( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_Add,"Add( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_EQ, "EQ( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_NE, "NE( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_GT, "GT( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_GE, "GE( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_LT, "LT( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_LE, "LE( self :@SQLHandle, table :class, field :string, value :any=none )=>@SQLHandle" },
	{ DaoSQLHandle_IN, "In( self :@SQLHandle, field :string, values:list<@T>={} )=>@SQLHandle" },
	{ DaoSQLHandle_IN, "In( self :@SQLHandle, table :class, field :string, values:list<@T>={} )=>@SQLHandle" },
	{ DaoSQLHandle_OR,"Or( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_And, "And( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Not,     "Not( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_LBrace,  "LBrace( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_RBrace,  "RBrace( self :@SQLHandle )=>@SQLHandle" },
	{ DaoSQLHandle_Match,   "Match( self :@SQLHandle, table1 :class, table2 :class, field1=\"\", field2=\"\" )=>@SQLHandle" },
	{ DaoSQLHandle_Sort,  "Sort( self :@SQLHandle, field :string, desc=0 )=>@SQLHandle" },
	{ DaoSQLHandle_Sort,  "Sort( self :@SQLHandle, table :class, field :string, desc=0 )=>@SQLHandle" },
	{ DaoSQLHandle_Range, "Range( self :@SQLHandle, limit :int, offset=0 )=>@SQLHandle" },
	{ DaoSQLHandle_Stop,  "Stop( self :@SQLHandle )" },
	{ NULL, NULL }
};

DaoTypeBase DaoSQLHandle_Typer = 
{ "SQLHandle<>", NULL, NULL, handlerMeths, {0}, {0}, NULL, NULL };

DString* DaoSQLDatabase_TableName( DaoClass *klass )
{
	DString *name = DString_New(1);
	DNode *node;
	int id=0, st=0;
	DString_SetChars( name, "__TABLE_NAME__" );
	node = DMap_Find( klass->lookupTable, name );
	DString_Delete( name );
	if( node ) st = LOOKUP_ST( node->value.pInt );
	if( node ) id = LOOKUP_ID( node->value.pInt );
	if( node && st == DAO_CLASS_CONSTANT && klass->constants->items.pConst[id]->value->type == DAO_STRING )
		return klass->constants->items.pConst[id]->value->xString.value;
	return klass->className;
}
void DaoSQLDatabase_CreateTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql )
{
	DaoVariable **vars;
	DString **names;
	DString *tabname = NULL;
	DNode *node;
	char *tpname;
	int i;
	switch( self->type ){
	case DAO_SQLITE     : DString_SetChars( sql, "__SQLITE_TABLE_PROPERTY__" ); break;
	case DAO_MYSQL      : DString_SetChars( sql, "__MYSQL_TABLE_PROPERTY__" );  break;
	case DAO_POSTGRESQL : DString_SetChars( sql, "__POSTGRESQL_TABLE_PROPERTY__" ); break;
	}
	node = DMap_Find( klass->lookupTable, sql );
	DString_Clear( sql );
	names = klass->objDataName->items.pString;
	vars = klass->instvars->items.pVar;
	tabname = DaoSQLDatabase_TableName( klass );
	DString_AppendChars( sql, "CREATE TABLE " );
	DString_Append( sql, tabname );
	DString_AppendChars( sql, "(" );
	for(i=1; i<klass->objDataName->size; i++){
		DaoType *type = DaoType_GetBaseType( vars[i]->dtype );
		if( i >1 ) DString_AppendChars( sql, "," );
		tpname = type->name->chars;
		DString_AppendChars( sql, "\n" );
		DString_Append( sql, names[i] );
		DString_AppendChars( sql, "  " );
		if( strcmp( tpname, "INT_PRIMARY_KEY" ) ==0 ){
			DString_AppendChars( sql, "INTEGER PRIMARY KEY" );
		}else if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ){
			if( self->type == DAO_SQLITE ){
				DString_AppendChars( sql, "INTEGER PRIMARY KEY AUTOINCREMENT" );
			}else if( self->type == DAO_POSTGRESQL ){
				DString_AppendChars( sql, "SERIAL PRIMARY KEY" );
			}else{
				DString_AppendChars( sql, "INTEGER PRIMARY KEY AUTO_INCREMENT" );
			}
		}else if( strstr( tpname, "CHAR" ) == tpname ){
			DString_AppendChars( sql, "CHAR(" );
			DString_AppendChars( sql, tpname+4 );
			DString_AppendChars( sql, ")" );
		}else if( strstr( tpname, "VARCHAR" ) == tpname ){
			DString_AppendChars( sql, "VARCHAR(" );
			DString_AppendChars( sql, tpname+7 );
			DString_AppendChars( sql, ")" );
		}else if( strcmp( tpname, "HSTORE" ) == 0 ){
			DString_AppendChars( sql, "HSTORE" );
		}else if( strcmp( tpname, "JSON" ) == 0 ){
			DString_AppendChars( sql, "JSON" );
		}else{
			DString_AppendChars( sql, tpname );
		}
	}
	if( node && LOOKUP_ST( node->value.pInt ) == DAO_CLASS_CONSTANT ){
		DaoValue *val = klass->constants->items.pConst[ LOOKUP_ID( node->value.pInt ) ]->value;
		if( val->type == DAO_STRING ){
			DString_AppendChars( sql, ",\n" );
			DString_Append( sql, val->xString.value );
		}
	}
	DString_AppendChars( sql, "\n);\n" );
	//printf( "%s\n", sql->chars );
}
int DaoSQLHandle_PrepareInsert( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoObject *object;
	DaoClass  *klass;
	DString *str = self->sqlSource;
	DString *tabname = NULL;
	char buf[20];
	int i, k;

	for(i=1; i<N; ++i){
		if( p[i]->type != DAO_OBJECT || p[i]->xObject.defClass != p[1]->xObject.defClass ){
			DaoProcess_RaiseError( proc, "Param", "" );
			return 0;
		}
		object = (DaoObject*) p[1];
		klass = object->defClass;
	}

	tabname = DaoSQLDatabase_TableName( klass );
	DList_PushBack( self->classList, klass );
	DString_AppendChars( self->sqlSource, "INSERT INTO " );
	DString_Append( self->sqlSource, tabname );
	DString_AppendChars( self->sqlSource, "(" );
	for(i=1,k=0; i<klass->objDataName->size; i++){
		if( self->database->type == DAO_POSTGRESQL ){
			DaoType *type = DaoType_GetBaseType( klass->instvars->items.pVar[i]->dtype );
			char *tpname = type->name->chars;
			if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) == 0 ) continue;
		}
		if( k++ ) DString_AppendChars( self->sqlSource, "," );
		DString_Append( self->sqlSource, klass->objDataName->items.pString[i] );
	}
	self->paramCount = 0;
	DString_AppendChars( self->sqlSource, ") VALUES(" );
	for(i=1,k=0; i<klass->objDataName->size; i++){
		DaoType *type = DaoType_GetBaseType( klass->instvars->items.pVar[i]->dtype );
		if( self->database->type == DAO_POSTGRESQL ){
			char *tpname = type->name->chars;
			if( strcmp( tpname, "INT_PRIMARY_KEY_AUTO_INCREMENT" ) == 0 ) continue;
		}
		self->partypes[self->paramCount++] = type;
		if( k++ ) DString_AppendChars( self->sqlSource, "," );
		if( self->database->type == DAO_POSTGRESQL ){
			sprintf( buf, "$%i", k );
			DString_AppendChars( self->sqlSource, buf );
			if( strcmp( type->name->chars, "HSTORE" ) == 0 ){
				DString_AppendChars( self->sqlSource, "::hstore" );
			}else if( strcmp( type->name->chars, "JSON" ) == 0 ){
				DString_AppendChars( self->sqlSource, "::json" );
			}
		}else{
			DString_AppendChars( self->sqlSource, "?" );
		}
	}
	DString_AppendChars( self->sqlSource, ");" );
	//printf( "%s\n", self->sqlSource->chars );
	return 1;
}
int DaoSQLHandle_PrepareDelete( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *str = self->sqlSource;
	DString *tabname = NULL;
	int i;
	if( p[1]->type != DAO_CLASS ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return 0;
	}
	klass = (DaoClass*) p[1];
	tabname = DaoSQLDatabase_TableName( klass );
	DList_PushBack( self->classList, klass );
	DString_AppendChars( self->sqlSource, "DELETE FROM " );
	DString_Append( self->sqlSource, tabname );
	DString_AppendChars( self->sqlSource, " " );
	return 1;
}
static int DaoTuple_ToPath( DaoTuple *self, DString *path, DString *sql, DaoProcess *proc, int k )
{
	DaoType *type = self->ctype;
	DList *id2name = NULL;
	DString *path2 = NULL;
	DNode *it;
	char chs[100] = {0};
	int i;

	if( type->mapNames != NULL && type->mapNames->size != self->size ){
		DaoProcess_RaiseError( proc, NULL, "Invalid JSON object" );
		return 0;
	}
	if( type->mapNames ){
		id2name = DList_New(0);
		for(it=DMap_First(type->mapNames); it; it=DMap_Next(type->mapNames,it)){
			DList_Append( id2name, it->key.pVoid );
		}
	}
	path2 = DString_New(1);
	for(i=0; i<self->size; ++i){
		DaoValue *item = self->values[i];
		DString_Assign( path2, path );
		DString_AppendChars( path2, "->" );
		if( item->type != DAO_TUPLE ) DString_AppendChars( path2, ">" );
		if( id2name ){
			DString_AppendSQL( path2, id2name->items.pString[i], 1, "\'" );
		}else{
			sprintf( chs, "%i", i+1 );
			DString_AppendChars( path2, chs );
		}
		switch( item->type ){
		case DAO_NONE :
			if( k ) DString_AppendChar( sql, ',' );
			DString_Append( sql, path2 );
			k += 1;
			break;
		case DAO_INTEGER :
			if( k ) DString_AppendChar( sql, ',' );
			DString_AppendChars( sql, "CAST(" );
			DString_Append( sql, path2 );
			DString_AppendChars( sql, " AS INTEGER)" );
			k += 1;
			break;
		case DAO_FLOAT :
			if( k ) DString_AppendChar( sql, ',' );
			DString_AppendChars( sql, "CAST(" );
			DString_Append( sql, path2 );
			DString_AppendChars( sql, " AS DOUBLE)" );
			k += 1;
			break;
		case DAO_STRING :
			if( k ) DString_AppendChar( sql, ',' );
			DString_AppendChars( sql, "CAST(" );
			DString_Append( sql, path2 );
			DString_AppendChars( sql, " AS TEXT)" );
			k += 1;
			break;
		case DAO_TUPLE :
			k = DaoTuple_ToPath( (DaoTuple*) item, path2, sql, proc, k );
			break;
		default :
			DaoProcess_RaiseError( proc, NULL, "Invalid JSON object" );
			break;
		}
	}
	if( id2name ) DList_Delete( id2name );
	DString_Delete( path2 );

	//printf( "%s\n", json->chars );
	//DString_SetChars( json, "{ \"name\": \"Firefox\" }" );
	return k;
}
int DaoSQLHandle_PrepareSelect( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DaoObject *object;
	DString *tabname = NULL;
	int i, j, k, m, ntable = 0;
	self->paramCount = 0;
	DString_AppendChars( self->sqlSource, "SELECT " );
	for(i=1; i<N; i++) ntable += p[i]->type == DAO_CLASS || p[i]->type == DAO_OBJECT;
	for(i=1; i<N; i++){
		if( p[i]->type != DAO_CLASS && p[i]->type != DAO_OBJECT ){
			DaoProcess_RaiseError( proc, "Param", "" );
			return 0;
		}
		klass = DaoValue_CastClass( p[i] );
		object = DaoValue_CastObject( p[i] );
		if( object ) klass = object->defClass;
		tabname = DaoSQLDatabase_TableName( klass );
		m = klass->objDataName->size;
		if( i+1 < N && p[i+1]->type == DAO_INTEGER ){
			i ++;
			if( p[i]->xInteger.value + 1 < m ) m = p[i]->xInteger.value + 1;
		}
		DList_PushBack( self->classList, klass );
		DList_PushBack( self->countList, (void*)(size_t) m );
		if( m == 1 ) continue;
		if( self->classList->size >1 ) DString_AppendChars( self->sqlSource, "," );
		for(j=1,k=0; j<m; j++){
			DaoType *type = DaoType_GetBaseType( klass->instvars->items.pVar[j]->dtype );
			if( type->tid == DAO_MAP && self->database->type == DAO_POSTGRESQL ){
				DaoMap *keys = object ? (DaoMap*) object->objValues[j] : NULL ;
				DNode *it = keys ? DaoMap_First(keys) : NULL;
				for(; it; it=DaoMap_Next(keys,it)){
					if( k++ ) DString_AppendChars( self->sqlSource, "," );
					if( ntable >1 ){
						DString_Append( self->sqlSource, tabname );
						DString_AppendChars( self->sqlSource, "." );
					}
					DString_Append( self->sqlSource, klass->objDataName->items.pString[j] );
					DString_AppendChars( self->sqlSource, "->" );
					DString_AppendSQL( self->sqlSource, it->key.pValue->xString.value, 1, "\'" );
				}
			}else if( type->tid == DAO_TUPLE && self->database->type == DAO_POSTGRESQL ){
				DaoTuple *json = object ? (DaoTuple*) object->objValues[j] : NULL ;
				DString *path = DString_New(1);
				if( object == NULL ){
				}
				if( ntable >1 ){
					DString_Append( path, tabname );
					DString_AppendChars( path, "." );
				}
				DString_Append( path, klass->objDataName->items.pString[j] );
				k = DaoTuple_ToPath( json, path, self->sqlSource, proc, k );
				DString_Delete( path );
			}else{
				if( k++ ) DString_AppendChars( self->sqlSource, "," );
				if( ntable >1 ){
					DString_Append( self->sqlSource, tabname );
					DString_AppendChars( self->sqlSource, "." );
				}
				DString_Append( self->sqlSource, klass->objDataName->items.pString[j] );
			}
		}
	}
	DString_AppendChars( self->sqlSource, " FROM " );
	for(i=1; i<N; i++){
		if( p[i]->type == DAO_INTEGER ) continue;
		if( i+1<N && p[i+1]->type == DAO_INTEGER && p[i+1]->xInteger.value ==0 ) continue;
		klass = DaoValue_CastClass( p[i] );
		object = DaoValue_CastObject( p[i] );
		if( object ) klass = object->defClass;
		tabname = DaoSQLDatabase_TableName( klass );
		if( i >1 ) DString_AppendChars( self->sqlSource, "," );
		DString_Append( self->sqlSource, tabname );
	}
	//printf( "%s\n", self->sqlSource->chars );
	return 1;
}
int DaoSQLHandle_PrepareUpdate( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *tabname = NULL;
	int i, j;
	self->paramCount = 0;
	DString_AppendChars( self->sqlSource, "UPDATE " );
	for(i=1; i<N; i++){
		if( i >1 ) DString_AppendChars( self->sqlSource, "," );
		klass = (DaoClass*) p[i];
		DList_PushBack( self->classList, klass );
		tabname = DaoSQLDatabase_TableName( klass );
		DString_Append( self->sqlSource, tabname );
	}
	DString_AppendChars( self->sqlSource, " SET " );
	return 1;
}
static void DaoSQLHandle_SQLString( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutString( proc, handler->sqlSource );
}
void DString_AppendSQL( DString *self, DString *content, int escape, const char *quote )
{
	DString *mbstring;
	
	if( escape == 0 ){
		DString_Append( self, content );
		return;
	}
	mbstring = DString_New();
	DString_Append( mbstring, content );
	DString_Change( mbstring, "[\\\"]", "\\%0", 0 );
	DString_Change( mbstring, "[\']", "''", 0 );
	DString_AppendChars( self, quote );
	DString_Append( self, mbstring );
	DString_AppendChars( self, quote );
	DString_Delete( mbstring );
}
static void DaoSQLHandle_SetAdd( DaoProcess *proc, DaoValue *p[], int N, int add )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *fname, *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *value = p[2];
	DaoClass *klass;
	DaoValue *data;
	DaoType *type;

	DaoProcess_PutValue( proc, p[0] );
	if( handler->classList->size == 0 ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	fname = DString_New(1);
	klass = handler->classList->items.pClass[0];
	if( handler->setCount ) DString_AppendChars( handler->sqlSource, ", " );
	DString_Assign( fname, field->xString.value );

	data = DaoClass_GetData( klass, fname, NULL );
	if( data == NULL || data->xBase.subtype != DAO_OBJECT_VARIABLE ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	type = data->xVar.dtype;

	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		klass = (DaoClass*) p[1];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( fname, tabname );
		DString_AppendChars( fname, "." );
	}
	DString_Append( handler->sqlSource, fname );
	DString_AppendChars( handler->sqlSource, "=" );
	if( add ){
		if( tabname != NULL ){
			DString_Append( fname, tabname );
			DString_AppendChars( fname, "." );
		}
		DString_Append( handler->sqlSource, fname );
		DString_AppendChars( handler->sqlSource, "+ " );
	}
	if( N >2 ){
		DaoValue_GetString( value, field->xString.value );
		DString_AppendSQL( handler->sqlSource, field->xString.value, value->type == DAO_STRING, "\'" );
	}else{
		handler->partypes[handler->paramCount++] = type;
		if( handler->database->type == DAO_POSTGRESQL ){
			char buf[20];
			sprintf( buf, "$%i", handler->paramCount );
			DString_AppendChars( handler->sqlSource, buf );
		}else{
			DString_AppendChars( handler->sqlSource, "?" );
		}
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
	DString_AppendChars( handler->sqlSource, " WHERE " );
}
static void DaoSQLHandle_Operator( DaoProcess *proc, DaoValue *p[], int N, char *op )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DString *tabname = NULL;
	DaoValue *field = p[1];
	DaoValue *value = p[2];
	DaoClass *klass;

	DaoProcess_PutValue( proc, p[0] );
	if( handler->classList->size == 0 ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	klass = handler->classList->items.pClass[0];
	if( handler->boolCount ) DString_AppendChars( handler->sqlSource, " AND " );
	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( handler->sqlSource, tabname );
		DString_AppendChars( handler->sqlSource, "." );
	}
	DString_Append( handler->sqlSource, field->xString.value );
	DString_AppendChars( handler->sqlSource, op );

	if( value->type ){
		DaoValue_GetString( value, field->xString.value );
		DString_AppendSQL( handler->sqlSource, field->xString.value, value->type == DAO_STRING, "\'" );
	}else{
		DaoValue *data = DaoClass_GetData( klass, field->xString.value, NULL );
		if( data == NULL || data->xBase.subtype != DAO_OBJECT_VARIABLE ){
			DaoProcess_RaiseError( proc, "Param", "" );
			return;
		}
		handler->partypes[handler->paramCount++] = data->xVar.dtype;
		if( handler->database->type == DAO_POSTGRESQL ){
			char buf[20];
			sprintf( buf, "$%i", handler->paramCount );
			DString_AppendChars( handler->sqlSource, buf );
		}else{
			DString_AppendChars( handler->sqlSource, "?" );
		}
	}
	handler->boolCount ++;
	//fprintf( stderr, "%s\n", handler->sqlSource->chars );
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
	DList *values;
	DaoValue *val;
	int i, fid=1, vid = 2;
	DaoProcess_PutValue( proc, p[0] );
	if( p[1]->type == DAO_CLASS ){
		fid = 2;  vid = 3;
		table = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
	}
	if( p[vid]->type != DAO_LIST || p[vid]->xList.value->size ==0 ){
		DaoProcess_RaiseError( proc, "Param", "need non-empty list" );
		return;
	}
	values = p[vid]->xList.value;
	DString_AppendChars( handler->sqlSource, " " );
	if( table ){
		DString_Append( handler->sqlSource, table );
		DString_AppendChars( handler->sqlSource, "." );
	}
	DString_Append( handler->sqlSource, p[fid]->xString.value );
	DString_AppendChars( handler->sqlSource, " IN (" );
	for(i=0; i<values->size; i++){
		if( i ) DString_AppendChars( handler->sqlSource, ", " );
		val = values->items.pValue[i];
		if( val->type ==0 ){
			DString_AppendChars( handler->sqlSource, "NULL" );
		}else if( val->type <= DAO_FLOAT ){
			DaoValue_GetString( val, handler->buffer );
			DString_Append( handler->sqlSource, handler->buffer );
		}else if( val->type == DAO_STRING ){
			DString_AppendChars( handler->sqlSource, "'" );
			DString_Append( handler->sqlSource, val->xString.value );
			DString_AppendChars( handler->sqlSource, "'" );
		}else{
			DaoProcess_RaiseError( proc, NULL, "invalid value in list" );
			break;
		}
	}
	DString_AppendChars( handler->sqlSource, " ) " );
	handler->boolCount ++;
	//printf( "%s\n", handler->sqlSource->chars );
}
static void DaoSQLHandle_OR( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " OR " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_And( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " AND " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_Not( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " NOT " );
	handler->boolCount = 0;
}
static void DaoSQLHandle_LBrace( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, "(" );
	handler->boolCount = 0;
}
static void DaoSQLHandle_RBrace( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, ")" );
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
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	c1 = (DaoClass*) p[1];
	c2 = (DaoClass*) p[2];
	tabname1 = DaoSQLDatabase_TableName( c1 );
	tabname2 = DaoSQLDatabase_TableName( c2 );
	if( handler->boolCount ) DString_AppendChars( handler->sqlSource, " AND " );
	handler->boolCount ++;
	if( N == 3 ){
		for(i=1; i<c1->objDataName->size; i++){
			DNode *node = DMap_Find( c2->lookupTable, c1->objDataName->items.pVoid[i] );
			if( node && LOOKUP_ST( node->value.pInt ) == DAO_OBJECT_VARIABLE ){
				if( k >0 ) DString_AppendChars( handler->sqlSource, " AND " );
				DString_Append( handler->sqlSource, tabname1 );
				DString_AppendChars( handler->sqlSource, "." );
				DString_Append( handler->sqlSource, c1->objDataName->items.pString[i] );
				DString_AppendChars( handler->sqlSource, "=" );
				DString_Append( handler->sqlSource, tabname2 );
				DString_AppendChars( handler->sqlSource, "." );
				DString_Append( handler->sqlSource, c1->objDataName->items.pString[i] );
				k ++;
			}
		}
	}else{
		DString *field1 = p[3]->xString.value;
		DString *field2 = p[3]->xString.value;
		DNode *node1, *node2;
		if( N > 4 ) field2 = p[4]->xString.value;
		node1 = DMap_Find( c1->lookupTable, field1 );
		node2 = DMap_Find( c2->lookupTable, field2 );
		if( node1 && LOOKUP_ST( node1->value.pInt ) == DAO_OBJECT_VARIABLE 
				&& node2 && LOOKUP_ST( node2->value.pInt ) == DAO_OBJECT_VARIABLE ){
			DString_Append( handler->sqlSource, tabname1 );
			DString_AppendChars( handler->sqlSource, "." );
			DString_Append( handler->sqlSource, field1 );
			DString_AppendChars( handler->sqlSource, "=" );
			DString_Append( handler->sqlSource, tabname2 );
			DString_AppendChars( handler->sqlSource, "." );
			DString_Append( handler->sqlSource, field2 );
		}else{
			DaoProcess_RaiseError( proc, "Param", "" );
			return;
		}
	}
}
static void DaoSQLHandle_Sort( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	int desc = 0;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " ORDER BY " );
	if( p[2]->type == DAO_INTEGER ){
		desc = p[2]->xInteger.value;
		DString_Append( handler->sqlSource, p[1]->xString.value );
	}else{
		if( p[1]->type == DAO_CLASS ){
			DString *tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
			DString_Append( handler->sqlSource, tabname );
			DString_AppendChars( handler->sqlSource, "." );
		}
		DString_Append( handler->sqlSource, p[2]->xString.value );
		desc = p[3]->xInteger.value;
	}
	if( desc ){
		DString_AppendChars( handler->sqlSource, " DESC " );
	}else{
		DString_AppendChars( handler->sqlSource, " ASC " );
	}
}
static void DaoSQLHandle_Range( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " LIMIT " );
	DaoValue_GetString( p[1], handler->buffer );
	DString_Append( handler->sqlSource, handler->buffer );
	if( N >2 ){
		DString_AppendChars( handler->sqlSource, " OFFSET " );
		DaoValue_GetString( p[2], handler->buffer );
		DString_Append( handler->sqlSource, handler->buffer );
	}
}
static void DaoSQLHandle_Stop( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	handler->stopQuery = 1;
}

int DaoOnLoad( DaoVmSpace * vms, DaoNamespace *ns )
{
	char *lang = getenv( "DAO_HELP_LANG" );
	DaoTypeBase *typers[] = { & DaoSQLDatabase_Typer, & DaoSQLHandle_Typer, NULL };

	DaoNamespace_DefineType( ns, "int", "INT" );
	DaoNamespace_DefineType( ns, "int", "TINYINT" );
	DaoNamespace_DefineType( ns, "int", "SMALLINT" );
	DaoNamespace_DefineType( ns, "int", "MEDIUMINT" );
	DaoNamespace_DefineType( ns, "int", "INT_PRIMARY_KEY" );
	DaoNamespace_DefineType( ns, "int", "INT_PRIMARY_KEY_AUTO_INCREMENT" );
	DaoNamespace_DefineType( ns, "float",  "BIGINT" );
	DaoNamespace_DefineType( ns, "string", "TEXT" );
	DaoNamespace_DefineType( ns, "string", "MEDIUMTEXT" );
	DaoNamespace_DefineType( ns, "string", "LONGTEXT" );
	DaoNamespace_DefineType( ns, "string", "BLOB" );
	DaoNamespace_DefineType( ns, "string", "MEDIUMBLOB" );
	DaoNamespace_DefineType( ns, "string", "LONGBLOB" );
	DaoNamespace_DefineType( ns, "string", "CHAR10" );
	DaoNamespace_DefineType( ns, "string", "CHAR20" );
	DaoNamespace_DefineType( ns, "string", "CHAR50" );
	DaoNamespace_DefineType( ns, "string", "CHAR100" );
	DaoNamespace_DefineType( ns, "string", "CHAR200" );
	DaoNamespace_DefineType( ns, "string", "VARCHAR10" );
	DaoNamespace_DefineType( ns, "string", "VARCHAR20" );
	DaoNamespace_DefineType( ns, "string", "VARCHAR50" );
	DaoNamespace_DefineType( ns, "string", "VARCHAR100" );
	DaoNamespace_DefineType( ns, "string", "VARCHAR200" );

	DaoNamespace_WrapTypes( ns, typers );

	if( lang ){
		char fname[100] = "help_module_official_sql_";
		strcat( fname, lang );
		DaoVmSpace_Load( vms, fname );
	}
	return 0;
}
