/*
// DaoSQL
// Database handling with mapping class instances to database table records.
// Copyright (C) 2008-2015, Limin Fu (http://fulimin.org).
*/
#include"stdlib.h"
#include"string.h"
#include"daoSQL.h"

void DaoSQLDatabase_Init( DaoSQLDatabase *self, int type )
{
	self->type = type;
	self->dataClass = DMap_New(0,0);
	self->name = DString_New();
	self->host = DString_New();
	self->user = DString_New();
	self->password = DString_New();
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
	{ DaoSQLDatabase_SetName,      "SetName( self: Database, name: string )" },
	{ DaoSQLDatabase_GetName,      "GetName( self: Database) => string" },
	{ DaoSQLDatabase_SetHost,      "SetHost( self: Database, host: string )" },
	{ DaoSQLDatabase_GetHost,      "GetHost( self: Database) => string" },
	{ DaoSQLDatabase_SetUser,      "SetUser( self: Database, user: string )" },
	{ DaoSQLDatabase_GetUser,      "GetUser( self: Database) => string" },
	{ DaoSQLDatabase_SetPassword,  "SetPassword( self: Database, pwd: string )" },
	{ DaoSQLDatabase_GetPassword,  "GetPassword( self: Database) => string" },
	{ NULL, NULL }
};

DaoTypeBase DaoSQLDatabase_Typer = 
{ "Database<>", NULL, NULL, modelMeths, {0}, {0}, NULL, NULL };

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
	self->sqlSource = DString_New();
	self->buffer = DString_New();
	self->classList = DList_New(0);
	self->countList = DList_New(0);
	self->setCount = 0;
	self->boolCount = 0;
	self->prepared = 0;
	self->executed = 0;
	self->paramCount = 0;
	self->stopQuery = 0;
	for( i=0; i<MAX_PARAM_COUNT; i++ ){
		self->pardata[i] = DString_New();
		self->resdata[i] = DString_New();
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
static void DaoSQLHandle_Inline( DaoProcess *proc, DaoValue *p[], int N );
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
static void DaoSQLHandle_GroupBy( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Sort( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Range( DaoProcess *proc, DaoValue *p[], int N );
static void DaoSQLHandle_Stop( DaoProcess *proc, DaoValue *p[], int N );

static DaoFuncItem handlerMeths[]=
{
	{ DaoSQLHandle_SQLString, "SQLString( self: @Handle )=>string" },
	{ DaoSQLHandle_Where,  "Where( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_Inline, "Inline( self: @Handle, field: string )=>@Handle" },
	{ DaoSQLHandle_Set, "Set( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_Add, "Add( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_EQ, "EQ( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_NE, "NE( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_GT, "GT( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_GE, "GE( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_LT, "LT( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_LE, "LE( self: @Handle, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_Set,"Set( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_Add,"Add( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_EQ, "EQ( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_NE, "NE( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_GT, "GT( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_GE, "GE( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_LT, "LT( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_LE, "LE( self: @Handle, table: class, field: string, value: any=none )=>@Handle" },
	{ DaoSQLHandle_IN, "In( self: @Handle, field: string, values:list<@T>={} )=>@Handle" },
	{ DaoSQLHandle_IN, "In( self: @Handle, table: class, field: string, values:list<@T>={} )=>@Handle" },
	{ DaoSQLHandle_OR,"Or( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_And, "And( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_Not,     "Not( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_LBrace,  "LBrace( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_RBrace,  "RBrace( self: @Handle )=>@Handle" },
	{ DaoSQLHandle_Match,   "Match( self: @Handle, table1: class, table2: class, field1=\"\", field2=\"\" )=>@Handle" },
	{ DaoSQLHandle_GroupBy,  "GroupBy( self: @Handle, field: string )=>@Handle" },
	{ DaoSQLHandle_GroupBy,  "GroupBy( self: @Handle, table: class, field: string )=>@Handle" },
	{ DaoSQLHandle_Sort,  "Sort( self: @Handle, field: string, desc=0 )=>@Handle" },
	{ DaoSQLHandle_Sort,  "Sort( self: @Handle, table: class, field: string, desc=0 )=>@Handle" },
	{ DaoSQLHandle_Range, "Range( self: @Handle, limit :int, offset=0 )=>@Handle" },
	{ DaoSQLHandle_Stop,  "Stop( self: @Handle )" },
	{ NULL, NULL }
};

DaoTypeBase DaoSQLHandle_Typer = 
{ "Handle<>", NULL, NULL, handlerMeths, {0}, {0}, NULL, NULL };

DString* DaoSQLDatabase_TableName( DaoClass *klass )
{
	DString *name = DString_New();
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
		if( strcmp( tpname, "INTEGER_PRIMARY_KEY" ) ==0 ){
			DString_AppendChars( sql, "INTEGER PRIMARY KEY" );
		}else if( strcmp( tpname, "INTEGER_PRIMARY_KEY_AUTO_INCREMENT" ) ==0 ){
			if( self->type == DAO_SQLITE ){
				DString_AppendChars( sql, "INTEGER PRIMARY KEY AUTOINCREMENT" );
			}else if( self->type == DAO_POSTGRESQL ){
				DString_AppendChars( sql, "SERIAL PRIMARY KEY" );
			}else{
				DString_AppendChars( sql, "BIGINT PRIMARY KEY AUTO_INCREMENT" );
			}
		}else if( type == dao_sql_type_date ){
			if( self->type == DAO_POSTGRESQL ){
				DString_AppendChars( sql, "DATE" );
			}else{
				DString_AppendChars( sql, "INTEGER" );
			}
		}else if( type == dao_sql_type_timestamp ){
			if( self->type == DAO_POSTGRESQL ){
				DString_AppendChars( sql, "TIMESTAMP DEFAULT CURRENT_TIMESTAMP" );
			}else if( self->type == DAO_MYSQL ){
				DString_AppendChars( sql, "TIMESTAMP DEFAULT CURRENT_TIMESTAMP" );
			}else{
				DString_AppendChars( sql, "BIGINT" );
			}
		}else if( type == dao_type_datetime ){
			if( self->type == DAO_POSTGRESQL ){
				DString_AppendChars( sql, tpname );
			}else{
				DString_AppendChars( sql, "BIGINT" );
			}
		}else if( type == dao_sql_type_double ){
			DString_AppendChars( sql, "DOUBLE PRECISION" );
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
void DaoSQLDatabase_DeleteTable( DaoSQLDatabase *self, DaoClass *klass, DString *sql )
{
	DString *tabname = NULL;

	DString_Clear( sql );
	tabname = DaoSQLDatabase_TableName( klass );
	DString_AppendChars( sql, "DROP TABLE " );
	DString_Append( sql, tabname );
	DString_AppendChars( sql, ";\n" );
	//printf( "%s\n", sql->chars );
}
int DaoSQLHandle_PrepareInsert( DaoSQLHandle *self, DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *klass;
	DString *tabname = NULL;
	DString *keyname = NULL;
	char buf[20];
	int i, k;

	for(i=1; i<N; ++i){
		if( p[1]->type == DAO_CLASS && N == 2 ){
			klass = (DaoClass*) p[i];
			break;
		}
		if( p[i]->type != DAO_OBJECT || p[i]->xObject.defClass != p[1]->xObject.defClass ){
			DaoProcess_RaiseError( proc, "Param", "Need object(s) of the same class" );
			return 0;
		}
		klass = p[1]->xObject.defClass;
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
			if( strcmp( tpname, "INTEGER_PRIMARY_KEY_AUTO_INCREMENT" ) == 0 ){
				keyname = klass->objDataName->items.pString[i];
				continue;
			}
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
			if( strcmp( tpname, "INTEGER_PRIMARY_KEY_AUTO_INCREMENT" ) == 0 ) continue;
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
	if( self->database->type == DAO_POSTGRESQL ){
		DString_AppendChars( self->sqlSource, ") RETURNING " );
		DString_Append( self->sqlSource, keyname );
		DString_AppendChars( self->sqlSource, ";" );
	}else{
		DString_AppendChars( self->sqlSource, ");" );
	}
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
	path2 = DString_New();
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
			if( type == dao_sql_type_date ) goto HandleNormalField;
			if( type == dao_sql_type_timestamp ) goto HandleNormalField;
			if( type->tid == DAO_MAP && self->database->type == DAO_POSTGRESQL ){
				DaoMap *keys = object ? (DaoMap*) object->objValues[j] : NULL ;
				DNode *it = keys ? DaoMap_First(keys) : NULL;
				if( object == NULL ){
					DaoProcess_RaiseError( proc, "Param", "HSTORE field requires instance for Select()" );
					return 0;
				}
				if( keys->value->size == 0 ) goto HandleNormalField;
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
				DString *path;
				if( object == NULL ){
					DaoProcess_RaiseError( proc, "Param", "JSON field requires instance for Select()" );
					return 0;
				}
				if( json->size == 0 ) goto HandleNormalField;
				path = DString_New();
				if( ntable >1 ){
					DString_Append( path, tabname );
					DString_AppendChars( path, "." );
				}
				DString_Append( path, klass->objDataName->items.pString[j] );
				k = DaoTuple_ToPath( json, path, self->sqlSource, proc, k );
				DString_Delete( path );
			}else if( type->tid == DAO_INTEGER && DString_FindChars( type->name, "COUNT", 0 ) == 0 ){
				DString *sql = self->sqlSource;
				if( k++ ) DString_AppendChars( self->sqlSource, "," );
				if( DString_FindChars( type->name, "COUNT_DISTINCT_", 0 ) == 0 ){
					DString_AppendChars( sql, "COUNT(DISTINCT " );
					if( ntable >1 ){
						DString_Append( self->sqlSource, tabname );
						DString_AppendChars( self->sqlSource, "." );
					}
					DString_AppendChars( sql, type->name->chars + strlen("COUNT_DISTINCT_") );
					DString_AppendChars( sql, ")" );
				}else if( DString_FindChars( type->name, "COUNT", 0 ) == 0 ){
					DString_AppendChars( sql, "COUNT(1)" );
				}
			}else if( type->tid == DAO_INTEGER && DString_FindChars( type->name, "SUM_INT", 0 ) == 0 ){
				DString *sql = self->sqlSource;
				if( k++ ) DString_AppendChars( self->sqlSource, "," );
				if( DString_FindChars( type->name, "SUM_INT_", 0 ) == 0 ){
					DString_AppendChars( sql, "SUM(" );
					if( ntable >1 ){
						DString_Append( self->sqlSource, tabname );
						DString_AppendChars( self->sqlSource, "." );
					}
					DString_AppendChars( sql, type->name->chars + strlen("SUM_INT_") );
					DString_AppendChars( sql, ")" );
				}else{
					DString_AppendChars( sql, "SUM(1)" );
				}
			}else if( type->tid == DAO_FLOAT && DString_FindChars( type->name, "SUM_FLOAT", 0 ) == 0 ){
				DString *sql = self->sqlSource;
				if( k++ ) DString_AppendChars( self->sqlSource, "," );
				if( DString_FindChars( type->name, "SUM_FLOAT_", 0 ) == 0 ){
					DString_AppendChars( sql, "SUM(" );
					if( ntable >1 ){
						DString_Append( self->sqlSource, tabname );
						DString_AppendChars( self->sqlSource, "." );
					}
					DString_AppendChars( sql, type->name->chars + strlen("SUM_FLOAT_") );
					DString_AppendChars( sql, ")" );
				}else{
					DString_AppendChars( sql, "SUM(1.0)" );
				}
			}else{
HandleNormalField:
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
	DaoObject *object;
	DString *tabname = NULL;
	int i, j;
	self->paramCount = 0;
	DString_AppendChars( self->sqlSource, "UPDATE " );
	for(i=1; i<N; i++){
		if( i >1 ) DString_AppendChars( self->sqlSource, "," );
		klass = DaoValue_CastClass( p[i] );
		object = DaoValue_CastObject( p[i] );
		if( object ) klass = object->defClass;
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
static void DaoSQLHandle_Inline( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );

	if( handler->boolCount ) DString_AppendChars( handler->sqlSource, " AND " );
	DString_Append( handler->sqlSource, p[1]->xString.value );
	handler->boolCount += 1;
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
	fname = DString_New();
	klass = handler->classList->items.pClass[0];
	if( handler->setCount ) DString_AppendChars( handler->sqlSource, ", " );

	if( p[1]->type == DAO_CLASS ){
		field = p[2];
		value = p[3];
		klass = (DaoClass*) p[1];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( fname, tabname );
		DString_AppendChars( fname, "." );
	}

	DString_Assign( fname, field->xString.value );
	data = DaoClass_GetData( klass, fname, NULL );
	if( data == NULL || data->xBase.subtype != DAO_OBJECT_VARIABLE ){
		DaoProcess_RaiseError( proc, "Param", "" );
		return;
	}
	type = data->xVar.dtype;

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
		DaoValue_GetString( value, fname );
		DString_AppendSQL( handler->sqlSource, fname, value->type == DAO_STRING, "\'" );
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
		klass = (DaoClass*) p[1];
		field = p[2];
		value = p[3];
		tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( handler->sqlSource, tabname );
		DString_AppendChars( handler->sqlSource, "." );
	}
	DString_Append( handler->sqlSource, field->xString.value );
	DString_AppendChars( handler->sqlSource, op );

	if( value->type ){
		DString *buffer = DString_New();
		DaoValue_GetString( value, buffer );
		DString_AppendSQL( handler->sqlSource, buffer, value->type == DAO_STRING, "\'" );
		DString_Delete( buffer );
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
	if( handler->boolCount ) DString_AppendChars( handler->sqlSource, " AND " );
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
static void DaoSQLHandle_GroupBy( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoSQLHandle *handler = (DaoSQLHandle*) p[0]->xCdata.data;
	DaoProcess_PutValue( proc, p[0] );
	DString_AppendChars( handler->sqlSource, " GROUP BY " );
	if( p[1]->type == DAO_CLASS ){
		DString *tabname = DaoSQLDatabase_TableName( (DaoClass*) p[1] );
		DString_Append( handler->sqlSource, tabname );
		DString_AppendChars( handler->sqlSource, "." );
		DString_Append( handler->sqlSource, p[2]->xString.value );
	}else{
		DString_Append( handler->sqlSource, p[1]->xString.value );
	}
	//printf( "%s\n", handler->sqlSource->chars );
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


static int FloorDiv( int a, int b ) 
{
    return (a - (a < 0? b - 1 : 0))/b;
}

static int ToJulianDay( int year, int month, int day )
{
    int a = FloorDiv(14 - month, 12);
    year += 4800 - a;
    month += 12*a - 3;
	day += FloorDiv( 153*month + 2, 5 ) + 365*year;
	day += FloorDiv( year, 4 ) - FloorDiv( year, 100 ) + FloorDiv( year, 400 );
	return day - 32045;
}
static void FromJulianDay( int jday, int *year, int *month, int *day )
{
	int F = jday + 1401 + (((4*jday + 274277) / 146097) * 3) / 4 - 38;
	int E = 4*F + 3;
	int G = (E%1461) / 4;
	int H = 5*G + 2;
	*day = (H%153) / 5 + 1;
	*month = ((H/153 + 2) % 12) + 1;
	*year = (E/1461) - 4716 + (12 + 2 - *month) / 12;
}

int32_t DaoSQL_EncodeDate( DaoTuple *tuple )
{
	if( DaoType_GetBaseType( tuple->ctype ) == dao_sql_type_date ){
		int year = tuple->values[0]->xInteger.value;
		int month = tuple->values[1]->xInteger.value;
		int day = tuple->values[2]->xInteger.value;
		int epoch_day = ToJulianDay( 2000, 1, 1 );
		int stamp_day = ToJulianDay( year, month, day );
		return stamp_day - epoch_day;
	}
	return 0;
}
int64_t DaoSQL_EncodeTimestamp( DaoTuple *tuple )
{
	if( DaoType_GetBaseType( tuple->ctype ) == dao_sql_type_timestamp ){
		int year = tuple->values[0]->xInteger.value;
		int month = tuple->values[1]->xInteger.value;
		int day = tuple->values[2]->xInteger.value;
		int64_t epoch_day = ToJulianDay( 2000, 1, 1 );
		int64_t stamp_day = ToJulianDay( year, month, day );
		double pg_time = (stamp_day - epoch_day) * 24 * 3600;
		pg_time += tuple->values[3]->xInteger.value * 3600;
		pg_time += tuple->values[4]->xInteger.value * 60;
		pg_time += tuple->values[5]->xFloat.value;
		return pg_time * 1E6;
	}
	return 0;
}
void DaoSQL_DecodeDate( DaoTuple *tuple, int32_t date_days )
{
	int64_t epoch_jday = ToJulianDay( 2000, 1, 1 );
	int64_t stamp_jday = epoch_jday + date_days;
	int year = 0, month = 0, day = 0;

	if( DaoType_GetBaseType( tuple->ctype ) != dao_sql_type_date ) return;

	FromJulianDay( stamp_jday, & year, & month, & day );
	tuple->values[0]->xInteger.value = year;
	tuple->values[1]->xInteger.value = month;
	tuple->values[2]->xInteger.value = day;
}
void DaoSQL_DecodeTimestamp( DaoTuple *tuple, int64_t stamp_msec )
{
	int64_t epoch_jday = ToJulianDay( 2000, 1, 1 );
	int64_t stamp_sec = stamp_msec / 1E6 + epoch_jday * 24 * 3600;
	int64_t stamp_jday = stamp_sec / (24 * 3600);
	int year = 0, month = 0, day = 0;

	if( DaoType_GetBaseType( tuple->ctype ) != dao_sql_type_timestamp ) return;

	FromJulianDay( stamp_jday, & year, & month, & day );
	tuple->values[0]->xInteger.value = year;
	tuple->values[1]->xInteger.value = month;
	tuple->values[2]->xInteger.value = day;
	stamp_sec = stamp_sec % (24 * 3600);
	tuple->values[3]->xInteger.value = stamp_sec / 3600;
	tuple->values[4]->xInteger.value = (stamp_sec % 3600) / 60;
	tuple->values[5]->xFloat.value = (stamp_sec % 60) + (stamp_msec % 1000000)/1E6;
}


DaoType *dao_sql_type_bigint = NULL;
DaoType *dao_sql_type_integer_primary_key = NULL;
DaoType *dao_sql_type_integer_primary_key_auto_increment = NULL;
DaoType *dao_sql_type_real = NULL;
DaoType *dao_sql_type_float = NULL;
DaoType *dao_sql_type_double = NULL;
DaoType *dao_sql_type_date = NULL;
DaoType *dao_sql_type_timestamp = NULL;
DaoType *dao_type_datetime = NULL;


static void SQL_EncodeTS( DaoProcess *proc, DaoValue *p[], int N )
{
	daoint value = DaoSQL_EncodeTimestamp( (DaoTuple*) p[0] );
	DaoProcess_PutInteger( proc, value );
}
static void SQL_DecodeTS( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTuple *tuple = DaoProcess_PutTuple( proc, 0 );
	DaoSQL_DecodeTimestamp( tuple, p[0]->xInteger.value );
}
static void SQL_TableName( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoClass *table = (DaoClass*) p[0];
	DString *name = DaoSQLDatabase_TableName( table );
	DaoProcess_PutString( proc, name );
}

static DaoFuncItem sqlMeths[]=
{
	{ SQL_EncodeTS,  "Encode( ts: TIMESTAMP ) => int" },
	{ SQL_DecodeTS,  "Decode( value: int ) => TIMESTAMP" },
	{ SQL_TableName,  "GetTableName( invar table: class ) => string" },
	{ NULL, NULL }
};

static DaoFuncItem absDateMeths[] =
{
	{ NULL,  ".year( self: DateType ) => int" },
	{ NULL,  ".month( self: DateType ) => int" },
	{ NULL,  ".day( self: DateType ) => int" },

	{ NULL,  ".year=( self: DateType , value: int )" },
	{ NULL,  ".month=( self: DateType , value: int )" },
	{ NULL,  ".day=( self: DateType , value: int )" },
	{ NULL, NULL }
};

DaoTypeBase absDateTyper =
{
	"DateType", NULL, NULL, (DaoFuncItem*) absDateMeths, {0}, {0},
	(FuncPtrDel) NULL, NULL
};


static void DATE_GetYear( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, self->time.year );
}
static void DATE_GetMonth( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, self->time.month );
}
static void DATE_GetDay( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, self->time.day );
}

static void DATE_SetYear( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	self->time.year = p[1]->xInteger.value;
}
static void DATE_SetMonth( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	int value = p[1]->xInteger.value;
	if( value < 1 || value > 12 ){
		DaoProcess_RaiseError( proc, "Param", "Invalid month" );
		return;
	}
	self->time.month = value;
}
static int LeapYear( int y )
{
	return (y%4 == 0 && y%100) || y%400 == 0;
}

static int DaysInMonth( int y, int m )
{
	const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	if ( m > 12 )
		return 0;
	return ( LeapYear( y ) && m == 2 )? 29 : days[m - 1];
}
static void DATE_SetDay( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	int value = p[1]->xInteger.value;
	int days = DaysInMonth( self->time.year, self->time.month );
	if( value < 1 || value > days ){
		DaoProcess_RaiseError( proc, "Param", "Invalid day" );
		return;
	}
	self->time.day = value;
}

static DaoFuncItem conDateMeths[] =
{
	{ DATE_GetYear,   ".year( self: DateType2 ) => int" },
	{ DATE_GetMonth,  ".month( self: DateType2 ) => int" },
	{ DATE_GetDay,    ".day( self: DateType2 ) => int" },

	{ DATE_SetYear,   ".year=( self: DateType2 , value: int )" },
	{ DATE_SetMonth,  ".month=( self: DateType2 , value: int )" },
	{ DATE_SetDay,    ".day=( self: DateType2 , value: int )" },
	{ NULL, NULL }
};

DaoTypeBase conDateTyper =
{
	"DateType2", NULL, NULL, (DaoFuncItem*) conDateMeths, {0}, {0},
	(FuncPtrDel) NULL, NULL
};



static void TIME_GetHour( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, self->time.hour );
}
static void TIME_GetMinute( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, self->time.minute );
}
static void TIME_GetSecond( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutFloat( proc, self->time.second );
}

static void TIME_SetHour( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	int value = p[1]->xInteger.value;
	if( value < 0 || value >= 24 ){
		DaoProcess_RaiseError( proc, "Param", "Invalid hour" );
		return;
	}
	self->time.hour = value;
}
static void TIME_SetMinute( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	int value = p[1]->xInteger.value;
	if( value < 0 || value >= 60 ){
		DaoProcess_RaiseError( proc, "Param", "Invalid minutes" );
		return;
	}
	self->time.minute = value;
}
static void TIME_SetSecond( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *self = (DaoTime*) p[0]->xCinValue.value;
	dao_float value = p[1]->xFloat.value;
	if( value < 0.0 || value >= 60.0 ){
		DaoProcess_RaiseError( proc, "Param", "Invalid seconds" );
		return;
	}
	self->time.second = value;
}

static DaoFuncItem absTimeMeths[] =
{
//	{ NULL,  ".year( self: TimeType ) => int" },
//	{ NULL,  ".month( self: TimeType ) => int" },
//	{ NULL,  ".day( self: TimeType ) => int" },
	{ NULL,  ".hour( self: TimeType ) => int" },
	{ NULL,  ".minute( self: TimeType ) => int" },
	{ NULL,  ".second( self: TimeType ) => float" },

//	{ NULL,  ".year=( self: TimeType , value: int )" },
//	{ NULL,  ".month=( self: TimeType , value: int )" },
//	{ NULL,  ".day=( self: TimeType , value: int )" },
	{ NULL,  ".hour=( self: TimeType , value: int )" },
	{ NULL,  ".minute=( self: TimeType , value: int )" },
	{ NULL,  ".second=( self: TimeType , value: float )" },
	{ NULL, NULL }
};

DaoTypeBase absTimeTyper =
{
	"TimeType", NULL, NULL, (DaoFuncItem*) absTimeMeths, {&absDateTyper, NULL}, {0},
	(FuncPtrDel) NULL, NULL
};


static void TIME_GetYear( DaoProcess *proc, DaoValue *p[], int N )
{
	DaoTime *time = (DaoTime*) p[0]->xCinValue.value;
	DaoProcess_PutInteger( proc, time->time.year );
}

static DaoFuncItem conTimeMeths[] =
{
//	{ DATE_GetYear,    ".year( self: TimeType2 ) => int" },
//	{ DATE_GetMonth,   ".month( self: TimeType2 ) => int" },
//	{ DATE_GetDay,     ".day( self: TimeType2 ) => int" },
	{ TIME_GetHour,    ".hour( self: TimeType2 ) => int" },
	{ TIME_GetMinute,  ".minute( self: TimeType2 ) => int" },
	{ TIME_GetSecond,  ".second( self: TimeType2 ) => float" },

//	{ DATE_SetYear,    ".year=( self: TimeType2 , value: int )" },
//	{ DATE_SetMonth,   ".month=( self: TimeType2 , value: int )" },
//	{ DATE_SetDay,     ".day=( self: TimeType2 , value: int )" },
	{ TIME_SetHour,    ".hour=( self: TimeType2 , value: int )" },
	{ TIME_SetMinute,  ".minute=( self: TimeType2 , value: int )" },
	{ TIME_SetSecond,  ".second=( self: TimeType2 , value: float )" },
	{ NULL, NULL }
};

DaoTypeBase conTimeTyper =
{
	"TimeType2", NULL, NULL, (DaoFuncItem*) conTimeMeths, {&conDateTyper, NULL}, {0},
	(FuncPtrDel) NULL, NULL
};


int DaoSQL_OnLoad( DaoVmSpace * vms, DaoNamespace *ns )
{
	char *lang = getenv( "DAO_HELP_LANG" );
	DaoTypeBase *typers[] = { & DaoSQLDatabase_Typer, & DaoSQLHandle_Typer, NULL };
	DaoNamespace *timens = DaoVmSpace_LinkModule( vms, ns, "time" );
	DaoNamespace *timens2 = DaoVmSpace_GetNamespace( vms, "time" );
	DaoNamespace *sqlns = DaoVmSpace_GetNamespace( vms, "SQL" );
	DaoType *absdate = DaoNamespace_WrapInterface( sqlns, & absDateTyper );
	DaoType *abstime = DaoNamespace_WrapInterface( sqlns, & absTimeTyper );
	DaoType *condate = DaoNamespace_WrapCinType( sqlns, & conDateTyper, absdate, _DaoTime_Type() );
	DaoType *contime = DaoNamespace_WrapCinType( sqlns, & conTimeTyper, abstime, _DaoTime_Type() );
	DaoMap *engines;

	DaoNamespace_AddConstValue( ns, "SQL", (DaoValue*) sqlns );
	DaoNamespace_AddConstValue( sqlns, "time", (DaoValue*) timens2 );

	DaoNamespace_DefineType( sqlns, "$SQLite",     "SQLite" );
	DaoNamespace_DefineType( sqlns, "$PostgreSQL", "PostgreSQL" );
	DaoNamespace_DefineType( sqlns, "$MySQL",      "MySQL" );

	DaoNamespace_DefineType( sqlns, "int", "COUNT" );
	DaoNamespace_DefineType( sqlns, "int", "SUM_INT" );
	DaoNamespace_DefineType( sqlns, "float", "SUM_FLOAT" );

	DaoNamespace_DefineType( sqlns, "int", "INTEGER" );
	DaoNamespace_DefineType( sqlns, "int", "SMALLINT" );
	dao_sql_type_bigint = DaoNamespace_DefineType( sqlns, "int", "BIGINT" );

	dao_sql_type_integer_primary_key
		= DaoNamespace_DefineType( sqlns, "int", "INTEGER_PRIMARY_KEY" );
	dao_sql_type_integer_primary_key_auto_increment
		= DaoNamespace_DefineType( sqlns, "int", "INTEGER_PRIMARY_KEY_AUTO_INCREMENT" );

	dao_sql_type_real = DaoNamespace_DefineType( sqlns, "float", "REAL" );
	dao_sql_type_float = DaoNamespace_DefineType( sqlns, "float", "FLOAT" );
	dao_sql_type_double = DaoNamespace_DefineType( sqlns, "float", "DOUBLE_PRECISION" );

	DaoNamespace_DefineType( sqlns, "string", "TEXT" );
	DaoNamespace_DefineType( sqlns, "string", "MEDIUMTEXT" );
	DaoNamespace_DefineType( sqlns, "string", "LONGTEXT" );
	DaoNamespace_DefineType( sqlns, "string", "BLOB" );
	DaoNamespace_DefineType( sqlns, "string", "MEDIUMBLOB" );
	DaoNamespace_DefineType( sqlns, "string", "LONGBLOB" );

	/*
	// For char(x) or varchar(x) with any x, one can use:
	// type charx = string
	// type varcharx = string
	*/
	DaoNamespace_DefineType( sqlns, "string", "CHAR1" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR2" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR4" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR8" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR12" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR16" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR24" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR32" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR64" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR128" );
	DaoNamespace_DefineType( sqlns, "string", "CHAR256" );

	DaoNamespace_DefineType( sqlns, "string", "VARCHAR2" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR4" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR8" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR12" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR16" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR24" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR32" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR64" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR128" );
	DaoNamespace_DefineType( sqlns, "string", "VARCHAR256" );

	dao_sql_type_date = DaoNamespace_DefineType( sqlns, "DateType<time::DateTime>", "DATE" );
	dao_sql_type_timestamp = DaoNamespace_DefineType( sqlns, "TimeType<time::DateTime>", "TIMESTAMP" );
	dao_type_datetime = _DaoTime_Type();

	DaoNamespace_WrapTypes( sqlns, typers );
	DaoNamespace_WrapFunctions( sqlns, sqlMeths );

	engines = DaoMap_New(0);
	DaoMap_SetType( engines, DaoNamespace_ParseType( sqlns, "map<string,any>" ) );

	/*
	// Cannot add constant, otherwise the bytecode encoder will not work for:
	//   load sqlite;
	//   io.writeln( SQL::Engines )
	// Because it will not be able to encode "Database<SQLite>"!
	*/
	DaoNamespace_AddValue( sqlns, "Engines", (DaoValue*) engines, NULL );

	if( lang ){
		char fname[100] = "help_module_official_sql_";
		strcat( fname, lang );
		DaoVmSpace_Load( vms, fname );
	}
	return 0;
}
