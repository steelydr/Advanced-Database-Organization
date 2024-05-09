#include <stdlib.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"


#define ASSERT_EQUALS_RECORDS(_l,_r, schema, message)			\
		do {									\
			Record *_lR = _l;                                                   \
			Record *_rR = _r;                                                   \
			ASSERT_TRUE(memcmp(_lR->data,_rR->data,getRecordSize(schema)) == 0, message); \
			int i;								\
			for(i = 0; i < schema->numAttr; i++)				\
			{									\
				Value *lVal, *rVal;                                             \
				char *lSer, *rSer; \
				getAttr(_lR, schema, i, &lVal);                                  \
				getAttr(_rR, schema, i, &rVal);                                  \
				lSer = serializeValue(lVal); \
				rSer = serializeValue(rVal); \
				ASSERT_EQUALS_STRING(lSer, rSer, "attr same");	\
				free(lVal); \
				free(rVal); \
				free(lSer); \
				free(rSer); \
			}									\
		} while(0)

#define ASSERT_EQUALS_RECORD_IN(_l,_r, rSize, schema, message)		\
		do {									\
			int i;								\
			boolean found = false;						\
			for(i = 0; i < rSize; i++)						\
			if (memcmp(_l->data,_r[i]->data,getRecordSize(schema)) == 0)	\
			found = true;							\
			ASSERT_TRUE(0, message);						\
		} while(0)

#define OP_TRUE(left, right, op, message)		\
		do {							\
			Value *result = (Value *) malloc(sizeof(Value));	\
			op(left, right, result);				\
			bool b = result->v.boolV;				\
			free(result);					\
			ASSERT_TRUE(b,message);				\
		} while (0)


// struct for test records
typedef struct TestRecord {
	int a;
	char *b;
	int c;
} TestRecord;


// helper methods
Record *testRecord(Schema *schema, int a, char *b, int c);
Schema *testSchema (void);
Record *fromTestRecord (Schema *schema, TestRecord in);
static void testPrimaryKeyConstraint();
static void testUpdateRecord();
// test name
char *testName;

int main(void) {
    testName = "";
    testPrimaryKeyConstraint();
    return 0;
}
void testPrimaryKeyConstraint(void) {
    RM_TableData *table = (RM_TableData *) malloc(sizeof(RM_TableData));
    TestRecord inserts[] = {
        {1, "aaaa", 3},
        {3, "bbbb", 2}, 
        {1, "ss", 5},// Duplicate primary key value
        {9,"s",9},
        {3, "bb", 4},// Duplicate primary key value
        {4,"s",9}
    };
    int numInserts = 4;
    Record *r;
    Schema *schema;
    RID *rids = (RID *) malloc(sizeof(RID) * numInserts); // Declare rids
    testName = "test primary key and null value constraint";
    schema = testSchema();

    TEST_CHECK(initRecordManager(NULL));
    TEST_CHECK(createTable("test_table_r", schema));
    TEST_CHECK(openTable(table, "test_table_r"));

    // Insert first record
    r = fromTestRecord(schema, inserts[0]);
    TEST_CHECK(insertRecord(table, r));
    rids[0] = r->id; // Store the RID
    freeRecord(r);
    
    // Insert first record
    r = fromTestRecord(schema, inserts[1]);
    TEST_CHECK(insertRecord(table, r));
    rids[1] = r->id; // Store the RID
    freeRecord(r);

    // Insert record with duplicate primary key
    r = fromTestRecord(schema, inserts[2]);
    ASSERT_ERROR(insertRecord(table, r), "Duplicate primary key should not be allowed");
    freeRecord(r);
    
    // Insert first record
    r = fromTestRecord(schema, inserts[3]);
    TEST_CHECK(insertRecord(table, r));
    rids[3] = r->id; // Store the RID
    freeRecord(r);
    
    r = fromTestRecord(schema, inserts[4]);
    ASSERT_ERROR(insertRecord(table, r), "Duplicate primary key should not be allowed");
    freeRecord(r);
    
     r = fromTestRecord(schema, inserts[5]);
    TEST_CHECK(insertRecord(table, r));
    rids[5] = r->id; // Store the RID
    freeRecord(r);
    
    TEST_CHECK(closeTable(table));
    TEST_CHECK(deleteTable("test_table_r"));
    TEST_CHECK(shutdownRecordManager());
    free(table);
    free(rids); // Free the rids array
    freeSchema(schema);
    TEST_DONE();
}

Schema *
testSchema (void)
{
	Schema *result;
	char *names[] = { "a", "b", "c" };
	DataType dt[] = { DT_INT, DT_STRING, DT_INT };
	int sizes[] = { 0, 4, 0 };
	int keys[] = {0};
	int i;
	char **cpNames = (char **) malloc(sizeof(char*) * 3);
	DataType *cpDt = (DataType *) malloc(sizeof(DataType) * 3);
	int *cpSizes = (int *) malloc(sizeof(int) * 3);
	int *cpKeys = (int *) malloc(sizeof(int));

	for(i = 0; i < 3; i++)
	{
		cpNames[i] = (char *) malloc(2);
		strcpy(cpNames[i], names[i]);
	}
	memcpy(cpDt, dt, sizeof(DataType) * 3);
	memcpy(cpSizes, sizes, sizeof(int) * 3);
	memcpy(cpKeys, keys, sizeof(int));

	result = createSchema(3, cpNames, cpDt, cpSizes, 1, cpKeys);

	return result;
}
Record *
fromTestRecord (Schema *schema, TestRecord in)
{
	return testRecord(schema, in.a, in.b, in.c);
}

Record *
testRecord(Schema *schema, int a, char *b, int c)
{
	Record *result;
	Value *value;

	TEST_CHECK(createRecord(&result, schema));

	MAKE_VALUE(value, DT_INT, a);
	TEST_CHECK(setAttr(result, schema, 0, value));
	freeVal(value);

	MAKE_STRING_VALUE(value, b);
	TEST_CHECK(setAttr(result, schema, 1, value));
	freeVal(value);

	MAKE_VALUE(value, DT_INT, c);
	TEST_CHECK(setAttr(result, schema, 2, value));
	freeVal(value);

	return result;
}
