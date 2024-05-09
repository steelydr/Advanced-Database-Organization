#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include<float.h>
#include<limits.h>

#include <pthread.h>
#define RC_RECORD_NULL_CONSTRAINT_VIOLATED 718
// Define a mutex to ensure thread safety
static pthread_mutex_t table_mutex = PTHREAD_MUTEX_INITIALIZER;

#define BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id) \
TableMetadata *table = getTableMetadata(rel); \
USE_PAGE_HANDLE_HEADER(RC_WRITE_FAILED); \
if (id.page != table->pageNumber) \
{ \
    BEGIN_USE_PAGE_HANDLE_HEADER(id.page) \
} \
else \
{ \
        handle = *table->handle; \
    header = getPageHeader(&handle); \
}

#define BEGIN_USE_PAGE_HANDLE_HEADER(pageNum) \
    result = pinPage(&bufferPool, &handle, pageNum); \
    if (result != RC_OK) return error; \
    header = getPageHeader(&handle);


#define USE_PAGE_HANDLE_HEADER(errorValue) \
    BM_PageHandle handle; \
    RC result; \
    int const error = errorValue; \
    PageHeader *header;
    
typedef struct ScanData {
    int numAttrs;
    RID id;
    Expr *condition;
    int *attrOrder;  
} ScanData;

#define END_USE_TABLE_PAGE_HANDLE_HEADER() \
if (id.page != table->pageNumber) \
{ \
    END_USE_PAGE_HANDLE_HEADER() \
}

#define END_USE_PAGE_HANDLE_HEADER() \
    result = unpinPage(&bufferPool, &handle); \
    if (result != RC_OK) return error;
    
#define MAX_NUM_KEYS 4
#define PAGE_FILE_NAME "data.txt"
#define TABLE_NAME_SIZE 16
#define MAX_NUM_ATTR 8
#define ATTR_NAME_SIZE 16


#define BEGIN_SLOT_WALK(table) \
BM_PageHandle *handle = table->handle; \
bool* slots; \
int slotIndex = -1; \
int slotResult = 0;

typedef struct TableMetadata {
    char attributeNames[MAX_NUM_ATTR * ATTR_NAME_SIZE];
    char name[TABLE_NAME_SIZE];
    int numAttributes;
    DataType dataTypes[MAX_NUM_ATTR];
    int keySize;
    int pageNumber;
    int keyAttributes[MAX_NUM_KEYS];
    int numTuples;
    int attributeLengths[MAX_NUM_ATTR];
    BM_PageHandle *handle;
} TableMetadata;

#define RC_IM_ATTR_TYPE_UNKNOWN 600
#define RC_MEMORY_ALLOCATION_FAILED 601
#define RC_PAGE_HEADER_NOT_FOUND 700
#define RC_PAGE_HANDLE_NOT_FOUND 701
#define RC_INVALID_RECORD 602
#define RC_INVALID_SCHEMA 603
#define RC_TABLE_IS_EMPTY 702
#define MAX_NUM_TABLES (PAGE_SIZE / (sizeof(TableMetadata) + sizeof(int) * 2))
#define RC_OUT_OF_MEMORY 703
#define RC_TABLE_METADATA_NOT_FOUND 704
#define RC_SYSTEM_CATALOG_ERROR 705
#define RC_TABLE_HANDLE_NOT_FOUND 706
#define RC_INVALID_ARGUMENT 707
#define RC_TABLE_NOT_FOUND 708
#define RC_TABLE_ALREADY_OPEN 709
#define RC_MALLOC_FAILED 710
#define RC_TABLE_ALREADY_EXISTS 711
#define RC_LIMIT_EXCEEDED 712
#define RC_PAGE_ALLOCATION_FAILED 713
#define RC_RECORD_SIZE_TOO_LARGE 714
#define RC_PAGE_INITIALIZATION_FAILED 715
#define RC_SYSTEM_CONFIG_ERROR 716
#define PAGE_HEADER_SIZE 64  
#define RC_INVALID_SCAN_HANDLE 717



BM_BufferPool bufferPool;
BM_PageHandle catalogPageHandle;

typedef struct PageHeader {
    int nextPageNumber;
    int prevPageNumber;
    int numSlots;
} PageHeader;

typedef struct SystemCatalog {
    int totalNumPages;
    int freePageNumber;
    int numTables;
    TableMetadata tables[MAX_NUM_TABLES];
    int nullBitmapPageNumber;  
} SystemCatalog;

SystemCatalog* getSystemCatalog();
RC markSystemCatalogDirty();
int setFreePage(BM_PageHandle* handle);
TableMetadata *getTableByName(char *name);
int calculateOffset(Schema* schema, int attributeNum);
int closeSlotWalk(TableMetadata *table, BM_PageHandle **handle);
bool *getSlots(BM_PageHandle* handle);
char *getTupleData(BM_PageHandle* handle);
int getFreePageNumber();
PageHeader *getPageHeader(BM_PageHandle* handle);
int appendToFreeList(int pageNumber);
int getNextSlotInWalk(TableMetadata *table, BM_PageHandle **handle, bool** slots, int *slotIndex);
RC validateSystemConfig();
void handleResult(const char *message, RC result, const char *fileName);
int getAttributeSize(Schema *schema, int attributeIndex);


RC markSystemCatalogDirty() {
    // Mark the catalog page as dirty in the buffer pool
    return markDirty(&bufferPool, &catalogPageHandle);
}


SystemCatalog* getSystemCatalog() {
    // Return the system catalog stored in the catalog page handle
    return (SystemCatalog*)catalogPageHandle.data;
}


PageHeader* getPageHeader(BM_PageHandle* handle) {
    // Return the page header from the page handle
    return (PageHeader*)handle->data;
}

TableMetadata* getTableByName(char* name) {
    // Check if the name argument is valid
    if (name == NULL) {
        return NULL;
    }

    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&table_mutex);

    // Get the system catalog
    SystemCatalog* catalog = getSystemCatalog();
    if (catalog == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&table_mutex);
        return NULL;
    }

    // Initialize pointers
    TableMetadata* table = catalog->tables;
    TableMetadata* endPtr = table + catalog->numTables;

    // Search for the table in the system catalog
    while (table < endPtr) {
        if (strcmp(table->name, name) == 0) {
            // Unlock the mutex before returning
            pthread_mutex_unlock(&table_mutex);
            return table;
        }
        table++;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&table_mutex);

    // Table not found
    return NULL;
}

// Define a mutex to ensure thread safety
static pthread_mutex_t tuple_mutex = PTHREAD_MUTEX_INITIALIZER;

char* getTupleData(BM_PageHandle* handle) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&tuple_mutex);

    // Get the page header
    PageHeader* header = getPageHeader(handle);

    // Unlock the mutex before returning
    pthread_mutex_unlock(&tuple_mutex);

    // Return the tuple data area
    return (char*)(getSlots(handle) + sizeof(bool) * header->numSlots);
}



// Define a mutex for getSlots function
static pthread_mutex_t slots_mutex = PTHREAD_MUTEX_INITIALIZER;

bool* getSlots(BM_PageHandle* handle) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&slots_mutex);

    // Return the slot array from the page handle
    bool* slots = (bool*)(handle->data + PAGE_HEADER_SIZE);

    // Unlock the mutex before returning
    pthread_mutex_unlock(&slots_mutex);

    return slots;
}

// Define a mutex for getTableMetadata function
static pthread_mutex_t metadata_mutex = PTHREAD_MUTEX_INITIALIZER;

TableMetadata* getTableMetadata(RM_TableData* rel) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&metadata_mutex);

    // Return the table metadata from the RM_TableData structure
    TableMetadata* metadata = (TableMetadata*)rel->mgmtData;

    // Unlock the mutex before returning
    pthread_mutex_unlock(&metadata_mutex);

    return metadata;
}

int setFreePage(BM_PageHandle* handle)
{
    // TODO: implement this for overflow pages
    return 1;
}


// Define a mutex for getTupleDataAt function
static pthread_mutex_t tuple_data_mutex = PTHREAD_MUTEX_INITIALIZER;

char* getTupleDataAt(BM_PageHandle* handle, int recordSize, int index) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&tuple_data_mutex);

    // Get the tuple data area
    char* tupleData = getTupleData(handle);

    // Unlock the mutex before returning
    pthread_mutex_unlock(&tuple_data_mutex);

    // Return the pointer to the tuple data at the given index
    return tupleData + index * recordSize;
}

// Define a mutex for getFreePageNumber function
static pthread_mutex_t free_page_mutex = PTHREAD_MUTEX_INITIALIZER;

int getFreePageNumber() {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&free_page_mutex);

    // Use the page handle header
    USE_PAGE_HANDLE_HEADER(NO_PAGE);

    // Get the system catalog
    SystemCatalog* catalog = getSystemCatalog();

    // Get the free page number from the system catalog
    int newPageNumber = catalog->freePageNumber;
    bool createNewPage = (newPageNumber == NO_PAGE);

    // If there's no free page, create a new one
    if (createNewPage) {
        newPageNumber = catalog->totalNumPages++;
        markSystemCatalogDirty();
    }

    // Use the new page handle header
    BEGIN_USE_PAGE_HANDLE_HEADER(newPageNumber) {
        // Get the next page number from the header
        int nextPageNumber = header->nextPageNumber;

        // Initialize the header
        header->nextPageNumber = NO_PAGE;
        header->prevPageNumber = NO_PAGE;
        markDirty(&bufferPool, &handle);

        // If a new page was created, there's nothing else to do
        if (createNewPage) {
            // Do nothing
        } else {
            // Update the free page number in the system catalog
            catalog->freePageNumber = nextPageNumber;
            markSystemCatalogDirty();

            // If there's a next page, update its previous page number
            if (nextPageNumber != NO_PAGE) {
                BEGIN_USE_PAGE_HANDLE_HEADER(nextPageNumber) {
                    header->prevPageNumber = 0;
                    markDirty(&bufferPool, &handle);
                } END_USE_PAGE_HANDLE_HEADER();
            }
        }
    } END_USE_PAGE_HANDLE_HEADER();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&free_page_mutex);

    return newPageNumber;
}

int closeSlotWalk(TableMetadata *table, BM_PageHandle **handle) {
    // Check if the input arguments are valid
    if (table == NULL || handle == NULL || *handle == NULL) {
        fprintf(stderr, "Error: Invalid arguments passed to closeSlotWalk\n");
        return 1;
    }

    // Get the page handle
    BM_PageHandle *pageHandle = *handle;
    int pageNum = pageHandle->pageNum;

    // If the page handle is not for the table's page, unpin it
    if (table->pageNumber != pageNum) {
        RC result = unpinPage(&bufferPool, pageHandle);
        if (result != RC_OK) {
            fprintf(stderr, "Error: Failed to unpin page %d. Error code: %d\n", pageNum, result);
            return 1;
        }
        *handle = NULL;
    }

    return 0; // Return success
}


// Define a mutex for getNextPage function
static pthread_mutex_t next_page_mutex = PTHREAD_MUTEX_INITIALIZER;

int getNextPage(TableMetadata *table, int pageNumber) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&next_page_mutex);

    // If the page number is the table's page number, return the next page number from the table's page handle
    if (pageNumber == table->pageNumber) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&next_page_mutex);
        return getPageHeader(table->handle)->nextPageNumber;
    }

    // Initialize the next page number to NO_PAGE
    int nextPageNumber = NO_PAGE;

    // Use the page handle header
    USE_PAGE_HANDLE_HEADER(NO_PAGE);

    // Get the next page number from the given page number
    BEGIN_USE_PAGE_HANDLE_HEADER(pageNumber) {
        nextPageNumber = header->nextPageNumber;
        // If the next page number is invalid, set it to NO_PAGE
        if (nextPageNumber != NO_PAGE) {
            if (nextPageNumber < 0) {
                fprintf(stderr, "Error: Invalid next page number %d for page %d\n", nextPageNumber, pageNumber);
                nextPageNumber = NO_PAGE;
            }
        }
    } END_USE_PAGE_HANDLE_HEADER();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&next_page_mutex);

    return nextPageNumber;
}


// Define a mutex for appendToFreeList function
static pthread_mutex_t append_mutex = PTHREAD_MUTEX_INITIALIZER;

int appendToFreeList(int pageNumber) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&append_mutex);

    // Use the page handle header
    USE_PAGE_HANDLE_HEADER(1);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Set the initial values
    int currentPage = pageNumber;
    int prevPage = 0;
    int nextPage = catalog->freePageNumber;

    // Traverse the free page list to the end
    while (nextPage != NO_PAGE) {
        BEGIN_USE_PAGE_HANDLE_HEADER(nextPage) {
            prevPage = nextPage;
            nextPage = header->nextPageNumber;
        } END_USE_PAGE_HANDLE_HEADER();
    }

    // Append the new page to the end of the list
    BEGIN_USE_PAGE_HANDLE_HEADER(currentPage) {
        header->prevPageNumber = prevPage;
        header->nextPageNumber = NO_PAGE;
        markDirty(&bufferPool, &handle);
    } END_USE_PAGE_HANDLE_HEADER();

    if (prevPage == 0) {
        // The new page becomes the free list
        catalog->freePageNumber = pageNumber;
    } else {
        // Update the previous page in the list
        BEGIN_USE_PAGE_HANDLE_HEADER(prevPage) {
            header->nextPageNumber = pageNumber;
            markDirty(&bufferPool, &handle);
        } END_USE_PAGE_HANDLE_HEADER();
    }

    markSystemCatalogDirty();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&append_mutex);

    return 0;
}

int getNextSlotInWalk(TableMetadata *table, BM_PageHandle **handle, bool **slots, int *slotIndex) {
    // Obtain the page header
    PageHeader *header = getPageHeader(*handle);
    
    // Get the slots
    RC result;
    *slots = getSlots(*handle);

    // Calculate remaining slots
    int remainingSlots = header->numSlots - *slotIndex;

    // Switch based on remaining slots
    switch (remainingSlots) {
        case 0:
            *slotIndex = 0;
            int nextPageNumber = header->nextPageNumber;

            // Unpin the current page if necessary
            result = (table->pageNumber != (*handle)->pageNum && nextPageNumber != NO_PAGE)
                     ? unpinPage(&bufferPool, *handle)
                     : RC_OK;

            // If there is a next page, pin it and recursively call getNextSlotInWalk
            if (result == RC_OK && nextPageNumber != NO_PAGE) {
                result = pinPage(&bufferPool, *handle, nextPageNumber);
                if (result == RC_OK) {
                    return getNextSlotInWalk(table, handle, slots, slotIndex);
                }
            }

            // Return -1 if there is no next page, otherwise return 1
            return (nextPageNumber == NO_PAGE) ? -1 : 1;

        default:
            // Increment the slot index and return 0
            (*slotIndex)++;
            return 0;
    }
}



// Define a mutex for initRecordManager function
static pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

RC initRecordManager(void *mgmtData) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&init_mutex);

    RC result;
    char *fileName;
    bool newSystem;
    SystemCatalog *catalog;

    // Validate system configuration
    result = validateSystemConfig();
    switch (result) {
        case RC_OK:
            break;
        default:
            fprintf(stderr, "Error: System catalog size or maximum table limit invalid\n");
            // Unlock the mutex before returning
            pthread_mutex_unlock(&init_mutex);
            return RC_SYSTEM_CONFIG_ERROR;
    }

    // Determine page file name
    fileName = mgmtData ? (char *)mgmtData : PAGE_FILE_NAME;

    // Check if file needs to be created and create if necessary
    newSystem = (access(fileName, F_OK) != 0);
    result = newSystem ? createPageFile(fileName) : RC_OK;
    handleResult("Failed to create page file '%s'", result, fileName);

    // Initialize buffer pool
    result = initBufferPool(&bufferPool, fileName, 16, RS_LRU, NULL);
    handleResult("Failed to initialize buffer pool", result, NULL);

    // Pin catalog page
    result = pinPage(&bufferPool, &catalogPageHandle, 0);
    handleResult("Failed to pin catalog page", result, NULL);
    if (result != RC_OK) {
        shutdownBufferPool(&bufferPool);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&init_mutex);
        return result;
    }

    // Create system schema if it's a new file
    catalog = getSystemCatalog();
    catalog->totalNumPages = newSystem ? 1 : catalog->totalNumPages;
    catalog->freePageNumber = newSystem ? NO_PAGE : catalog->freePageNumber;
    catalog->numTables = newSystem ? 0 : catalog->numTables;
    markSystemCatalogDirty();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&init_mutex);

    return RC_OK;
}
RC validateSystemConfig() {
    // Check if the page size is smaller than the system catalog size or the maximum number of tables is invalid
    return (PAGE_SIZE < sizeof(SystemCatalog) || MAX_NUM_TABLES <= 0) ? RC_SYSTEM_CONFIG_ERROR : RC_OK;
}

void handleResult(const char *message, RC result, const char *fileName) {
    // Print an error message if the result is not successful
    if (result != RC_OK) {
        if (fileName) {
            fprintf(stderr, "Error: %s. Error code: %d\n", message, result);
        } else {
            fprintf(stderr, "Error: %s. Error code: %d\n", message, result);
        }
    }
}

RC shutdownRecordManager() {
    RC result = unpinPage(&bufferPool, &catalogPageHandle);
    if (result != RC_OK) {
        fprintf(stderr, "Error: Failed to unpin catalog page. Error code: %d\n", result);
    }

    result = shutdownBufferPool(&bufferPool);
    if (result != RC_OK) {
        fprintf(stderr, "Error: Failed to shutdown buffer pool. Error code: %d\n", result);
    }

    return result;
}


// Define a mutex for createTable function
static pthread_mutex_t create_table_mutex = PTHREAD_MUTEX_INITIALIZER;

RC createTable(char *name, Schema *schema) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&create_table_mutex);

    if (name == NULL || strlen(name) == 0 || schema == NULL) {
        fprintf(stderr, "Error: Invalid arguments provided to createTable\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_INVALID_ARGUMENT;
    }

    SystemCatalog *catalog = getSystemCatalog();
    if (catalog == NULL) {
        fprintf(stderr, "Error: Failed to get system catalog\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_SYSTEM_CATALOG_ERROR;
    }

    // Check if table already exists
    TableMetadata *existingTable = getTableByName((char *)name);
    if (existingTable != NULL) {
        fprintf(stderr, "Error: Table '%s' already exists\n", name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_TABLE_ALREADY_EXISTS;
    }

    // Check if catalog can hold another table and schema is valid
    if (catalog->numTables >= MAX_NUM_TABLES || schema->numAttr > MAX_NUM_ATTR || schema->keySize > MAX_NUM_KEYS) {
        fprintf(stderr, "Error: Maximum number of tables or attributes reached\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_LIMIT_EXCEEDED;
    }

    // Create a new table metadata
    TableMetadata *table = &(catalog->tables[catalog->numTables]);
    memset(table, 0, sizeof(TableMetadata));
    strncpy(table->name, name, TABLE_NAME_SIZE - 1);
    table->numAttributes = schema->numAttr;
    table->handle = NULL;
    table->numTuples = 0;
    
    int i = 0;
    while (i < table->numAttributes) {
        table->dataTypes[i] = schema->dataTypes[i];
        table->attributeLengths[i] = schema->typeLength[i];
        strncpy(&(table->attributeNames[i * ATTR_NAME_SIZE]), schema->attrNames[i], ATTR_NAME_SIZE - 1);
        i++;
    }

    // Copy key data
    table->keySize = schema->keySize;
    memcpy(table->keyAttributes, schema->keyAttrs, sizeof(int) * schema->keySize);

    catalog->numTables++;
    int slotSize = sizeof(bool);
    int pageHeaderSize = sizeof(PageHeader);
    
    // Allocate a new page for the table
    table->pageNumber = getFreePageNumber();
    if (table->pageNumber == NO_PAGE) {
        fprintf(stderr, "Error: Failed to allocate page for table '%s'\n", name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_PAGE_ALLOCATION_FAILED;
    }

    // Initialize the page
    int recordSize = getRecordSize(schema);
    int recordsPerPage = (PAGE_SIZE - pageHeaderSize) / (recordSize + slotSize);
    if (!(recordsPerPage > 0)) {
        fprintf(stderr, "Error: Record size too large for table '%s'\n", name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&create_table_mutex);
        return RC_RECORD_SIZE_TOO_LARGE;
    }

    USE_PAGE_HANDLE_HEADER(RC_PAGE_INITIALIZATION_FAILED);

    BEGIN_USE_PAGE_HANDLE_HEADER(table->pageNumber);
    {
        // Mark all the slots as free
        bool *slots = getSlots(&handle);
        header->numSlots = recordsPerPage;
        memset(slots, FALSE, sizeof(bool) * recordsPerPage);

        markDirty(&bufferPool, &handle);
    }
    END_USE_PAGE_HANDLE_HEADER();

    markSystemCatalogDirty();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&create_table_mutex);

    return RC_OK;
}


// Define a mutex for openTable function
static pthread_mutex_t open_table_mutex = PTHREAD_MUTEX_INITIALIZER;

RC openTable(RM_TableData *rel, char *name) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&open_table_mutex);

    if (rel == NULL || name == NULL || strlen(name) == 0) {
        fprintf(stderr, "Error: Invalid arguments provided to openTable\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_INVALID_ARGUMENT;
    }

    TableMetadata *table = getTableByName((char *)name);
    if (table == NULL) {
        fprintf(stderr, "Error: Table '%s' not found\n", name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_TABLE_NOT_FOUND;
    }

    if (table->handle != NULL) {
        fprintf(stderr, "Error: Table '%s' is already open\n", name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_TABLE_ALREADY_OPEN;
    }

    // Initialize the RM_TableData structure
    rel->name = strdup(table->name);
    if (rel->name == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for table name\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    rel->schema = (Schema *)malloc(sizeof(Schema));
    if (rel->schema == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for schema\n");
        free(rel->name);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    rel->schema->numAttr = table->numAttributes;
    rel->schema->attrNames = (char **)malloc(sizeof(char *) * table->numAttributes);
    if (rel->schema->attrNames == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for attribute names\n");
        free(rel->name);
        free(rel->schema);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    rel->schema->dataTypes = (DataType *)malloc(sizeof(DataType) * table->numAttributes);
    if (rel->schema->dataTypes == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for data types\n");
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    rel->schema->typeLength = (int *)malloc(sizeof(int) * table->numAttributes);
    if (rel->schema->typeLength == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for type lengths\n");
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    // Copy attribute data
    for (int i = 0; i < table->numAttributes; i++) {
        rel->schema->attrNames[i] = strdup(&table->attributeNames[i * ATTR_NAME_SIZE]);
        if (rel->schema->attrNames[i] == NULL) {
            fprintf(stderr, "Error: Memory allocation failed for attribute name\n");
            for (int j = 0; j < i; j++) {
                free(rel->schema->attrNames[j]);
            }
            free(rel->name);
            free(rel->schema->attrNames);
            free(rel->schema->dataTypes);
            free(rel->schema->typeLength);
            free(rel->schema);
            // Unlock the mutex before returning
            pthread_mutex_unlock(&open_table_mutex);
            return RC_MALLOC_FAILED;
        }
        rel->schema->dataTypes[i] = table->dataTypes[i];
        rel->schema->typeLength[i] = table->attributeLengths[i];
    }

    // Copy key data
    rel->schema->keySize = table->keySize;
    rel->schema->keyAttrs = (int *)malloc(sizeof(int) * table->keySize);
    if (rel->schema->keyAttrs == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for key attributes\n");
        for (int i = 0; i < table->numAttributes; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema->typeLength);
        free(rel->schema);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }
    memcpy(rel->schema->keyAttrs, table->keyAttributes, sizeof(int) * table->keySize);

    // Point to the system schema
    rel->mgmtData = (void *)table;

    // Allocate memory for the page handle
    table->handle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    if (table->handle == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for page handle\n");
        for (int i = 0; i < table->numAttributes; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema->typeLength);
        free(rel->schema->keyAttrs);
        free(rel->schema);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return RC_MALLOC_FAILED;
    }

    // Pin the table's page
    RC result = pinPage(&bufferPool, table->handle, table->pageNumber);
    if (result != RC_OK) {
        fprintf(stderr, "Error: Failed to pin page for table '%s'\n", name);
        for (int i = 0; i < table->numAttributes; i++) {
            free(rel->schema->attrNames[i]);
        }
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema->typeLength);
        free(rel->schema->keyAttrs);
        free(rel->schema);
        free(table->handle);
        table->handle = NULL;
        // Unlock the mutex before returning
        pthread_mutex_unlock(&open_table_mutex);
        return result;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&open_table_mutex);

    return RC_OK;
}


// Define a mutex for closeTable function
static pthread_mutex_t close_table_mutex = PTHREAD_MUTEX_INITIALIZER;

RC closeTable(RM_TableData *rel) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&close_table_mutex);

    if (rel == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&close_table_mutex);
        return RC_TABLE_HANDLE_NOT_FOUND;
    }

    TableMetadata *table = getTableMetadata(rel);
    if (table == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&close_table_mutex);
        return RC_TABLE_METADATA_NOT_FOUND;
    }

    // Unpin and force the page to disk
    RC result = unpinPage(&bufferPool, table->handle);
    if (result != RC_OK) {
        // Log the error and handle it accordingly
        fprintf(stderr, "Error unpinning page: %d\n", result);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&close_table_mutex);
        return result;
    }

    result = forcePage(&bufferPool, table->handle);
    if (result != RC_OK) {
        // Log the error and handle it accordingly
        fprintf(stderr, "Error forcing page: %d\n", result);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&close_table_mutex);
        return result;
    }

    // Free memory
    free(rel->schema->attrNames);
    free(rel->schema);
    free(table->handle);
    table->handle = NULL;

    // Unlock the mutex before returning
    pthread_mutex_unlock(&close_table_mutex);

    return RC_OK;
}


// Define a mutex for deleteTable function
static pthread_mutex_t delete_table_mutex = PTHREAD_MUTEX_INITIALIZER;

RC deleteTable(char *name) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&delete_table_mutex);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog was retrieved successfully
    if (catalog == NULL) {
        // If the system catalog cannot be retrieved, print an error message
        fprintf(stderr, "Error: Failed to get system catalog\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&delete_table_mutex);
        return RC_SYSTEM_CATALOG_ERROR;
    }

    TableMetadata *table = NULL;
    int tableIndex = -1;

    // Find the table with the matching name
    for (int i = 0; i < catalog->numTables; i++) {
        if (strcmp(catalog->tables[i].name, name) == 0) {
            table = &(catalog->tables[i]);
            tableIndex = i;
            break;
        }
    }

    // Check if the table was found
    if (table == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&delete_table_mutex);
        // If the table was not found, return RC_IM_KEY_NOT_FOUND
        return RC_IM_KEY_NOT_FOUND;
    }

    // Put the table's page chain in the free list
    RC result = appendToFreeList(table->pageNumber);
    if (result != RC_OK) {
        // If appending to the free list fails, print an error message
        fprintf(stderr, "Error: Failed to append table pages to free list\n");
        // Unlock the mutex before returning
        pthread_mutex_unlock(&delete_table_mutex);
        return result;
    }

    // Shift entries in the table catalog down
    memmove(&(catalog->tables[tableIndex]), &(catalog->tables[tableIndex + 1]),
            (catalog->numTables - tableIndex - 1) * sizeof(TableMetadata));

    // Decrement the number of tables in the catalog
    catalog->numTables--;

    // Mark the system catalog as dirty
    markSystemCatalogDirty();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&delete_table_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}


// Define a mutex for getNumTuples function
static pthread_mutex_t get_num_tuples_mutex = PTHREAD_MUTEX_INITIALIZER;

int getNumTuples(RM_TableData *rel) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&get_num_tuples_mutex);

    // Check if the relation pointer is NULL
    if (rel == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_tuples_mutex);
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Invalid table data\n");
        return -1;
    }

    // Get the TableMetadata from the relation
    TableMetadata *table = getTableMetadata(rel);

    // Check if the TableMetadata pointer is NULL
    if (table == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_tuples_mutex);
        // If NULL, print an error message and return -2
        fprintf(stderr, "Error: Failed to get table metadata\n");
        return -2;
    }

    // Get the number of tuples from the TableMetadata
    int numTuples = table->numTuples;

    // Check if the number of tuples is less than 0
    if (numTuples < 0) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_tuples_mutex);
        // If negative, print an error message and return -3
        fprintf(stderr, "Error: Invalid number of tuples in table metadata\n");
        return -3;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&get_num_tuples_mutex);

    // If everything is valid, return the number of tuples
    return numTuples;
}


// Define a mutex for getNumPages function
static pthread_mutex_t get_num_pages_mutex = PTHREAD_MUTEX_INITIALIZER;

int getNumPages() {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&get_num_pages_mutex);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog pointer is NULL
    if (catalog == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_pages_mutex);
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Failed to get system catalog\n");
        return -1;
    }

    // Get the total number of pages from the system catalog
    int numPages = catalog->totalNumPages;

    // Check if the number of pages is less than 0
    if (numPages < 0) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_pages_mutex);
        // If negative, print an error message and return -2
        fprintf(stderr, "Error: Invalid number of pages in system catalog\n");
        return -2;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&get_num_pages_mutex);

    // If everything is valid, return the number of pages
    return numPages;
}

// Define a mutex for getNumFreePages function
static pthread_mutex_t get_num_free_pages_mutex = PTHREAD_MUTEX_INITIALIZER;

int getNumFreePages() {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&get_num_free_pages_mutex);

    // Get the page handle header for page 0
    USE_PAGE_HANDLE_HEADER(0);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog pointer is NULL
    if (catalog == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_free_pages_mutex);
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Failed to get system catalog\n");
        return -1;
    }

    // Get the current free page number from the system catalog
    int currentPageNumber = catalog->freePageNumber;
    int count = 0;

    // Check if the current free page number is not NO_PAGE
    if (currentPageNumber != NO_PAGE) {
        do {
            // Get the page handle header for the current page number
            BEGIN_USE_PAGE_HANDLE_HEADER(currentPageNumber);
            {
                // Increment the count of free pages
                count++;
                // Move to the next free page number
                currentPageNumber = header->nextPageNumber;
            }
            END_USE_PAGE_HANDLE_HEADER();
        } while (currentPageNumber != NO_PAGE); // Continue until the end of the free page list
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&get_num_free_pages_mutex);

    // Return the count of free pages
    return count;
}


// Define a mutex for getNumTables function
static pthread_mutex_t get_num_tables_mutex = PTHREAD_MUTEX_INITIALIZER;

int getNumTables() {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&get_num_tables_mutex);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog pointer is NULL
    if (catalog == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_num_tables_mutex);
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Failed to retrieve the system catalog.\n");
        return -1;
    }

    // Get the number of tables from the system catalog
    int numTables = catalog->numTables;

    // Unlock the mutex before returning
    pthread_mutex_unlock(&get_num_tables_mutex);

    // Return the number of tables
    return numTables;
}
RC checkNullValues(Record *record, Schema *schema) {
    // Get the record size based on the schema
    int recordSize = getRecordSize(schema);

    // Initialize the offset to the end of the record data
    int offset = recordSize;

    // Iterate over all attributes in the schema
    for (int attributeIndex = 0; attributeIndex < schema->numAttr; attributeIndex++) {
        // Get the data type of the current attribute
        DataType dataType = schema->dataTypes[attributeIndex];

        // Get the size of the current attribute
        int attributeSize = getAttributeSize(schema, attributeIndex);

        // Update the offset to point to the start of the current attribute
        offset -= attributeSize;

        // Check if the current attribute is NULL based on its data type
        switch (dataType) {
            case DT_INT:
                // If the value is INT_MAX, consider it as NULL
                if (*(int *)(record->data + offset) == INT_MAX) {
                    return RC_RECORD_NULL_CONSTRAINT_VIOLATED;
                }
                break;
            case DT_STRING:
                // If the string is empty, consider it as NULL
                if (strlen(record->data + offset) == 0) {
                    return RC_RECORD_NULL_CONSTRAINT_VIOLATED;
                }
                break;
            case DT_FLOAT:
                // If the value is FLT_MAX, consider it as NULL
                if (*(float *)(record->data + offset) == FLT_MAX) {
                    return RC_RECORD_NULL_CONSTRAINT_VIOLATED;
                }
                break;
            case DT_BOOL:
                // Boolean values can never be NULL
                break;
            default:
                // Unknown data type, return an error
                return RC_RECORD_NULL_CONSTRAINT_VIOLATED;
        }
    }

    // If no NULL values were found, return RC_OK
    return RC_OK;
}

// Define a mutex for the insertRecord function
static pthread_mutex_t insert_record_mutex = PTHREAD_MUTEX_INITIALIZER;

RC insertRecord(RM_TableData *rel, Record *record) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&insert_record_mutex);

    // Get the TableMetadata for the relation
    TableMetadata *table = getTableMetadata(rel);
    
    // Check NULL constraint
    RC result = checkNullValues(record, rel->schema);
    if (result != RC_OK) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&insert_record_mutex);
        return RC_RECORD_NULL_CONSTRAINT_VIOLATED;
    }

    // Check primary key constraint
    result = checkPrimaryKeyConstraint(rel, record);
    if (result != RC_OK) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&insert_record_mutex);
        return result;
    }

    // Start the slot walk to find an available slot
    BEGIN_SLOT_WALK(table);

    for (;;) {
        int newPageNumber;
        slotResult = getNextSlotInWalk(table, &handle, &slots, &slotIndex);

        // Switch based on the result of getNextSlotInWalk
        switch (slotResult) {
            case 0:
                // Case 0: A free slot was found
                break;
            case -1:
                newPageNumber = getFreePageNumber();
                if (newPageNumber != NO_PAGE) {
                    // Pin the new page to the buffer pool
                    RC result = pinPage(&bufferPool, handle, newPageNumber);
                    if (result == RC_OK) {
                        // Set the slotIndex to 0 for the new page
                        slotIndex = 0;
                        break;
                    } else {
                        // If pinning the page failed, close the slot walk and return the error code
                        closeSlotWalk(table, &handle);
                        pthread_mutex_unlock(&insert_record_mutex);
                        return result;
                    }
                } else {
                    // If no free page available, close the slot walk and return RC_WRITE_FAILED
                    closeSlotWalk(table, &handle);
                    pthread_mutex_unlock(&insert_record_mutex);
                    return RC_WRITE_FAILED;
                }
                break;
            default:
                // Default case: An error occurred, close the slot walk and return RC_WRITE_FAILED
                closeSlotWalk(table, &handle);
                pthread_mutex_unlock(&insert_record_mutex);
                return RC_WRITE_FAILED;
        }

        // Check if the current slot is free
        if (slots[slotIndex] == FALSE) {
            RC result = markDirty(&bufferPool, handle);
            if (result != RC_OK) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&insert_record_mutex);
                return result;
            }
            // Get the record size from the schema
            int recordSize = getRecordSize(rel->schema);
            // Get the pointer to the tuple data for the current slot
            char *tupleData = getTupleDataAt(handle, recordSize, slotIndex);
            // Copy the record data to the tuple data
            memcpy(tupleData, record->data, recordSize);
            // Mark the slot as occupied
            slots[slotIndex] = true;
            // Set the record ID with the current page and slot
            record->id.page = handle->pageNum;
            // Increment the number of tuples in the table
            table->numTuples++;
            record->id.slot = slotIndex;
            // Mark the system catalog as dirty
            markSystemCatalogDirty();
            // Close the slot walk and return RC_OK
            closeSlotWalk(table, &handle);
            // Unlock the mutex before returning
            pthread_mutex_unlock(&insert_record_mutex);
            return RC_OK;
        }
    }

    // If no free slot was found, close the slot walk and return RC_WRITE_FAILED
    closeSlotWalk(table, &handle);
    // Unlock the mutex before returning
    pthread_mutex_unlock(&insert_record_mutex);
    return RC_WRITE_FAILED;
}

bool equalValues(Value *val1, Value *val2) {
    if (val1->dt != val2->dt) {
        return false;
    }

    switch (val1->dt) {
        case DT_INT:
            return val1->v.intV == val2->v.intV;
        case DT_STRING:
            return strcmp(val1->v.stringV, val2->v.stringV) == 0;
        case DT_FLOAT:
            return val1->v.floatV == val2->v.floatV;
        case DT_BOOL:
            return val1->v.boolV == val2->v.boolV;
        default:
            return false;
    }
}

RC checkPrimaryKeyConstraint(RM_TableData *rel, Record *record) {
    // Create a scan to check for existing records with the same key attribute values
    RM_ScanHandle scanHandle;
    RC result = startScan(rel, &scanHandle, NULL);
    if (result != RC_OK) {
        return result;
    }

    Record existingRecord;
    while ((result = next(&scanHandle, &existingRecord)) == RC_OK) {
        bool hasSameKey = true;
        for (int i = 0; i < rel->schema->keySize; i++) {
            int keyAttrIndex = rel->schema->keyAttrs[i];
            Value *newValue, *existingValue;
            getAttr(record, rel->schema, keyAttrIndex, &newValue);
            getAttr(&existingRecord, rel->schema, keyAttrIndex, &existingValue);
            if (!equalValues(newValue, existingValue)) {
                hasSameKey = false;
                freeVal(newValue);
                freeVal(existingValue);
                break;
            }
            freeVal(newValue);
            freeVal(existingValue);
        }
        if (hasSameKey) {
            closeScan(&scanHandle);
            return RC_IM_KEY_ALREADY_EXISTS;
        }
    }

    closeScan(&scanHandle);
    return RC_OK;
}
pthread_mutex_t delete_record_mutex = PTHREAD_MUTEX_INITIALIZER;

RC deleteRecord(RM_TableData *rel, RID id) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&delete_record_mutex);

    // Begin using the table page header for the given RID
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Check if the slot index is valid (within the range of slots in the page header)
    if (id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&delete_record_mutex);
        return RC_WRITE_FAILED;
    }

    // Get the slot array
    bool *slots = getSlots(&handle);

    // Check if the slot at the given index is occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&delete_record_mutex);
        return RC_WRITE_FAILED;
    }

    // Mark the slot as unoccupied (FALSE)
    slots[id.slot] = FALSE;

    // Mark the system catalog as dirty
    result = markSystemCatalogDirty();

    // Check if marking the system catalog as dirty failed
    if (result != RC_OK) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&delete_record_mutex);
        return RC_WRITE_FAILED;
    }

    // Mark the page as dirty in the buffer pool
    result = markDirty(&bufferPool, &handle);

    // Check if marking the page as dirty failed
    if (result != RC_OK) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&delete_record_mutex);
        return RC_WRITE_FAILED;
    }

    // End using the table page header
    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&delete_record_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

pthread_mutex_t update_record_mutex = PTHREAD_MUTEX_INITIALIZER;

RC updateRecord(RM_TableData *rel, Record *record) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&update_record_mutex);

    // Get the RID (record identifier) from the record
    RID id = record->id;

    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Get the slot array
    bool *slots = getSlots(&handle);

    // Check if the slot index is valid (within the range of slots in the page header)
    if (id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&update_record_mutex);
        return RC_WRITE_FAILED;
    }

    // Check if the slot at the given index is occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&update_record_mutex);
        return RC_WRITE_FAILED;
    }

    // Get the record size based on the schema
    int recordSize = getRecordSize(rel->schema);

    // Get the pointer to the tuple data for the given slot
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);

    // Get the schema and the number of fields
    Schema *schema = rel->schema;
    int numFields = schema->numAttr;

    // Initialize the offset to 0
    int offset = 0;

    // Loop through each field in the record
    for (int i = 0; i < numFields; i++) {
        // Get the data type of the current field
        DataType dataType = schema->dataTypes[i];

        // Initialize the field size
        int fieldSize = 0;

        // Switch based on the data type to determine the field size and update the tuple data
        switch (dataType) {
            case DT_INT:
                fieldSize = sizeof(int);
                *(int *)(tupleData + offset) = *(int *)(record->data + offset);
                break;
            case DT_STRING:
                fieldSize = schema->typeLength[i];
                strncpy(tupleData + offset, record->data + offset, fieldSize);
                break;
        }

        // Update the offset for the next field
        offset += fieldSize;
    }

    // Mark the page as dirty in the buffer pool
    result = markDirty(&bufferPool, &handle);

    // Check if marking the page as dirty failed
    if (result != RC_OK) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        pthread_mutex_unlock(&update_record_mutex);
        return RC_WRITE_FAILED;
    }

    // End using the table page header
    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&update_record_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}


// Define a mutex for startScan function
static pthread_mutex_t start_scan_mutex = PTHREAD_MUTEX_INITIALIZER;

// record_mgr.c
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&start_scan_mutex);

    RC result = RC_OK;
    TableMetadata *table = getTableMetadata(rel);

    // Check if the table metadata is found
    if (table == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&start_scan_mutex);
        return RC_TABLE_METADATA_NOT_FOUND;
    }

    BM_PageHandle *handle = table->handle;

    // Check if the page handle is found
    if (handle == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&start_scan_mutex);
        return RC_PAGE_HANDLE_NOT_FOUND;
    }

    scan->rel = rel;

    // Allocate memory for the scan management data
    scan->mgmtData = malloc(sizeof(ScanData));

    // Check if memory allocation failed
    if (scan->mgmtData == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&start_scan_mutex);
        return RC_OUT_OF_MEMORY;
    }
    ScanData *scanData = (ScanData *)scan->mgmtData;
    scanData->id.page = handle->pageNum;
    scanData->id.slot = -1;

    PageHeader *header = getPageHeader(handle);
    scanData->condition = cond;

    // Check if the page header is found
    if (header == NULL) {
        free(scan->mgmtData);
        scan->mgmtData = NULL;
        // Unlock the mutex before returning
        pthread_mutex_unlock(&start_scan_mutex);
        return RC_PAGE_HEADER_NOT_FOUND;
    }
    
    if (header->numSlots == 0) {
        // Initialize the slots to 0
        int recordSize = getRecordSize(rel->schema);
        int slotSize = sizeof(bool);
        int pageSize = PAGE_SIZE;
        int numSlots = (pageSize - sizeof(PageHeader)) / (recordSize + slotSize);

        // Allocate memory for the slots
        bool *slots = (bool *)malloc(numSlots * sizeof(bool));
        if (slots == NULL) {
            free(scan->mgmtData);
            scan->mgmtData = NULL;
            // Unlock the mutex before returning
            pthread_mutex_unlock(&start_scan_mutex);
            return RC_OUT_OF_MEMORY;
        }

        // Initialize the slots to 0
        memset(slots, 0, numSlots * sizeof(bool));

        // Update the page header
        header->numSlots = numSlots;
        header->nextPageNumber = NO_PAGE;
        header->prevPageNumber = NO_PAGE;

        // Mark the page as dirty
        RC markDirtyResult = markDirty(&bufferPool, handle);
        if (markDirtyResult != RC_OK) {
            free(slots);
            free(scan->mgmtData);
            scan->mgmtData = NULL;
            // Unlock the mutex before returning
            pthread_mutex_unlock(&start_scan_mutex);
            return markDirtyResult;
        }

        // Set up the scan handle for the initialized table
        scan->rel = rel;
        scan->mgmtData = malloc(sizeof(ScanData));
        if (scan->mgmtData == NULL) {
            free(slots);
            // Unlock the mutex before returning
            pthread_mutex_unlock(&start_scan_mutex);
            return RC_OUT_OF_MEMORY;
        }
        ScanData *scanData = (ScanData *)scan->mgmtData;
        scanData->id.page = handle->pageNum;
        scanData->id.slot = -1;
        scanData->condition = cond;

        free(slots);
    }
    
    // Unlock the mutex before returning
    pthread_mutex_unlock(&start_scan_mutex);
    return RC_OK;
}
// Define a mutex for getRecord function
static pthread_mutex_t get_record_mutex = PTHREAD_MUTEX_INITIALIZER;

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&get_record_mutex);

    // Check if the relation or record pointer is NULL
    if (rel == NULL || record == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_INVALID_SCHEMA;
    }

    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Check if the slot index is valid
    if (id.slot < 0 || id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_INVALID_RECORD;
    }

    // Get the slot array
    bool *slots = getSlots(&handle);

    // Check if the slot array is NULL
    if (slots == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Check if the slot is not occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Get the record size based on the schema
    int recordSize = getRecordSize(rel->schema);

    // Check if the record size is invalid
    if (recordSize <= 0) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_INVALID_SCHEMA;
    }

    // Get the pointer to the tuple data
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);

    // Check if the tuple data pointer is NULL
    if (tupleData == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_RM_NO_MORE_TUPLES;
    }

    // Allocate memory for the record data
    record->data = (char *)malloc(recordSize);

    // Check if memory allocation failed
    if (record->data == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        // Unlock the mutex before returning
        pthread_mutex_unlock(&get_record_mutex);
        return RC_MEMORY_ALLOCATION_FAILED;
    }
    record->id.slot = id.slot;

    // Copy the tuple data to the record data
    memcpy(record->data, tupleData, recordSize);

    // Set the record ID
    record->id.page = id.page;
    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // Unlock the mutex before returning
    pthread_mutex_unlock(&get_record_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}


// Define a mutex for the next function
static pthread_mutex_t next_mutex = PTHREAD_MUTEX_INITIALIZER;

RC next(RM_ScanHandle *scan, Record *record) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&next_mutex);

    ScanData *scanData = (ScanData *)scan->mgmtData;
    RM_TableData *rel = scan->rel;
    RC result = RC_OK;
    TableMetadata *table = getTableMetadata(rel);
    BM_PageHandle *handle = table->handle;
    Value *value = NULL;
    bool *slots = getSlots(handle);
    PageHeader *header = getPageHeader(handle);

    // Loop through the slots until a record satisfying the condition is found
    while (scanData->id.slot < header->numSlots - 1) {
        scanData->id.slot++;

        // Check if the slot is occupied
        if (slots[scanData->id.slot] == TRUE) {
            result = getRecord(rel, scanData->id, record);

            // Check if getting the record failed
            if (result != RC_OK) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&next_mutex);
                return result;
            }

            // Check if a condition is provided
            if (scanData->condition != NULL) {
                result = evalExpr(record, scan->rel->schema, scanData->condition, &value);

                // Check if evaluating the expression failed
                if (result != RC_OK) {
                    freeVal(value);
                    // Unlock the mutex before returning
                    pthread_mutex_unlock(&next_mutex);
                    return result;
                }

                // Check if the condition is satisfied
                if (value->v.boolV == TRUE) {
                    freeVal(value);
                    // Unlock the mutex before returning
                    pthread_mutex_unlock(&next_mutex);
                    return RC_OK;
                }

                freeVal(value);
            } else {
                // If no condition is provided, return the record
                // Unlock the mutex before returning
                pthread_mutex_unlock(&next_mutex);
                return RC_OK;
            }
        }
    }

    // No more tuples satisfying the condition
    // Unlock the mutex before returning
    pthread_mutex_unlock(&next_mutex);
    return RC_RM_NO_MORE_TUPLES;
}
RC closeScan(RM_ScanHandle *scan) {
    // Check if the scan handle pointer is NULL
    if (scan == NULL) {
        // If the scan handle pointer is NULL, return RC_INVALID_SCAN_HANDLE
        return RC_INVALID_SCAN_HANDLE;
    }

    // Check if the mgmtData pointer is NULL
    if (scan->mgmtData == NULL) {
        // If the mgmtData pointer is NULL, there's nothing to free
        // Return RC_OK since the scan handle is already closed
        return RC_OK;
    }

    // Free the memory allocated for the mgmtData
    free(scan->mgmtData);

    // Set the mgmtData pointer to NULL after freeing the memory
    scan->mgmtData = NULL;

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

// Define a mutex for the getAttributeSize function
static pthread_mutex_t getAttributeSize_mutex = PTHREAD_MUTEX_INITIALIZER;

int getAttributeSize(Schema *schema, int attributeIndex) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&getAttributeSize_mutex);

    int size = -1; // Default size indicating error

    // Check if the schema pointer is NULL
    if (schema != NULL) {
        // Check if the attributeIndex is within the range of attributes in the schema
        if (attributeIndex >= 0 && attributeIndex < schema->numAttr) {
            // Get the data type of the attribute from the schema
            DataType dataType = schema->dataTypes[attributeIndex];

            // Determine the size based on the data type
            switch (dataType) {
                case DT_INT:
                    // Size of int is typically 4 bytes
                    size = 4;
                    break;
                case DT_STRING:
                    // Size of string is type length + 1 (to account for the null terminator)
                    size = schema->typeLength[attributeIndex] + 1;
                    break;
                case DT_FLOAT:
                    // Size of float is typically 4 bytes
                    size = 4;
                    break;
                case DT_BOOL:
                    // Size of bool is typically 1 byte
                    size = 1;
                    break;
                default:
                    // Unknown data type
                    size = -1;
            }
        }
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&getAttributeSize_mutex);

    return size;
}

int getRecordSize(Schema *schema) {
    // Check if the schema pointer is NULL
    if (schema == NULL) {
        // If the schema pointer is NULL, return -1 (Invalid schema pointer)
        return -1;
    }

    // Initialize the size variable to 0
    int size = 0;

    // Iterate over all attributes in the schema
    for (int attributeIndex = 0; attributeIndex < schema->numAttr; attributeIndex++) {
        // Get the size of the current attribute
        int attributeSize = getAttributeSize(schema, attributeIndex);

        // Check if an error occurred while getting the attribute size
        if (attributeSize == -1) {
            // If an error occurred, return -1 (Error occurred while getting attribute size)
            return -1;
        }

        // Add the attribute size to the total size
        size += attributeSize;
    }

    // Return the total record size
    return size;
}


// Define a mutex for the createSchema function
static pthread_mutex_t createSchema_mutex = PTHREAD_MUTEX_INITIALIZER;

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&createSchema_mutex);

    // Allocate memory for the Schema struct
    Schema *schema = (Schema *)malloc(sizeof(Schema));

    // Check if memory allocation for the Schema struct failed
    if (schema == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&createSchema_mutex);
        // If memory allocation failed, return NULL
        return NULL;
    }

    // Allocate memory for attribute names, data types, and type lengths
    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));

    // Check if memory allocation for attribute names, data types, or type lengths failed
    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL) {
        // If memory allocation failed, free the Schema struct, unlock the mutex, and return NULL
        freeSchema(schema);
        pthread_mutex_unlock(&createSchema_mutex);
        return NULL;
    }

    // Copy attribute names, data types, and type lengths
    for (int i = 0; i < numAttr; i++) {
        // Duplicate the attribute name string and store it
        schema->attrNames[i] = strdup(attrNames[i]);
        // Copy the data type and type length
        schema->dataTypes[i] = dataTypes[i];
        schema->typeLength[i] = typeLength[i];
    }

    // Allocate memory for key attributes
    schema->keyAttrs = (int *)malloc(keySize * sizeof(int));

    // Check if memory allocation for key attributes failed
    if (schema->keyAttrs == NULL) {
        // If memory allocation failed, free the Schema struct, unlock the mutex, and return NULL
        freeSchema(schema);
        pthread_mutex_unlock(&createSchema_mutex);
        return NULL;
    }

    // Copy key attributes
    for (int i = 0; i < keySize; i++) {
        schema->keyAttrs[i] = keys[i];
    }

    // Set the numAttr and keySize fields of the Schema
    schema->numAttr = numAttr;
    schema->keySize = keySize;

    // Unlock the mutex before returning
    pthread_mutex_unlock(&createSchema_mutex);

    // If everything succeeded, return the created Schema
    return schema;
}

// Define a mutex for the freeSchema function
static pthread_mutex_t freeSchema_mutex = PTHREAD_MUTEX_INITIALIZER;

RC freeSchema(Schema *schema) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&freeSchema_mutex);

    if (schema == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&freeSchema_mutex);
        // Invalid schema pointer
        return RC_INVALID_SCHEMA;
    }

    // Free attribute names
    if (schema->attrNames != NULL) {
        for (int i = 0; i < schema->numAttr; i++) {
            free(schema->attrNames[i]);
        }
        free(schema->attrNames);
    }

    // Free data types, type lengths, and key attributes
    free(schema->dataTypes);
    free(schema->typeLength);
    free(schema->keyAttrs);

    // Free the schema struct itself
    free(schema);

    // Unlock the mutex before returning
    pthread_mutex_unlock(&freeSchema_mutex);

    return RC_OK;
}


// Define a mutex for the createRecord function
static pthread_mutex_t createRecord_mutex = PTHREAD_MUTEX_INITIALIZER;

RC createRecord(Record **record, Schema *schema) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&createRecord_mutex);

    // Get the size of the record based on the schema
    int recordSize = getRecordSize(schema);

    // Allocate memory for the Record struct
    *record = (Record *)malloc(sizeof(Record));

    // Check if memory allocation for the Record struct failed
    if (*record == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&createRecord_mutex);
        // If memory allocation failed, return RC_MEMORY_ALLOCATION_FAILED
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Get a pointer to the newly allocated Record struct
    Record *recordPtr = *record;

    // Allocate memory for the record data using calloc (initialized with 0)
    recordPtr->data = (char *)calloc(recordSize, sizeof(char));

    // Check if memory allocation for the record data failed
    if (recordPtr->data == NULL) {
        // Free the previously allocated Record struct
        free(recordPtr);
        // Unlock the mutex before returning
        pthread_mutex_unlock(&createRecord_mutex);
        // Return RC_MEMORY_ALLOCATION_FAILED
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Initialize the record fields
    // Set the page and slot fields of the record ID to -1 (indicating no page or slot assigned)
    recordPtr->id.page = -1;
    recordPtr->id.slot = -1;

    // Unlock the mutex before returning
    pthread_mutex_unlock(&createRecord_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

// Define a mutex for the freeRecord function
static pthread_mutex_t freeRecord_mutex = PTHREAD_MUTEX_INITIALIZER;

RC freeRecord(Record *record) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&freeRecord_mutex);

    // Check if the record pointer is NULL
    if (record == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&freeRecord_mutex);
        // If the record pointer is NULL, return RC_INVALID_RECORD
        return RC_INVALID_RECORD;
    }

    // Check if the record data pointer is not NULL
    if (record->data != NULL) {
        // If the record data pointer is not NULL, free the memory allocated for the record data
        free(record->data);
        // Set the record data pointer to NULL after freeing the memory
        record->data = NULL;
    }

    // Free the memory allocated for the Record struct itself
    free(record);

    // Unlock the mutex before returning
    pthread_mutex_unlock(&freeRecord_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

// Define a mutex for the getAttr function
static pthread_mutex_t getAttr_mutex = PTHREAD_MUTEX_INITIALIZER;

RC getAttr(Record *record, Schema *schema, int attributeNum, Value **value) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&getAttr_mutex);

    // Check if the record pointer or schema pointer is NULL
    if (record == NULL || schema == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&getAttr_mutex);
        // If the record or schema pointer is NULL, return RC_INVALID_SCHEMA
        return RC_INVALID_SCHEMA;
    }

    // Check if the attributeNum is valid (within the range of schema attributes)
    if (attributeNum < 0 || attributeNum >= schema->numAttr) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&getAttr_mutex);
        // If not, return RC_WRITE_FAILED
        return RC_WRITE_FAILED;
    }

    // Get a pointer to the start of the record data
    char *dataPtr = record->data;

    // Initialize the offset to 0
    int offset = 0;

    // If the attributeNum is not 0 (i.e., not the first attribute)
    if (attributeNum > 0) {
        // Calculate the offset for the desired attribute
        offset = calculateOffset(schema, attributeNum);
        // Move the data pointer to the start of the desired attribute
        dataPtr += offset;
    }

    // Get the size of the desired attribute
    int attributeSize = getAttributeSize(schema, attributeNum);

    // Check if the attribute size is valid
    if (attributeSize <= 0) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&getAttr_mutex);
        // If invalid, return RC_INVALID_SCHEMA
        return RC_INVALID_SCHEMA;
    }

    // Allocate memory for a new Value struct
    *value = (Value *)malloc(sizeof(Value));

    // Check if memory allocation for the Value struct failed
    if (*value == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&getAttr_mutex);
        // If memory allocation failed, return RC_MEMORY_ALLOCATION_FAILED
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Get a pointer to the newly allocated Value struct
    Value *valuePtr = *value;

    // Set the data type of the Value struct based on the schema
    valuePtr->dt = schema->dataTypes[attributeNum];

    // Based on the data type, retrieve the attribute value and set it in the Value struct
    switch (valuePtr->dt) {
        case DT_INT: {
            // If the attribute is an integer, dereference dataPtr as an int pointer and set valuePtr->v.intV
            valuePtr->v.intV = *((int *)dataPtr);
            break;
        }
        case DT_STRING: {
            // If the attribute is a string, allocate memory for the string and copy the string value
            valuePtr->v.stringV = (char *)malloc(attributeSize);
            if (valuePtr->v.stringV == NULL) {
                // Memory allocation failed, free the allocated Value struct
                free(valuePtr);
                // Unlock the mutex before returning
                pthread_mutex_unlock(&getAttr_mutex);
                // Return RC_MEMORY_ALLOCATION_FAILED
                return RC_MEMORY_ALLOCATION_FAILED;
            }
            strncpy(valuePtr->v.stringV, dataPtr, attributeSize);
            break;
        }
        case DT_FLOAT: {
            // If the attribute is a float, dereference dataPtr as a float pointer and set valuePtr->v.floatV
            valuePtr->v.floatV = *((float *)dataPtr);
            break;
        }
        case DT_BOOL: {
            // If the attribute is a boolean, dereference dataPtr as a bool pointer and set valuePtr->v.boolV
            valuePtr->v.boolV = *((bool *)dataPtr);
            break;
        }
        default:
            // If the data type is unknown, free the allocated Value struct
            free(valuePtr);
            // Unlock the mutex before returning
            pthread_mutex_unlock(&getAttr_mutex);
            // Return RC_IM_ATTR_TYPE_UNKNOWN
            return RC_IM_ATTR_TYPE_UNKNOWN;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&getAttr_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

// Function to calculate the offset for a given attribute in a schema
int calculateOffset(Schema *schema, int attributeNum) {
   // Initialize the offset to 0
   int offset = 0;
   // Initialize the loop counter to 0
   int i = 0;

   // Loop until the desired attribute is reached
   while (i < attributeNum) {
       // Add the size of the current attribute to the offset
       offset += getAttributeSize(schema, i);
       // Increment the loop counter
       i++;
   }
   // Return the calculated offset for the desired attribute
   return offset;
}


// Define a mutex for the setAttr function
static pthread_mutex_t setAttr_mutex = PTHREAD_MUTEX_INITIALIZER;

RC setAttr(Record *record, Schema *schema, int attributeNum, Value *value) {
    // Lock the mutex to ensure exclusive access
    pthread_mutex_lock(&setAttr_mutex);

    // Check if the record pointer or schema pointer is NULL
    if (record == NULL || schema == NULL) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&setAttr_mutex);
        // If the record or schema pointer is NULL, return RC_INVALID_SCHEMA
        return RC_INVALID_SCHEMA;
    }

    // Check if the attributeNum is valid (within the range of schema attributes)
    if (attributeNum < 0 || attributeNum >= schema->numAttr) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&setAttr_mutex);
        // If not, return RC_WRITE_FAILED
        return RC_WRITE_FAILED;
    }

    // Get a pointer to the start of the record data
    char *dataPtr = record->data;

    // Initialize the offset to 0
    int offset = 0;

    // If the attributeNum is not 0 (i.e., not the first attribute)
    if (attributeNum > 0) {
        // Calculate the offset for the desired attribute
        offset = calculateOffset(schema, attributeNum);
        // Move the data pointer to the start of the desired attribute
        dataPtr += offset;
    }

    // Get the size of the desired attribute
    int attributeSize = getAttributeSize(schema, attributeNum);

    // Check if the attribute size is valid
    if (attributeSize <= 0) {
        // Unlock the mutex before returning
        pthread_mutex_unlock(&setAttr_mutex);
        // If invalid, return RC_INVALID_SCHEMA
        return RC_INVALID_SCHEMA;
    }

    // Check the data type of the value
    switch (value->dt) {
        case DT_INT: {
            // If the value is an integer, set the attribute to the integer value
            if (attributeSize != sizeof(int)) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&setAttr_mutex);
                // If the attribute size is invalid, return RC_INVALID_SCHEMA
                return RC_INVALID_SCHEMA;
            }
            *((int *)dataPtr) = value->v.intV;
            break;
        }
        case DT_STRING: {
            // If the value is a string, copy the string value to the attribute
            if (attributeSize < strlen(value->v.stringV) + 1) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&setAttr_mutex);
                // If the attribute size is insufficient, return RC_INVALID_SCHEMA
                return RC_INVALID_SCHEMA;
            }
            strncpy(dataPtr, value->v.stringV, attributeSize);
            break;
        }
        case DT_FLOAT: {
            // If the value is a float, set the attribute to the float value
            if (attributeSize != sizeof(float)) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&setAttr_mutex);
                // If the attribute size is invalid, return RC_INVALID_SCHEMA
                return RC_INVALID_SCHEMA;
            }
            *((float *)dataPtr) = value->v.floatV;
            break;
        }
        case DT_BOOL: {
            // If the value is a boolean, set the attribute to the boolean value
            if (attributeSize != sizeof(bool)) {
                // Unlock the mutex before returning
                pthread_mutex_unlock(&setAttr_mutex);
                // If the attribute size is invalid, return RC_INVALID_SCHEMA
                return RC_INVALID_SCHEMA;
            }
            *((bool *)dataPtr) = value->v.boolV;
            break;
        }
        default:
            // If the data type is unknown, return RC_IM_ATTR_TYPE_UNKNOWN
            // Unlock the mutex before returning
            pthread_mutex_unlock(&setAttr_mutex);
            return RC_IM_ATTR_TYPE_UNKNOWN;
    }

    // Unlock the mutex before returning
    pthread_mutex_unlock(&setAttr_mutex);

    // If the function executed successfully, return RC_OK
    return RC_OK;
}
