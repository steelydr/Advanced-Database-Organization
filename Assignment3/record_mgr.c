#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>



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
} SystemCatalog;

typedef struct ScanData {
    RID id;
    Expr *condition;
} ScanData;


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

    // Get the system catalog
    SystemCatalog* catalog = getSystemCatalog();
    if (catalog == NULL) {
        return NULL;
    }

    // Initialize pointers
    TableMetadata* table = catalog->tables;
    TableMetadata* endPtr = table + catalog->numTables;

    // Search for the table in the system catalog
    while (table < endPtr) {
        if (strcmp(table->name, name) == 0) {
            return table;
        }
        table++;
    }

    // Table not found
    return NULL;
}

char* getTupleData(BM_PageHandle* handle) {
    // Get the page header
    PageHeader* header = getPageHeader(handle);

    // Return the tuple data area
    return (char*)(getSlots(handle) + sizeof(bool) * header->numSlots);
}

bool* getSlots(BM_PageHandle* handle) {
    // Return the slot array from the page handle
    return (bool*)(handle->data + PAGE_HEADER_SIZE);
}

TableMetadata* getTableMetadata(RM_TableData* rel) {
    // Return the table metadata from the RM_TableData structure
    return (TableMetadata*)rel->mgmtData;
}

int setFreePage(BM_PageHandle* handle)
{
    // TODO: implement this for overflow pages
    return 1;
}

char* getTupleDataAt(BM_PageHandle* handle, int recordSize, int index) {
    // Get the tuple data area
    char* tupleData = getTupleData(handle);

    // Return the pointer to the tuple data at the given index
    return tupleData + index * recordSize;
}


int getFreePageNumber() {
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

int getNextPage(TableMetadata *table, int pageNumber) {
    // If the page number is the table's page number, return the next page number from the table's page handle
    if (pageNumber == table->pageNumber) {
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

    return nextPageNumber;
}
int appendToFreeList(int pageNumber) {
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

    return 0;
}

int getNextSlotInWalk(TableMetadata *table, BM_PageHandle **handle, bool **slots, int *slotIndex) {

    PageHeader *header = getPageHeader(*handle);
    
    RC result;
    *slots = getSlots(*handle);

    int remainingSlots = header->numSlots - *slotIndex;

    switch (remainingSlots) {
        case 0:
            *slotIndex = 0;
            int nextPageNumber = header->nextPageNumber;

            result = (table->pageNumber != (*handle)->pageNum && nextPageNumber != NO_PAGE)
                     ? unpinPage(&bufferPool, *handle)
                     : RC_OK;

            if (result == RC_OK && nextPageNumber != NO_PAGE) {
                result = pinPage(&bufferPool, *handle, nextPageNumber);
                if (result == RC_OK) {
                    return getNextSlotInWalk(table, handle, slots, slotIndex);
                }
            }

            return (nextPageNumber == NO_PAGE) ? -1 : 1;

        default:
            (*slotIndex)++;
            return 0;
    }
}

RC initRecordManager(void *mgmtData) {
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
        return result;
    }

    // Create system schema if it's a new file
    catalog = getSystemCatalog();
    catalog->totalNumPages = newSystem ? 1 : catalog->totalNumPages;
    catalog->freePageNumber = newSystem ? NO_PAGE : catalog->freePageNumber;
    catalog->numTables = newSystem ? 0 : catalog->numTables;
    markSystemCatalogDirty();

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

RC createTable(char *name, Schema *schema) {
    if (name == NULL || strlen(name) == 0 || schema == NULL) {
        fprintf(stderr, "Error: Invalid arguments provided to createTable\n");
        return RC_INVALID_ARGUMENT;
    }

    SystemCatalog *catalog = getSystemCatalog();
    if (catalog == NULL) {
        fprintf(stderr, "Error: Failed to get system catalog\n");
        return RC_SYSTEM_CATALOG_ERROR;
    }

    // Check if table already exists
    TableMetadata *existingTable = getTableByName((char *)name);
    if (existingTable != NULL) {
        fprintf(stderr, "Error: Table '%s' already exists\n", name);
        return RC_TABLE_ALREADY_EXISTS;
    }

    // Check if catalog can hold another table and schema is valid
    if (catalog->numTables >= MAX_NUM_TABLES || schema->numAttr > MAX_NUM_ATTR || schema->keySize > MAX_NUM_KEYS) {
        fprintf(stderr, "Error: Maximum number of tables or attributes reached\n");
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
        return RC_PAGE_ALLOCATION_FAILED;
    }

    // Initialize the page
    int recordSize = getRecordSize(schema);
    int recordsPerPage = (PAGE_SIZE - pageHeaderSize) / (recordSize + slotSize);
    if (!(recordsPerPage > 0)) {
    fprintf(stderr, "Error: Record size too large for table '%s'\n", name);
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
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name) {
    if (rel == NULL || name == NULL || strlen(name) == 0) {
        fprintf(stderr, "Error: Invalid arguments provided to openTable\n");
        return RC_INVALID_ARGUMENT;
    }

    TableMetadata *table = getTableByName((char *)name);
    if (table == NULL) {
        fprintf(stderr, "Error: Table '%s' not found\n", name);
        return RC_TABLE_NOT_FOUND;
    }

    if (table->handle != NULL) {
        fprintf(stderr, "Error: Table '%s' is already open\n", name);
        return RC_TABLE_ALREADY_OPEN;
    }

    // Initialize the RM_TableData structure
    rel->name = strdup(table->name);
    if (rel->name == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for table name\n");
        return RC_MALLOC_FAILED;
    }

    rel->schema = (Schema *)malloc(sizeof(Schema));
    if (rel->schema == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for schema\n");
        free(rel->name);
        return RC_MALLOC_FAILED;
    }

    rel->schema->numAttr = table->numAttributes;
    rel->schema->attrNames = (char **)malloc(sizeof(char *) * table->numAttributes);
    if (rel->schema->attrNames == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for attribute names\n");
        free(rel->name);
        free(rel->schema);
        return RC_MALLOC_FAILED;
    }

    rel->schema->dataTypes = (DataType *)malloc(sizeof(DataType) * table->numAttributes);
    if (rel->schema->dataTypes == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for data types\n");
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema);
        return RC_MALLOC_FAILED;
    }

    rel->schema->typeLength = (int *)malloc(sizeof(int) * table->numAttributes);
    if (rel->schema->typeLength == NULL) {
        fprintf(stderr, "Error: Memory allocation failed for type lengths\n");
        free(rel->name);
        free(rel->schema->attrNames);
        free(rel->schema->dataTypes);
        free(rel->schema);
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
        return result;
    }

    return RC_OK;
}

RC closeTable(RM_TableData *rel) {
    if (rel == NULL) {
        return RC_TABLE_HANDLE_NOT_FOUND;
    }

    TableMetadata *table = getTableMetadata(rel);
    if (table == NULL) {
        return RC_TABLE_METADATA_NOT_FOUND;
    }

    // Unpin and force the page to disk
    RC result = unpinPage(&bufferPool, table->handle);
    if (result != RC_OK) {
        // Log the error and handle it accordingly
        fprintf(stderr, "Error unpinning page: %d\n", result);
        // Add additional error handling logic here if needed
        return result;
    }

    result = forcePage(&bufferPool, table->handle);
    if (result != RC_OK) {
        // Log the error and handle it accordingly
        fprintf(stderr, "Error forcing page: %d\n", result);
        // Add additional error handling logic here if needed
        return result;
    }

    // Free memory
    free(rel->schema->attrNames);
    free(rel->schema);
    free(table->handle);
    table->handle = NULL;

    return RC_OK;
}
RC deleteTable(char *name) {
    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog was retrieved successfully
    if (catalog == NULL) {
        // If the system catalog cannot be retrieved, print an error message
        fprintf(stderr, "Error: Failed to get system catalog\n");
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
        // If the table was not found, return RC_IM_KEY_NOT_FOUND
        return RC_IM_KEY_NOT_FOUND;
    }

    // Put the table's page chain in the free list
    if (appendToFreeList(table->pageNumber) != RC_OK) {
        // If appending to the free list fails, print an error message
        fprintf(stderr, "Error: Failed to append table pages to free list\n");
        return RC_WRITE_FAILED;
    }

    // Shift entries in the table catalog down
    memmove(&(catalog->tables[tableIndex]), &(catalog->tables[tableIndex + 1]),
            (catalog->numTables - tableIndex - 1) * sizeof(TableMetadata));

    // Decrement the number of tables in the catalog
    catalog->numTables--;

    // Mark the system catalog as dirty
    markSystemCatalogDirty();

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

int getNumTuples(RM_TableData *rel) {
    // Check if the relation pointer is NULL
    if (rel == NULL) {
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Invalid table data\n");
        return -1;
    }

    // Get the TableMetadata from the relation
    TableMetadata *table = getTableMetadata(rel);

    // Check if the TableMetadata pointer is NULL
    if (table == NULL) {
        // If NULL, print an error message and return -2
        fprintf(stderr, "Error: Failed to get table metadata\n");
        return -2;
    }

    // Get the number of tuples from the TableMetadata
    int numTuples = table->numTuples;

    // Check if the number of tuples is less than 0
    if (numTuples < 0) {
        // If negative, print an error message and return -3
        fprintf(stderr, "Error: Invalid number of tuples in table metadata\n");
        return -3;
    }

    // If everything is valid, return the number of tuples
    return numTuples;
}

int getNumPages() {
    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog pointer is NULL
    if (catalog == NULL) {
        // If NULL, print an error message and return -1
        fprintf(stderr, "Error: Failed to get system catalog\n");
        return -1;
    }

    // Get the total number of pages from the system catalog
    int numPages = catalog->totalNumPages;

    // Check if the number of pages is less than 0
    if (numPages < 0) {
        // If negative, print an error message and return -2
        fprintf(stderr, "Error: Invalid number of pages in system catalog\n");
        return -2;
    }

    // If everything is valid, return the number of pages
    return numPages;
}

int getNumFreePages() {
    // Get the page handle header for page 0
    USE_PAGE_HANDLE_HEADER(0);

    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

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

    // Return the count of free pages
    return count;
}

int getNumTables() {
    // Get the system catalog
    SystemCatalog *catalog = getSystemCatalog();

    // Check if the system catalog was retrieved successfully
    if (catalog == NULL) {
        // If the system catalog cannot be retrieved, print an error message
        fprintf(stderr, "Error: Failed to retrieve the system catalog.\n");
        // Return -1 to indicate an error
        return -1;
    }

    // Get the number of tables from the system catalog
    int numTables = catalog->numTables;

    // Return the number of tables
    return numTables;
}

RC insertRecord(RM_TableData *rel, Record *record) {
    // Get the TableMetadata for the relation
    TableMetadata *table = getTableMetadata(rel);

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
        return result;
    }
} else {
    // If no free page available, close the slot walk and return RC_WRITE_FAILED
    closeSlotWalk(table, &handle);
    return RC_WRITE_FAILED;
}

                break;
            default:
                // Default case: An error occurred, close the slot walk and return RC_WRITE_FAILED
                closeSlotWalk(table, &handle);
                return RC_WRITE_FAILED;
        }

        // Check if the current slot is free
        if (slots[slotIndex] == FALSE) {
            RC result = markDirty(&bufferPool, handle);
            if (result != RC_OK) return result;
            // Get the record size from the schema
            int recordSize = getRecordSize(rel->schema);
            // Get the pointer to the tuple data for the current slot
            char *tupleData = getTupleDataAt(handle, recordSize, slotIndex);
            // Mark the page as dirty in the buffer pool
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
            return RC_OK;
        }
    }

    // If no free slot was found, close the slot walk and return RC_WRITE_FAILED
    closeSlotWalk(table, &handle);
    return RC_WRITE_FAILED;
}

RC deleteRecord(RM_TableData *rel, RID id) {
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Check if the slot index is valid (within the range of slots in the page header)
    if (id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_WRITE_FAILED;
    }

    // Get the slot array
    bool *slots = getSlots(&handle);

    // Check if the slot at the given index is occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_WRITE_FAILED;
    }

    // Mark the slot as unoccupied (FALSE)
    slots[id.slot] = FALSE;

    // Mark the system catalog as dirty
    result = markSystemCatalogDirty();

    // Check if marking the system catalog as dirty failed
    if (result != RC_OK) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_WRITE_FAILED;
    }

    // Mark the page as dirty in the buffer pool
    result = markDirty(&bufferPool, &handle);

    // Check if marking the page as dirty failed
    if (result != RC_OK) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_WRITE_FAILED;
    }

    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    // Get the RID (record identifier) from the record
    RID id = record->id;

    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);


    // Get the slot array
    bool *slots = getSlots(&handle);
    
    // Check if the slot index is valid (within the range of slots in the page header)
    if (id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_WRITE_FAILED;
    }

    // Check if the slot at the given index is occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
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
        return RC_WRITE_FAILED;
    }

    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // If the function executed successfully, return RC_OK
    return RC_OK;
}


// Function to start a scan on a table
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    RC result = RC_OK;
    TableMetadata *table = getTableMetadata(rel);

    // Check if the table metadata is found
    if (table == NULL) {
        return RC_TABLE_METADATA_NOT_FOUND;
    }

    BM_PageHandle *handle = table->handle;

    // Check if the page handle is found
    if (handle == NULL) {
        return RC_PAGE_HANDLE_NOT_FOUND;
    }

    scan->rel = rel;

    // Allocate memory for the scan management data
    scan->mgmtData = malloc(sizeof(ScanData));

    // Check if memory allocation failed
    if (scan->mgmtData == NULL) {
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
        return RC_PAGE_HEADER_NOT_FOUND;
    }

    // Check if the table is empty
    if (header->numSlots == 0) {
        free(scan->mgmtData);
        scan->mgmtData = NULL;
        return RC_TABLE_IS_EMPTY;
    }

    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    // Check if the relation or record pointer is NULL
    if (rel == NULL || record == NULL) {
        return RC_INVALID_SCHEMA;
    }

    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Check if the slot index is valid
    if (id.slot < 0 || id.slot >= header->numSlots) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_INVALID_RECORD;
    }

    // Get the slot array
    bool *slots = getSlots(&handle);

    // Check if the slot array is NULL
    if (slots == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_RM_NO_MORE_TUPLES;
    }

    // Check if the slot is not occupied
    if (slots[id.slot] == FALSE) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_RM_NO_MORE_TUPLES;
    }

    // Get the record size based on the schema
    int recordSize = getRecordSize(rel->schema);

    // Check if the record size is invalid
    if (recordSize <= 0) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_INVALID_SCHEMA;
    }

    // Get the pointer to the tuple data
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);

    // Check if the tuple data pointer is NULL
    if (tupleData == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_RM_NO_MORE_TUPLES;
    }

    // Allocate memory for the record data
    record->data = (char *)malloc(recordSize);

    // Check if memory allocation failed
    if (record->data == NULL) {
        END_USE_TABLE_PAGE_HANDLE_HEADER();
        return RC_MEMORY_ALLOCATION_FAILED;
    }
    record->id.slot = id.slot;

    // Copy the tuple data to the record data
    memcpy(record->data, tupleData, recordSize);

    // Set the record ID
    record->id.page = id.page;
    END_USE_TABLE_PAGE_HANDLE_HEADER();

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

// Function to get the next record satisfying the condition
RC next(RM_ScanHandle *scan, Record *record) {
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
                return result;
            }

            // Check if a condition is provided
            if (scanData->condition != NULL) {
                result = evalExpr(record, scan->rel->schema, scanData->condition, &value);

                // Check if evaluating the expression failed
                if (result != RC_OK) {
                    freeVal(value);
                    return result;
                }

                // Check if the condition is satisfied
                if (value->v.boolV == TRUE) {
                    freeVal(value);
                    return RC_OK;
                }

                freeVal(value);
            } else {
                // If no condition is provided, return the record
                return RC_OK;
            }
        }
    }

    // No more tuples satisfying the condition
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

int getAttributeSize(Schema *schema, int attributeIndex) {
    // Check if the schema pointer is NULL
    if (schema == NULL) {
        // If the schema pointer is NULL, return -1 (Invalid schema pointer)
        return -1;
    }

    // Check if the attributeIndex is valid (within the range of attributes in the schema)
    if (attributeIndex < 0 || attributeIndex >= schema->numAttr) {
        // If the attributeIndex is invalid, return -1 (Invalid attribute index)
        return -1;
    }

    // Get the data type of the attribute from the schema
    DataType dataType = schema->dataTypes[attributeIndex];

    // Check the data type and return the appropriate size
    if (dataType == DT_INT) {
        // If the data type is DT_INT (integer), return 4 (Size of int is typically 4 bytes)
        return 4;
    } else if (dataType == DT_STRING) {
        // If the data type is DT_STRING (string), return the type length + 1 (to account for the null terminator)
        return schema->typeLength[attributeIndex] + 1;
    } else if (dataType == DT_FLOAT) {
        // If the data type is DT_FLOAT (float), return 4 (Size of float is typically 4 bytes)
        return 4;
    } else if (dataType == DT_BOOL) {
        // If the data type is DT_BOOL (boolean), return 1 (Size of bool is typically 1 byte)
        return 1;
    } else {
        // If the data type is unknown or not handled, return -1 (Unknown data type)
        return -1;
    }
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

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
    // Allocate memory for the Schema struct
    Schema *schema = (Schema *)malloc(sizeof(Schema));

    // Check if memory allocation for the Schema struct failed
    if (schema == NULL) {
        // If memory allocation failed, return NULL
        return NULL;
    }

    // Allocate memory for attribute names, data types, and type lengths
    schema->attrNames = (char **)malloc(numAttr * sizeof(char *));
    schema->dataTypes = (DataType *)malloc(numAttr * sizeof(DataType));
    schema->typeLength = (int *)malloc(numAttr * sizeof(int));

    // Check if memory allocation for attribute names, data types, or type lengths failed
    if (schema->attrNames == NULL || schema->dataTypes == NULL || schema->typeLength == NULL) {
        // If memory allocation failed, free the Schema struct and return NULL
        freeSchema(schema);
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
        // If memory allocation failed, free the Schema struct and return NULL
        freeSchema(schema);
        return NULL;
    }

    // Copy key attributes
    for (int i = 0; i < keySize; i++) {
        schema->keyAttrs[i] = keys[i];
    }

    // Set the numAttr and keySize fields of the Schema
    schema->numAttr = numAttr;
    schema->keySize = keySize;

    // If everything succeeded, return the created Schema
    return schema;
}
RC freeSchema(Schema *schema) {
    if (schema == NULL) {
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

    return RC_OK;
}

RC createRecord(Record **record, Schema *schema) {
    // Get the size of the record based on the schema
    int recordSize = getRecordSize(schema);

    // Allocate memory for the Record struct
    *record = (Record *)malloc(sizeof(Record));

    // Check if memory allocation for the Record struct failed
    if (*record == NULL) {
        // If memory allocation failed, return RC_MEMORY_ALLOCATION_FAILED
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Get a pointer to the newly allocated Record struct
    Record *recordPtr = *record;

    // Allocate memory for the record data using calloc (initialized with 0)
    recordPtr->data = (char *)calloc(recordSize, sizeof(char));

    // Check if memory allocation for the record data failed
    if (recordPtr->data == NULL) {
        // If memory allocation failed, free the previously allocated Record struct
        free(recordPtr);
        // Return RC_MEMORY_ALLOCATION_FAILED
        return RC_MEMORY_ALLOCATION_FAILED;
    }

    // Initialize the record fields
    // Set the page and slot fields of the record ID to -1 (indicating no page or slot assigned)
    recordPtr->id.page = -1;
    recordPtr->id.slot = -1;

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

RC freeRecord(Record *record) {
    // Check if the record pointer is NULL
    if (record == NULL) {
        // If the record pointer is NULL, return RC_INVALID_RECORD
        // Invalid record pointer
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

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attributeNum, Value **value) {
    // Check if the attributeNum is valid (within the range of schema attributes)
    if (!(attributeNum < schema->numAttr)) {
    // If not, return RC_WRITE_FAILED
    return RC_WRITE_FAILED;
}


    // Get a pointer to the start of the record data
    char *dataPtr = record->data;
    // Allocate memory for a new Value struct
    *value = (Value *)malloc(sizeof(Value));

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

    // Get a pointer to the newly allocated Value struct
    Value *valuePtr = *value;

    // Set the data type of the Value struct based on the schema
    valuePtr->dt = schema->dataTypes[attributeNum];

    // Based on the data type, retrieve the attribute value and set it in the Value struct
    if (valuePtr->dt == DT_INT) {
        // If the attribute is an integer, dereference dataPtr as an int pointer and set valuePtr->v.intV
        valuePtr->v.intV = *((int *)dataPtr);
    } else if (valuePtr->dt == DT_STRING) {
        // If the attribute is a string, allocate memory for the string and copy the string value
        valuePtr->v.stringV = (char *)malloc(attributeSize);
        strncpy(valuePtr->v.stringV, dataPtr, attributeSize);
    } else if (valuePtr->dt == DT_FLOAT) {
        // If the attribute is a float, dereference dataPtr as a float pointer and set valuePtr->v.floatV
        valuePtr->v.floatV = *((float *)dataPtr);
    } else {
        // If the attribute is a boolean, dereference dataPtr as a bool pointer and set valuePtr->v.boolV
        valuePtr->v.boolV = *((bool *)dataPtr);
    }

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

RC setAttr(Record *record, Schema *schema, int attributeNum, Value *value) {
    // Check if the attributeNum is valid (within the range of schema attributes)
    if (!(attributeNum < schema->numAttr)) {
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

    // Check the data type of the value and set the attribute accordingly
    if (value->dt == DT_INT) {
        // If the value is an integer, set the attribute to the integer value
        *((int *)dataPtr) = value->v.intV;
    } else if (value->dt == DT_STRING) {
        // If the value is a string, copy the string value to the attribute
        strncpy(dataPtr, value->v.stringV, attributeSize);
    } else if (value->dt == DT_FLOAT) {
        // If the value is a float, set the attribute to the float value
        *((float *)dataPtr) = value->v.floatV;
    } else if (value->dt == DT_BOOL) {
        // If the value is a boolean, set the attribute to the boolean value
        *((bool *)dataPtr) = value->v.boolV;
    } else {
        // If the data type is unknown, return RC_IM_ATTR_TYPE_UNKNOWN
        return RC_IM_ATTR_TYPE_UNKNOWN;
    }

    // If the function executed successfully, return RC_OK
    return RC_OK;
}

