#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <limits.h>
#define RC_BUFFERPOOL_NOT_INITIALIZED ((BM_PageFrame *)24)
#define RC_BUFFERPOOL_FULL ((BM_PageFrame *)25)
#define RC_BUFFERPOOL_ALL_PINNED ((BM_PageFrame *)27)
#define RC_BUFFER_FULL 23
#define RC_READ_FAILED 31
#define RC_CAPACITY_ERROR 30
#define RC_OTHER_ERROR 20
#define ARRAY_LIST_SIZE 16
#define PAGE_TABLE_SIZE 256


typedef struct HT_TableHandle {
    int size; // Size of the hash table
    void *mgmt; // Pointer to the management structure of the hash table
} HT_TableHandle; // Define structure for hash table handle

typedef unsigned int TimeStamp; // Define type for time stamp

typedef struct BM_PageFrame {
    char *data; // Pointer to data of the page frame
    PageNumber pageNum; // Page number of the page frame
    int frameIndex; // Index of the page frame
    int fixCount; // Fix count of the page frame
    bool dirty; // Dirty flag of the page frame
    bool occupied; // Occupied flag of the page frame
    TimeStamp timeStamp; // Time stamp of the page frame
} BM_PageFrame; // Define structure for buffer manager page frame

typedef struct HT_KeyValuePair {
    int key; // Key of the key-value pair
    int value; // Value of the key-value pair
} HT_KeyValuePair; // Define structure for hash table key-value pair

typedef struct BM_Metadata {
    BM_PageFrame *pageFrames; // Array of buffer manager page frames
    HT_TableHandle pageTable; // Handle to the hash table for page management
    SM_FileHandle pageFile; // File handle for page file
    TimeStamp timeStamp; // Time stamp for buffer manager metadata
    int queueIndex; // Index for buffer manager queue
    bool isInitialized;
    int numRead; // Number of reads performed
    int numWrite; // Number of writes performed
} BM_Metadata; // Define structure for buffer manager metadata

typedef struct HT_ArrayList {
    int capacity; // Capacity of the array list
    int size; // Current size of the array list
    HT_KeyValuePair *list; // Pointer to the array list
} HT_ArrayList; // Define structure for array list


HT_ArrayList *AL_get(HT_TableHandle *const ht, int i)
{
    HT_ArrayList *ls = (HT_ArrayList *)ht->mgmt;
    return &ls[i];
}

int AL_push(HT_ArrayList *al, int key, int value)
{
    if (al->size == al->capacity)
    {
        al->capacity += ARRAY_LIST_SIZE;
        al->list = realloc(al->list, sizeof(HT_KeyValuePair) * al->capacity);
        if (al->list == NULL)
            return 1;
    }
    al->list[al->size].value = value;
    al->list[al->size].key = key;
    al->size++;
    return 0;
}


// Function to remove an element at index i from the array list
void AL_remoteAt(HT_ArrayList *al, int i)
{
    if (i >= 0 && i < al->size)
    {
        for (int j = i; j < al->size - 1; j++)
            al->list[j] = al->list[j + 1]; // Shift elements to the left
        al->size--; // Decrement the size
    }
}

// Function to initialize the hash table
int initHashTable(HT_TableHandle *const ht, int size)
{
    if (size <= 0) {
        // Size must be positive
        return -1;
    }

    ht->size = size; // Set the size of the hash table
    ht->mgmt = malloc(sizeof(HT_ArrayList) * size); // Allocate memory for array of array lists
    if (ht->mgmt == NULL) {
        // Memory allocation failed
        return -1;
    }

    // Initialize each hash table entry
    for (int i = 0; i < size; i++)
    {
        HT_ArrayList *al = AL_get(ht, i); // Get the array list at index i
        if (al == NULL) {
            // Error accessing hash table entry
            free(ht->mgmt); // Free allocated memory
            return -1;
        }

        al->list = malloc(sizeof(HT_KeyValuePair) * ARRAY_LIST_SIZE); // Allocate memory for array list
        if (al->list == NULL)
        {
            // Memory allocation failed
            free(ht->mgmt); // Free allocated memory
            return -1;
        }
        al->capacity = ARRAY_LIST_SIZE; // Set capacity of the array list
        al->size = 0; // Initialize size of the array list
    }
    return 0; // Initialization successful
}

// Function to get the value corresponding to a key from the hash table
int getValue(HT_TableHandle *const ht, int key, int *value)
{
    int i = key % ht->size; // Calculate hash value
    HT_ArrayList *al = AL_get(ht, i); // Get the array list corresponding to the hash value

    int found = 0; // Flag variable to indicate whether key is found

    for (int j = 0; j < al->size && !found; j++)
    {
        found = (al->list[j].key == key); // Update flag if key is found
        *value = found ? al->list[j].value : *value; // Assign value if key is found
    }

    return !found; // Return 1 if key is not found, 0 otherwise
}

// Function to set the value corresponding to a key in the hash table
int setValue(HT_TableHandle *const ht, int key, int value)
{
    int i = key % ht->size; // Calculate hash value
    HT_ArrayList *al = AL_get(ht, i); // Get the array list corresponding to the hash value

    int found = 0; // Flag variable to indicate whether key is found

    for (int j = 0; j < al->size && !found; j++)
    {
        found = (al->list[j].key == key); // Update flag if key is found

        if (found)
            al->list[j].value = value; // Update value if key is found
    }

    if (!found)
        return AL_push(al, key, value); // Push new key-value pair if key is not found

    return 0; // Return 0 indicating success
}

// Function to remove a key-value pair from the hash table
int removePair(HT_TableHandle *const ht, int key)
{
    int i = key % ht->size; // Calculate hash value
    HT_ArrayList *al = AL_get(ht, i); // Get the array list corresponding to the hash value

    int found = 0; // Flag variable to indicate whether key is found

    for (int j = 0; j < al->size && !found; j++)
    {
        found = (al->list[j].key == key); // Update flag if key is found

        if (found)
            AL_remoteAt(al, j); // Remove pair if key is found
    }

    return !found; // Return 1 if key is not found, 0 otherwise
}

void freeHashTable(HT_TableHandle *const ht)
{
    int i = 0; // Initialize loop counter
    while (i < ht->size) // Iterate through each element in the hash table
    {
        HT_ArrayList *al = AL_get(ht, i++); // Get the ArrayList at index i and increment i
        free(al->list); // Free the memory allocated for the ArrayList's list
    }
    free(ht->mgmt); // Free the memory allocated for hash table management structure
}

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm);
TimeStamp getTimeStamp(BM_Metadata *metadata);
BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex);
BM_PageFrame *replacementLRU(BM_BufferPool *const bm);



RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    (void)stratData;
    // Allocate and initialize metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata)); // Allocate memory for buffer pool metadata
    // Open page file
    RC result = openPageFile((char *)pageFileName, &(metadata->pageFile)); // Open the page file
    if (metadata == NULL) {
        return RC_OTHER_ERROR; // Return error code if memory allocation failed
    }

    HT_TableHandle *pageTable = &(metadata->pageTable); // Get a pointer to the page table
    metadata->timeStamp = 0; // Initialize timestamp
    // Other metadata initialization

    metadata->queueIndex = numPages - 1; // Initialize queue index
    metadata->numRead = 0; // Initialize number of reads
    metadata->isInitialized = true;
    metadata->numWrite = 0; // Initialize number of writes

    if (result != RC_OK) {
        free(metadata); // Release allocated memory in case of failure
        return result; // Return error code
    }
    bm->mgmtData = (void *)metadata; // Set buffer pool management data
    bm->pageFile = (char *)&(metadata->pageFile); // Set page file

    // Initialize page table and page frames
    initHashTable(pageTable, PAGE_TABLE_SIZE); // Initialize hash table
    metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numPages); // Allocate memory for page frames
    bm->strategy = strategy; // Set replacement strategy
    bm->numPages = numPages; // Set number of pages
    
    if (metadata->pageFrames == NULL) {
        free(metadata); // Release allocated memory in case of failure
        return RC_OTHER_ERROR; // Return error code
    }

    // Initialize page frames sequentially using a while loop
     for (int i = 0; i < numPages; i++) {
        metadata->pageFrames[i].data = (char *)malloc(PAGE_SIZE); // Allocate memory for page frame data
        metadata->pageFrames[i].frameIndex = i; // Set frame index
        metadata->pageFrames[i].timeStamp = getTimeStamp(metadata); // Get timestamp

        if (metadata->pageFrames[i].data == NULL) {
            exit(EXIT_FAILURE); // Exit program if memory allocation fails
        }
        metadata->pageFrames[i].occupied = false; // Set occupied flag to false
        metadata->pageFrames[i].fixCount = 0; // Initialize fix count
        metadata->pageFrames[i].dirty = false; // Set dirty flag to false
    }

    return RC_OK; // Return success code
}


// Function to shutdown a buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    // Extract metadata from buffer pool
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    // Extract page frames from metadata
    BM_PageFrame *pageFrames = metadata->pageFrames;

    // Check if the buffer pool is initialized
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error code if not initialized
    }

    HT_TableHandle *pageTable = &(metadata->pageTable); // Extract page table from metadata

    // Check if any page has an outstanding fix count
    int i = 0;
    while (i < bm->numPages && pageFrames[i].fixCount == 0)
    {
        i++;
    }

    if (i < bm->numPages)
    {
        return RC_WRITE_FAILED; // Return error code if any page has outstanding fix count
    }

    // Flush all dirty pages to disk
    forceFlushPool(bm);

    // Free allocated memory for each page frame data
    i = 0;
    while (i < bm->numPages)
    {
        if (pageFrames[i].data != NULL)
        {
            free(pageFrames[i].data);
            pageFrames[i].data = NULL; // Set the pointer to NULL after freeing to avoid dangling pointers
        }
        i++;
    }

    // Close the page file
    closePageFile(&(metadata->pageFile));

    // Free resources related to the page table
    freeHashTable(pageTable);

    // Free the array of page frames
    free(pageFrames);

    // Set mgmtData to NULL after freeing to avoid dangling pointers
    bm->mgmtData = NULL;

    // Free metadata structure
    free(metadata);

    return RC_OK; // Return success code
}

// Function to force flush dirty pages to disk
RC forceFlushPool(BM_BufferPool *const bm) {
    // Check if the buffer pool is initialized
    if (bm == NULL || bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT; // Return error code if not initialized
    }

    // Extract metadata from buffer pool
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    // Extract page frames from metadata
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int i = 0;  // Initialize loop variable

    // Use a while loop instead of a for loop
    while (i < bm->numPages) {
        // Skip iteration if the page is not occupied, dirty, or has outstanding fixes
        if (!(pageFrames[i].occupied && pageFrames[i].dirty && pageFrames[i].fixCount == 0)) {
            i++;  // Increment loop variable
            continue;
        }

        // Write the block to disk
        writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);

        pageFrames[i].timeStamp = getTimeStamp(metadata); // Update timestamp
        metadata->numWrite++; // Increment write count in metadata
        pageFrames[i].dirty = false; // Mark the page as clean

        i++;  // Increment loop variable
    }

    return RC_OK; // Return success code
}

// Function to mark a page as dirty
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int frameIndex;
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    // Check if the buffer pool is initialized
    if (bm->mgmtData == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT; // Return error code if not initialized
    }

    HT_TableHandle *pageTable = &(metadata->pageTable); // Extract page table from metadata
    // Check if the page is in the buffer pool
    int result = getValue(pageTable, page->pageNum, &frameIndex);

    switch (result)
    {
        case 0:
            // Update the timestamp and mark the page as dirty
            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata); // Update timestamp
            pageFrames[frameIndex].dirty = true; // Mark page as dirty
            return RC_OK; // Return success code
        
        default:
            return RC_IM_KEY_NOT_FOUND; // Return error code if page not found in buffer pool
    }
}


RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
     int frameIndex;
     BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    // Check if the buffer pool is initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Get the mapped frameIndex from pageNum
    if (getValue(pageTable, page->pageNum, &frameIndex) != 0) {
        return RC_IM_KEY_NOT_FOUND;
    }

    // Update timestamp and fixCount
    pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
    // Decrement fixCount (not below 0)
    pageFrames[frameIndex].fixCount = (pageFrames[frameIndex].fixCount > 0) ? pageFrames[frameIndex].fixCount - 1 : 0;

    return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    // Check if the buffer pool is initialized
    if (bm->mgmtData == NULL)
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    int frameIndex;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Check if the page is in the buffer pool
    if (getValue(pageTable, page->pageNum, &frameIndex) == 0)
    {
        BM_PageFrame *pageFrame = &pageFrames[frameIndex];

        // Check if the page has no outstanding fixes
        switch (pageFrame->fixCount) {
            case 0:
                // Write the block to disk only if it's dirty
                if (pageFrame->dirty)
                {
                    writeBlock(page->pageNum, &(metadata->pageFile), pageFrame->data);

                    // Update metadata and page frame information
                    metadata->numWrite++;
                    pageFrame->dirty = false;

                    return RC_OK;
                }
                else
                {
                    // Page is not dirty, no need to write to disk
                    return RC_OK;
                }
                break;
            default:
                // Page has outstanding fixes, cannot write to disk
                return RC_WRITE_FAILED;
        }
    }
    else
    {
        // Page is not in the buffer pool
        return RC_IM_KEY_NOT_FOUND;
    }
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    
    // Check if the buffer pool is initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Check if the page number is valid
    if (pageNum < 0) {
        return RC_IM_KEY_NOT_FOUND;
    }

    int frameIndex;

    // Check if the page is already in the buffer pool
    if (getValue(pageTable, pageNum, &frameIndex) == 0) {
        // Page is in the buffer pool, update metadata and return
        pageFrames[frameIndex].fixCount++;
        page->pageNum = pageNum;
        pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
        page->data = pageFrames[frameIndex].data;
        return RC_OK;
    }

    // Page is not in the buffer pool, replace a page using the specified strategy
    BM_PageFrame *pageFrame = (bm->strategy == RS_FIFO) ? replacementFIFO(bm) : replacementLRU(bm);

    // Check if replacement was unsuccessful
    if (pageFrame == NULL) {
        return RC_WRITE_FAILED; // Or another appropriate error code indicating all pages are pinned
    }

    // Ensure capacity and read block
    if (ensureCapacity(pageNum + 1, &(metadata->pageFile)) != RC_OK) {
        return RC_CAPACITY_ERROR;
    }

    if (readBlock(pageNum, &(metadata->pageFile), pageFrame->data) != RC_OK) {
        return RC_READ_FAILED;
    }

    // Update metadata with the new page information
    setValue(pageTable, pageNum, pageFrame->frameIndex);

    // Update page frame metadata
    metadata->numRead++;
    pageFrame->occupied = true;
    pageFrame->dirty = false;
    pageFrame->fixCount = 1;
    pageFrame->pageNum = pageNum;

    // Update the output page handle
    page->pageNum = pageNum;
    page->data = pageFrame->data;

    return RC_OK;
}

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    if (bm->mgmtData == NULL)
        return NULL;

    PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
    int i = 0;
    while (i < bm->numPages)
    {
        switch (pageFrames[i].occupied)
        {
        case true:
            array[i] = pageFrames[i].pageNum;
            break;
        case false:
            array[i] = NO_PAGE;
            break;
        }
        i++;
    }
    return array;
}

bool *getDirtyFlags(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    if (bm->mgmtData == NULL)
        return NULL;

    bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
    int i = 0;
    while (i < bm->numPages)
    {
        switch (pageFrames[i].occupied)
        {
        case true:
            array[i] = pageFrames[i].dirty;
            break;
        case false:
            array[i] = false;
            break;
        }
        i++;
    }
    return array;
}

int *getFixCounts(BM_BufferPool *const bm) {
    if (bm->mgmtData == NULL) {
        return NULL; // Invalid buffer pool
    }

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int *fixCounts = (int *)calloc(bm->numPages, sizeof(int));
    if (fixCounts == NULL) {
        return NULL; 
    }

    int i = 0;
    while (i < bm->numPages) {
        switch (pageFrames[i].occupied) {
            case true:
                fixCounts[i] = pageFrames[i].fixCount;
                break;
        }
        i++;
    }

    return fixCounts;
}


// Function to retrieve the number of read IO operations performed on the buffer pool
int getNumReadIO(BM_BufferPool *const bm)
{
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return 0; // Return 0 if buffer pool is not initialized
    }

    return ((BM_Metadata *)bm->mgmtData)->numRead; // Retrieve number of read IO operations from metadata
}

// Function to retrieve the number of write IO operations performed on the buffer pool
int getNumWriteIO(BM_BufferPool *const bm)
{
    if (bm == NULL || bm->mgmtData == NULL)
    {
        return 0; // Return 0 if buffer pool is not initialized
    }

    return ((BM_Metadata *)bm->mgmtData)->numWrite; // Retrieve number of write IO operations from metadata
}

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm) {
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    if (bm == NULL || bm->mgmtData == NULL) {
        // Return error code for NULL buffer pool or metadata
        return RC_BUFFERPOOL_NOT_INITIALIZED;
    }
    int currentIndex = metadata->queueIndex;
    
    int numPages = bm->numPages;

    // Find the first unpinned page or loop back to the starting point
    do {
        currentIndex = (currentIndex + 1) % numPages;
        if (pageFrames[currentIndex].fixCount == 0) {
            metadata->queueIndex = currentIndex;
            return getAfterEviction(bm, currentIndex);
        }
    } while (currentIndex != metadata->queueIndex);

    // No unpinned page found
    metadata->queueIndex = currentIndex; // Update queueIndex

    // Check if all frames are pinned
    for (int i = 0; i < numPages; i++) {
        if (pageFrames[i].fixCount == 0)
            return RC_BUFFERPOOL_FULL; // Return error code for full buffer pool
    }

    // All frames are pinned, return appropriate error code
    return RC_BUFFERPOOL_ALL_PINNED;
}




// Helper function to check if all pages are fixed
bool allPagesFixed(BM_PageFrame *pageFrames, int numPages) {
    for (int i = 0; i < numPages; i++) {
        if (pageFrames[i].fixCount == 0) {
            return false; 
        }
    }
    return true; 
}

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    return metadata->timeStamp++;
}

// Existing function with improvements
void updateTimeStamp(BM_PageFrame *pageFrame, BM_Metadata *metadata) {
    pageFrame->timeStamp = getTimeStamp(metadata);
}

void evictPage(BM_PageFrame *pageFrame, HT_TableHandle *pageTable, BM_Metadata *metadata) {
    // Remove entry from the pageTable
    removePair(pageTable, pageFrame->pageNum);

    // If the frame is dirty, write it back to the page file
    if (pageFrame->dirty) {
        writeBlock(pageFrame->pageNum, &(metadata->pageFile), pageFrame->data);
        metadata->numWrite++;
    }
}

BM_PageFrame *replacementLRU(BM_BufferPool *const bm) {
    // Cast the management data of the buffer pool to BM_Metadata
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    // Get the array of page frames from the metadata
    BM_PageFrame *pageFrames = metadata->pageFrames;

    // If the buffer pool has no pages or all pages are fixed, return NULL
    if (bm->numPages == 0 || allPagesFixed(pageFrames, bm->numPages)) {
        return NULL;
    }

    // Initialize variables to find the page with the least recent timestamp
    TimeStamp minTimestamp = UINT_MAX;
    int replacementIndex = -1;
    int i = 0;  
    // Find the first unfixed page
    while (i < bm->numPages && pageFrames[i].fixCount != 0) {
        i++;
    }

    // Loop through all pages to find the one with the minimum timestamp
    while (i < bm->numPages) {
        if (pageFrames[i].fixCount == 0 && pageFrames[i].timeStamp < minTimestamp) {
            // Update the minimum timestamp and the index of the replacement page
            replacementIndex = i;
            minTimestamp = pageFrames[i].timeStamp;
        }
        i++;
    }

    // If no replacement page is found, return NULL
    if (replacementIndex == -1) {
        return NULL;
    } else {
        // Increment timestamps of all other unfixed pages except the replacement page
        for (int i = 0; i < bm->numPages; i++) {
            if (i != replacementIndex && pageFrames[i].fixCount == 0) {
                pageFrames[i].timeStamp++;
            }
        }

        // Update the timestamp of the replacement page to the minimum timestamp found
        pageFrames[replacementIndex].timeStamp = minTimestamp;

        // Get the replacement page after eviction
        return getAfterEviction(bm, replacementIndex);
    }
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex) {
    // Cast the management data of the buffer pool to BM_Metadata
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    // Get the array of page frames from the metadata
    BM_PageFrame *pageFrames = metadata->pageFrames;

    // If the frame index is out of bounds, return NULL
    if (frameIndex < 0 || frameIndex >= bm->numPages) {
        return NULL;
    }
    
    // Get a handle to the page table from the metadata
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Update the timestamp of the page frame at the given index
    updateTimeStamp(&(pageFrames[frameIndex]), metadata);

    // If the page frame is occupied, evict the page from the page table
    if (pageFrames[frameIndex].occupied) {
        evictPage(&(pageFrames[frameIndex]), pageTable, metadata);
    }

    // Return a pointer to the page frame at the given index
    return &(pageFrames[frameIndex]);
}

