#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>  

#define RC_BUFFER_POOL_OVERFLOW 45
#define GLOBAL_DEPTH 7
#define BUCKET_SIZE 4

typedef unsigned int TimeStamp; 

typedef struct BM_PageFrame {
    PageNumber pageNum;
    char* data;
    int fixCount;
    bool dirty;
    TimeStamp timestamp;
} BM_PageFrame;

typedef struct BM_Metadata {
    BM_PageFrame* pageFrames;
    SM_FileHandle pageFile;
    int queueIndex;
    bool isInitialized;
    int numRead;
    int numWrite;
    TimeStamp timestamp;
} BM_Metadata;

BM_Metadata metadata;  

typedef struct HashBucket {
    BM_PageFrame entries[BUCKET_SIZE];
    int count;
    int localDepth;
} HashBucket;

typedef struct {
    HashBucket* buckets;
    int numBuckets;
    int globalDepth;
    int numReadIO;
    int numWriteIO;
} BufferPoolHashTable;

unsigned long hashFunction(PageNumber pageNum, int depth) {
    return (pageNum >> (GLOBAL_DEPTH - depth)) & ((1 << depth) - 1);
}

TimeStamp getTimeStamp(BM_Metadata* metadata) {
    return metadata->timestamp;
}

BM_PageFrame _replacementFIFO(BM_BufferPool* const bm);  
BM_PageFrame _replacementLRU(BM_BufferPool* const bm);  

RC initBufferPool(BM_BufferPool* const bm, const char* const pageFileName, const int numPages, ReplacementStrategy strategy, void* stratData) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)malloc(sizeof(BufferPoolHashTable));
    hashTable->numBuckets = 1 << GLOBAL_DEPTH;
    hashTable->buckets = (HashBucket*)calloc(hashTable->numBuckets, sizeof(HashBucket));
    for (int i = 0; i < hashTable->numBuckets; i++) {
        hashTable->buckets[i].localDepth = GLOBAL_DEPTH;
    }
    hashTable->globalDepth = GLOBAL_DEPTH;
    hashTable->numReadIO = 0;
    hashTable->numWriteIO = 0;
    bm->mgmtData = hashTable;
    bm->numPages = numPages;
    bm->pageFile = (char*)pageFileName;
    bm->strategy = strategy;
    metadata.isInitialized = false;
    metadata.queueIndex = 0;
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    for (int i = 0; i < hashTable->numBuckets; i++) {
        HashBucket* bucket = &hashTable->buckets[i];
        for (int j = 0; j < bucket->count; j++) {
            free(bucket->entries[j].data);
        }
    }
    free(hashTable->buckets);
    free(hashTable);
    bm->mgmtData = NULL;
    bm->numPages = 0;
    bm->pageFile = NULL;
    return RC_OK;
}

RC pinPage(BM_BufferPool* const bm, BM_PageHandle* const page, const PageNumber pageNum) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    int depth = hashTable->globalDepth;
    unsigned long hashKey = hashFunction(pageNum, depth);
    HashBucket* bucket = &hashTable->buckets[hashKey];

    while (depth >= 0) {
        for (int i = 0; i < bucket->count; i++) {
            if (bucket->entries[i].pageNum == pageNum) {
                bucket->entries[i].fixCount++;
                page->pageNum = pageNum;
                page->data = bucket->entries[i].data;
                bucket->entries[i].timestamp = getTimeStamp(&metadata);  // Update timestamp
                return RC_OK;
            }
        }

        if (bucket->localDepth > depth) {
            depth--;
            hashKey = hashFunction(pageNum, depth);
            bucket = &hashTable->buckets[hashKey];
        } else {
            break;
        }
    }

    // Page not found in buffer pool, load from disk
    RC rc;
    BM_PageHandle newPage = {pageNum};
    rc = openPageFile(bm->pageFile, &metadata.pageFile);
    if (rc != RC_OK) {
        return rc;
    }
    rc = ensureCapacity(pageNum, &metadata.pageFile);
    if (rc != RC_OK) {
        closePageFile(&metadata.pageFile);
        return rc;
    }
    newPage.data = (char*)malloc(PAGE_SIZE);
    rc = readBlock(pageNum, &metadata.pageFile, newPage.data);
    closePageFile(&metadata.pageFile);
    if (rc != RC_OK) {
        free(newPage.data);
        return rc;
    }
    hashTable->numReadIO++;

    // Insert the new page into the bucket
    depth = hashTable->globalDepth;
    hashKey = hashFunction(pageNum, depth);
    bucket = &hashTable->buckets[hashKey];

    while (bucket->count == BUCKET_SIZE && bucket->localDepth == depth) {
        // Split the bucket
        int newDepth = depth + 1;
        if (newDepth > GLOBAL_DEPTH) {
            // Exceed the global depth limit, cannot split further
            return RC_BUFFER_POOL_OVERFLOW;
        }

        hashTable->numBuckets = 1 << newDepth;
        HashBucket* newBuckets = (HashBucket*)realloc(hashTable->buckets, hashTable->numBuckets * sizeof(HashBucket));
        if (newBuckets == NULL) {
            return RC_BUFFER_POOL_OVERFLOW;
        }
        hashTable->buckets = newBuckets;

        for (int i = 0; i < hashTable->numBuckets; i++) {
            if (i >= (1 << depth)) {
                hashTable->buckets[i].localDepth = newDepth;
            }
        }

        for (int i = 0; i < bucket->count; i++) {
            unsigned long newHashKey = hashFunction(bucket->entries[i].pageNum, newDepth);
            HashBucket* newBucket = &hashTable->buckets[newHashKey];
            newBucket->entries[newBucket->count++] = bucket->entries[i];
        }

        bucket->count = 0;
        depth = newDepth;
        hashKey = hashFunction(pageNum, depth);
        bucket = &hashTable->buckets[hashKey];
    }

    bucket->entries[bucket->count].pageNum = pageNum;
    bucket->entries[bucket->count].data = newPage.data;
    bucket->entries[bucket->count].fixCount = 1;
    bucket->entries[bucket->count].dirty = false;
    bucket->entries[bucket->count].timestamp = getTimeStamp(&metadata);  
    bucket->count++;

    *page = newPage;
    return RC_OK;
}

RC unpinPage(BM_BufferPool* const bm, BM_PageHandle* const page) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    int depth = hashTable->globalDepth;
    unsigned long hashKey = hashFunction(page->pageNum, depth);
    HashBucket* bucket = &hashTable->buckets[hashKey];

    while (depth >= 0) {
        for (int i = 0; i < bucket->count; i++) {
            if (bucket->entries[i].pageNum == page->pageNum) {
                bucket->entries[i].fixCount--;
                if (bucket->entries[i].fixCount == 0) {
                    // Implement page replacement strategy here
                    BM_PageFrame frameToReplace;
                    switch (bm->strategy) {
                        case RS_FIFO:
                            frameToReplace = _replacementFIFO(bm);
                            break;
                        case RS_LRU:
                            frameToReplace = _replacementLRU(bm);
                            break;
                        default:
                            // No replacement strategy specified
                            break;
                    }
                    // Handle the replaced page frame if needed
                }
                return RC_OK;
            }
        }

        if (bucket->localDepth > depth) {
            depth--;
            hashKey = hashFunction(page->pageNum, depth);
            bucket = &hashTable->buckets[hashKey];
        } else {
            break;
        }
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC markDirty(BM_BufferPool* const bm, BM_PageHandle* const page) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    int depth = hashTable->globalDepth;
    unsigned long hashKey = hashFunction(page->pageNum, depth);
    HashBucket* bucket = &hashTable->buckets[hashKey];

    while (depth >= 0) {
        for (int i = 0; i < bucket->count; i++) {
            if (bucket->entries[i].pageNum == page->pageNum) {
                bucket->entries[i].dirty = true;
                return RC_OK;
            }
        }

        if (bucket->localDepth > depth) {
            depth--;
            hashKey = hashFunction(page->pageNum, depth);
            bucket = &hashTable->buckets[hashKey];
        } else {
            break;
        }
    }

    return RC_READ_NON_EXISTING_PAGE;
}

RC forcePage(BM_BufferPool* const bm, BM_PageHandle* const page) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    RC rc = openPageFile(bm->pageFile, &metadata.pageFile);
    if (rc != RC_OK) {
        return rc;
    }
    rc = writeBlock(page->pageNum, &metadata.pageFile, page->data);
    closePageFile(&metadata.pageFile);
    if (rc != RC_OK) {
        return rc;
    }
    hashTable->numWriteIO++;
    return RC_OK;
}

PageNumber* getFrameContents(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    PageNumber* contents = (PageNumber*)malloc(bm->numPages * sizeof(PageNumber));
    int count = 0;
    for (int i = 0; i < hashTable->numBuckets; i++) {
        HashBucket* bucket = &hashTable->buckets[i];
        for (int j = 0; j < bucket->count; j++) {
            contents[count++] = bucket->entries[j].pageNum;
        }
    }
    return contents;
}

bool* getDirtyFlags(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    bool* dirtyFlags = (bool*)malloc(bm->numPages * sizeof(bool));
    memset(dirtyFlags, 0, bm->numPages * sizeof(bool));
    int count = 0;
    for (int i = 0; i < hashTable->numBuckets; i++) {
        HashBucket* bucket = &hashTable->buckets[i];
        for (int j = 0; j < bucket->count; j++) {
            dirtyFlags[count++] = bucket->entries[j].dirty;
        }
    }
    return dirtyFlags;
}

int* getFixCounts(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    int* fixCounts = (int*)malloc(bm->numPages * sizeof(int));
    memset(fixCounts, 0, bm->numPages * sizeof(int));
    int count = 0;
    for (int i = 0; i < hashTable->numBuckets; i++) {
        HashBucket* bucket = &hashTable->buckets[i];
        for (int j = 0; j < bucket->count; j++) {
            fixCounts[count++] = bucket->entries[j].fixCount;
        }
    }
    return fixCounts;
}

int getNumReadIO(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    return hashTable->numReadIO;
}

int getNumWriteIO(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    return hashTable->numWriteIO;
}

BM_PageFrame _replacementFIFO(BM_BufferPool* const bm) {
    if (!metadata.isInitialized) {
        metadata.pageFrames = (BM_PageFrame*)malloc(bm->numPages * sizeof(BM_PageFrame));
        metadata.queueIndex = 0;
        metadata.isInitialized = true;
    }

    BM_PageFrame* pageFrames = metadata.pageFrames;
    int queueIndex = metadata.queueIndex;

    BM_PageFrame frameToReplace = pageFrames[queueIndex];
    metadata.queueIndex = (queueIndex + 1) % bm->numPages;

    return frameToReplace;
}

BM_PageFrame _replacementLRU(BM_BufferPool* const bm) {
    if (!metadata.isInitialized) {
        metadata.pageFrames = (BM_PageFrame*)malloc(bm->numPages * sizeof(BM_PageFrame));
        metadata.isInitialized = true;
    }

    BM_PageFrame* pageFrames = metadata.pageFrames;

    TimeStamp oldestTimestamp = UINT_MAX;
    int oldestIndex = -1;

    for (int i = 0; i < bm->numPages; i++) {
        if (pageFrames[i].fixCount == 0 && pageFrames[i].timestamp < oldestTimestamp) {
            oldestTimestamp = pageFrames[i].timestamp;
            oldestIndex = i;
        }
    }

    if (oldestIndex == -1) {
        return pageFrames[0];
    }

    return pageFrames[oldestIndex];
}

RC forceFlushPool(BM_BufferPool* const bm) {
    BufferPoolHashTable* hashTable = (BufferPoolHashTable*)bm->mgmtData;
    RC rc = openPageFile(bm->pageFile, &metadata.pageFile);
    if (rc != RC_OK) {
        return rc;
    }

    for (int i = 0; i < hashTable->numBuckets; i++) {
        HashBucket* bucket = &hashTable->buckets[i];
        for (int j = 0; j < bucket->count; j++) {
            BM_PageFrame* frame = &bucket->entries[j];
            if (frame->dirty) {
                rc = writeBlock(frame->pageNum, &metadata.pageFile, frame->data);
                if (rc != RC_OK) {
                    closePageFile(&metadata.pageFile);
                    return rc;
                }
                frame->dirty = false;
                hashTable->numWriteIO++;
            }
        }
    }

    closePageFile(&metadata.pageFile);
    return RC_OK;
}
