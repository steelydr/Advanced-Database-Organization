#include <string.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#define RC_ERROR_NODE_NOT_FOUND 94
#define RC_ERROR_INVALID_INDEX 95
#define RC_ERROR_NULL_POINTER 96
#define RC_ERROR_NOT_ROOT 89
#define RC_ERROR_EMPTY_ROOT 90
#define RC_ERROR_CONDITION_NOT_MET 92
#define RC_MEM_ERROR 93
#define RC_OUT_OF_MEMORY 120
#define RC_INVALID_HANDLE 130
#define MAX_LINE_LENGTH 100
#define MAX_T_LENGTH 10

typedef struct RM_BtreeNode {
    Value *keys;
    void **ptr;
    struct RM_BtreeNode *pptr;
    bool isLeaf;
    int KeCounts;
    int pos; 
} RM_BtreeNode;

typedef struct RM_bTree_mgmtData {
    BM_BufferPool *bp;
    int maxKeNum;
    int num;
} RM_bTree_mgmtData;

typedef struct RM_BScan_mgmt{
    int tot_Scan;
    RM_BtreeNode *cur;
    int index;
}RM_BScan_mgmt;

typedef struct TreeNode {
    int val;
    struct TreeNode *left;
    struct TreeNode *right;
} TreeNode;

char *s_s = NULL;
char *s = NULL;
Value empty;
int numNodValue = 0;
int globalPos = 0;
int numLevels = 0;
int sizeofNod = 0;
RM_BtreeNode *root = NULL;

RC mergeNodes(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int position, int NumKeys, int *brotherNumKeys);
RC deleteLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys);
RC deleteNonLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys);
RC splitLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index);
RC mergeNodesAtZeroPosition(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int NumKeys);
RC insertNewRoot(RM_bTree_mgmtData *bTreeMgmt, Value *key, RID rid);
RC insertKeyHelper(BTreeHandle *tree, Value *key, RID rid);
RC insertInLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index);
RC insertLeaf(RM_BtreeNode *leaf, Value *key, RID rid);
int walk(RM_BtreeNode *bTreeNode, char *result);

bool strNoLarger(char *c, char *c1) {
    // Initialize a pointer ptr_c to the start of string c
    char *ptr_c = c;
    // Initialize a pointer ptr_c1 to the start of string c1
    char *ptr_c1 = c1;

    // Move ptr_c to the end of string c
    while (*ptr_c != '\0')
        ptr_c++;

    // Move ptr_c1 to the end of string c1
    while (*ptr_c1 != '\0')
        ptr_c1++;

    // Calculate the length of string c by subtracting the start pointer from the end pointer
    ptrdiff_t len_c = ptr_c - c;
    // Calculate the length of string c1 by subtracting the start pointer from the end pointer
    ptrdiff_t len_c1 = ptr_c1 - c1;

    // If the length of c is less than the length of c1, return true
    if (len_c < len_c1)
        return true;

    // If the length of c is greater than the length of c1, return false
    if (len_c > len_c1)
        return false;

    // Compare the characters of c and c1 one by one
    while (*c != '\0') {
        // If the current character of c is less than the current character of c1, return true
        if (*c < *c1)
            return true;

        // If the current character of c is greater than the current character of c1, return false
        if (*c > *c1)
            return false;

        // Move to the next character in c
        c++;
        // Move to the next character in c1
        c1++;
    }

    // If all characters are equal and the end of the strings is reached, return true
    return true;
}

RM_BtreeNode *createNewNod(RM_BtreeNode *thisNode) {
    // Initialize globalPos to 0
    globalPos = 0;

    // Allocate memory for a new RM_BtreeNode
    RM_BtreeNode *bTreeNode = (RM_BtreeNode *)malloc(sizeof(RM_BtreeNode));

    // Check if memory allocation failed
    if (bTreeNode == NULL) {
        // Set globalPos to -1 to indicate an error
        globalPos = -1;
        // Return NULL to indicate failure
        return NULL;
    }

    // Allocate memory for the 'ptr' array in the new node
    bTreeNode->ptr = malloc(sizeofNod * sizeof(void *));
    // Allocate memory for the 'keys' array in the new node
    bTreeNode->keys = malloc((sizeofNod - 1) * sizeof(Value));

    // Check if memory allocation failed for 'ptr' or 'keys'
    if (bTreeNode->ptr == NULL || bTreeNode->keys == NULL) {
        // Free the allocated memory for 'ptr' if it was successful
        free(bTreeNode->ptr);
        // Free the allocated memory for 'keys' if it was successful
        free(bTreeNode->keys);
        // Free the allocated memory for the new node
        free(bTreeNode);
        // Set globalPos to -1 to indicate an error
        globalPos = -1;
        // Return NULL to indicate failure
        return NULL;
    }

    // Initialize all elements of the 'ptr' array to NULL
    for (int i = 0; i < sizeofNod; i++) {
        bTreeNode->ptr[i] = NULL;
    }

    // Set the parent pointer of the new node to NULL
    bTreeNode->pptr = NULL;
    // Set the key count of the new node to 0
    bTreeNode->KeCounts = 0;
    // Set the isLeaf flag of the new node to false
    bTreeNode->isLeaf = false;
    // Set the position of the new node to 0
    bTreeNode->pos = 0;

    // Increment the global node count
    numNodValue++;

    // Return the newly created node
    return bTreeNode;
}
RC deleteNode(RM_BtreeNode *bTreeNode, int index) {
    // Check if the bTreeNode is NULL
    if (bTreeNode == NULL) {
        return RC_ERROR_NULL_POINTER;
    }
    
    // Check if the index is out of range
    if (index < 0 || index >= (*bTreeNode).KeCounts) {
        return RC_ERROR_INVALID_INDEX;
    }
    
    // Declare a pointer to store the sibling node
    RM_BtreeNode *brother;
    
    // Decrement the key count of the current node
    (*bTreeNode).KeCounts--;
    
    // Store the updated key count in a variable
    int NumKeys = (*bTreeNode).KeCounts;
    
    // Declare variables for position, loop counters, and temporary storage
    int position, i, j;
    
    // Check if the current node is a leaf node
    if ((*bTreeNode).isLeaf) {
        // Call the deleteLeafNode function to handle leaf node deletion
        deleteLeafNode(bTreeNode, index, NumKeys);
    } else {
        // Call the deleteNonLeafNode function to handle non-leaf node deletion
        deleteNonLeafNode(bTreeNode, index, NumKeys);
    }
    
    // Calculate the minimum number of keys required based on the node type
    i = (*bTreeNode).isLeaf ? (sizeofNod >> 1) : ((sizeofNod - 1) >> 1);
    
    // Set the return code based on the number of remaining keys
    RC returnCode = (NumKeys >= i) ? RC_OK : 0;
    
    // Check if the current node is not the root
    if (bTreeNode != root) {
        return RC_ERROR_NOT_ROOT;
    }
    
    // Check if the root is empty or has no keys
    if (!root || !root->KeCounts | !(root->KeCounts & 0x7fffffff)) {
        return RC_ERROR_EMPTY_ROOT;
    }
    
    // Declare a pointer to store the new root
    RM_BtreeNode *newRoot = NULL;
    
    // Check if the root is not NULL and is a leaf node
    if (!(root == NULL || !root->isLeaf)) {
        // Set the new root to the first child of the current root
        newRoot = *(root->ptr);
        // Set the parent pointer of the new root to NULL
        (newRoot->pptr) = NULL;
    }
    
    // Check if the root exists
    if (root) {
        // Free the memory allocated for the keys of the root
        if ((*root).keys) {
            free((*root).keys);
            (*root).keys = NULL;
        }
        
        // Free the memory allocated for the pointers of the root
        if ((*root).ptr) {
            free((*root).ptr);
            (*root).ptr = NULL;
        }
        
        // Free the memory allocated for the root
        free(root);
        root = NULL;
        
        // Decrement the node count
        numNodValue--;
    }
    
    // Update the root to the new root
    root = newRoot;
    
    // Return success
    return RC_OK;
    
    // Get the parent node of the current node
    RM_BtreeNode *parentNode = (*bTreeNode).pptr;
    
    // Check if the parent node is NULL
    if (parentNode == NULL) {
        return RC_ERROR_NULL_POINTER;
    }
    
    // Initialize the position to 0
    position = 0;
    
    // Find the position of the current node in the parent node
    while (position < (*parentNode).KeCounts && (*parentNode).ptr[position] != bTreeNode) {
        position++;
    }
    
    // Check if the current node is not found in the parent node
    if (position == (*parentNode).KeCounts) {
        return RC_ERROR_NODE_NOT_FOUND;
    }
    
    // Get the sibling node based on the position
    brother = (position == 0) ? (*parentNode).ptr[1] : (*parentNode).ptr[position - 1];
    
    // Check if the sibling node is NULL
    if (brother == NULL) {
        return RC_ERROR_NULL_POINTER;
    }
    
    // Calculate the maximum number of keys allowed in the merged node
    i = (*bTreeNode).isLeaf ? sizeofNod - 1 : sizeofNod - 2;
    
    // Check if merging the current node and sibling node exceeds the maximum allowed keys
    if ((*brother).KeCounts + NumKeys > i) {
        return RC_ERROR_CONDITION_NOT_MET;
    }
    
    // Check if the current node is the leftmost child
    if (!position) {
        // Swap the current node and sibling node using XOR swap
        bTreeNode = (RM_BtreeNode *)((uintptr_t)bTreeNode ^ (uintptr_t)brother);
        brother = (RM_BtreeNode *)((uintptr_t)bTreeNode ^ (uintptr_t)brother);
        bTreeNode = (RM_BtreeNode *)((uintptr_t)bTreeNode ^ (uintptr_t)brother);
        
        // Update the position to 1
        position = 1;
        
        // Update the number of keys in the current node
        NumKeys = bTreeNode->KeCounts;
    }
    
    // Store the number of keys in the sibling node
    i = (*brother).KeCounts;
    
    // Check if the current node is not a leaf node
    if (!(*bTreeNode).isLeaf) {
        // Copy the key from the parent node to the sibling node
        *(brother->keys + i) = *(parentNode->keys + position - 1);
        
        // Increment the index and the number of keys
        i++;
        NumKeys += 1;
    }
    
    // Initialize the loop counter j to 0
    j = 0;
    
    // Merge the keys and pointers from the current node to the sibling node
    while (j < NumKeys) {
        *(brother->keys + i) = *(bTreeNode->keys + j);
        *(brother->ptr + i) = *(bTreeNode->ptr + j);
        
        // Clear the keys and pointers in the current node
        *(bTreeNode->keys + j) = empty;
        *(bTreeNode->ptr + j) = NULL;
        
        // Increment the loop counters
        i = i + 1;
        j = j + 1;
    }
    // Update the sibling pointer using XOR swap
    (*brother).ptr[sizeofNod - 1] = (void*)((uintptr_t)(*brother).ptr[sizeofNod - 1] ^ (uintptr_t)(*bTreeNode).ptr[sizeofNod - 1]);
    (*bTreeNode).ptr[sizeofNod - 1] = (void*)((uintptr_t)(*brother).ptr[sizeofNod - 1] ^ (uintptr_t)(*bTreeNode).ptr[sizeofNod - 1]);
    (*brother).ptr[sizeofNod - 1] = (void*)((uintptr_t)(*brother).ptr[sizeofNod - 1] ^ (uintptr_t)(*bTreeNode).ptr[sizeofNod - 1]);
    
    // Delete the non-leaf node
    deleteNonLeafNode(bTreeNode, index, NumKeys);
    // Decrement the node count
    numNodValue -= 1;
    
    // Free the memory allocated for the keys of the current node
    if (bTreeNode->keys) {
        free(bTreeNode->keys);
        bTreeNode->keys = NULL;
    }
    // Free the memory allocated for the pointers of the current node
    if (bTreeNode->ptr) {
        free(bTreeNode->ptr);
        bTreeNode->ptr = NULL;
    }
    // Free the memory allocated for the current node
    free(bTreeNode);
    bTreeNode = NULL;
    // Recursively delete the parent node
    return deleteNode(parentNode, position);
    // Store the number of keys in the sibling node
    int brotherNumKeys = (*brother).KeCounts;
    // Check if the current node is not the leftmost child
    if (position != 0) {
        // Merge the current node with the sibling node
        mergeNodes(bTreeNode, brother, parentNode, position, NumKeys, &brotherNumKeys);
    } else {
        // Merge the current node with the sibling node at position 0
        mergeNodesAtZeroPosition(bTreeNode, brother, parentNode, NumKeys);
    }
    // Increment the key count of the current node
    (*bTreeNode).KeCounts++;
    
    // Decrement the key count of the sibling node
    (*brother).KeCounts--;
    
    // Return success
    return RC_OK;
}

// Function to delete a leaf node in a B-tree
RC deleteLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys) {
    // Free the memory allocated for the pointer at the specified index
    free((*bTreeNode).ptr[index]);
    // Set the pointer at the specified index to NULL
    (*bTreeNode).ptr[index] = NULL;

    // Shift keys and pointers to the left after the deleted key
    memmove(&(*bTreeNode).keys[index], &(*bTreeNode).keys[index + 1], (NumKeys - index) * sizeof((*bTreeNode).keys[0]));
    memmove(&(*bTreeNode).ptr[index], &(*bTreeNode).ptr[index + 1], (NumKeys - index) * sizeof((*bTreeNode).ptr[0]));

    // Set the last key and pointer to empty and NULL, respectively
    (*bTreeNode).keys[NumKeys] = empty;
    (*bTreeNode).ptr[NumKeys] = NULL;

    return RC_OK; // Return success code
}

// Function to delete a non-leaf node in a B-tree
RC deleteNonLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys) {
    // Shift keys and pointers to the left after the deleted key
    memmove(&(*bTreeNode).keys[index - 1], &(*bTreeNode).keys[index], (NumKeys - index) * sizeof((*bTreeNode).keys[0]));
    memmove(&(*bTreeNode).ptr[index], &(*bTreeNode).ptr[index + 1], (NumKeys - index + 1) * sizeof((*bTreeNode).ptr[0]));

    // Set the last key to empty and the last pointer to NULL
    (*bTreeNode).keys[NumKeys - 1] = empty;
    (*bTreeNode).ptr[NumKeys] = NULL;

    return RC_OK; // Return success code
}

// Function to merge two sibling nodes
RC mergeNodes(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int position, int NumKeys, int *brotherNumKeys) {
    // If the current node is not a leaf, adjust pointers
    if (!(*bTreeNode).isLeaf) {
        (*bTreeNode).ptr[NumKeys + 1] = (*bTreeNode).ptr[NumKeys];
    }

    // Shift keys and pointers in the current node to the right
    memmove(&(*bTreeNode).keys[1], &(*bTreeNode).keys[0], NumKeys * sizeof((*bTreeNode).keys[0]));
    memmove(&(*bTreeNode).ptr[1], &(*bTreeNode).ptr[0], NumKeys * sizeof((*bTreeNode).ptr[0]));

    // Determine the number of keys in the brother node
    *brotherNumKeys = (*bTreeNode).isLeaf ? (uintptr_t)(*brother).KeCounts - 1 : (uintptr_t)brother->KeCounts - 1;

    // Copy the last key from the brother node to the first position of the current B-tree node
    bTreeNode->keys[0] = (*brother).keys[*brotherNumKeys];

    // Replace the key in the parent node with the first key of the current B-tree node
    parentNode->keys[position - 1] = bTreeNode->keys[0];

    // Update pointer in the current node with the last pointer from the brother node
    (*bTreeNode).ptr[0] = (*brother).ptr[*brotherNumKeys];

    // Clear the last key and pointer in the brother node
    (*brother).keys[*brotherNumKeys] = empty;
    (*brother).ptr[*brotherNumKeys] = NULL;

    return RC_OK; // Return success code
}


// Function to merge two sibling nodes, where the left sibling has only one key, 
// and the right sibling has multiple keys, at the zero position
RC mergeNodesAtZeroPosition(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int NumKeys) {
    // Get the number of keys in the right sibling node
    int brotherNumKeys = (*brother).KeCounts;
    // Determine the index of the sibling key
    int siblingKeyIndex = (*bTreeNode).isLeaf ? 1 : 0;

    // Copy the sibling key and pointer to the target node
    (*bTreeNode).keys[NumKeys] = (*bTreeNode).isLeaf ? (*brother).keys[0] : (*parentNode).keys[0];
    (*bTreeNode).ptr[NumKeys + !(*bTreeNode).isLeaf] = (*brother).ptr[0];
    // Update the key in the parent node
    (*parentNode).keys[0] = (*brother).keys[siblingKeyIndex];

    // Shift keys and pointers in the sibling node to the left
    memmove(&(*brother).keys[0], &(*brother).keys[1], (brotherNumKeys - 1) * sizeof((*brother).keys[0]));
    memmove(&(*brother).ptr[0], &(*brother).ptr[1], (brotherNumKeys - 0) * sizeof((*brother).ptr[0]));

    // Clear the last key and pointer in the sibling node
    (*brother).keys[brotherNumKeys - 1] = empty;
    (*brother).ptr[brotherNumKeys] = NULL;

    return RC_OK; // Return success code
}

// Function to initialize the index manager
RC initIndexManager(void *mgmtData) {
    root = NULL; // Initialize the root of the B-tree to NULL
    numNodValue = sizeofNod = empty.v.intV = 0; // Initialize various counters and values to 0
    empty.dt = DT_INT; // Set the data type of an empty value to integer
    return RC_OK; // Return success code
}

// Function to insert a new parent node between two child nodes in the B-tree
RC insertParent(RM_BtreeNode *left, RM_BtreeNode *right, Value key) {
    RM_BtreeNode *pptr = left->pptr; // Get the parent node of the left child node
    int i = 0, index = 0; // Declare variables for iteration and index tracking
    
    // Check if the parent node is NULL (indicating it's the root node)
    if (pptr == NULL) {
        // Create a new root node
        RM_BtreeNode *NewRoot = createNewNod(NewRoot);
        if (NewRoot == NULL)
            return RC_MEM_ERROR; // Return memory error if creating the new node fails
        
        // Initialize the new root node with key and pointers
        NewRoot->keys[0] = key;
        NewRoot->KeCounts = 1;
        NewRoot->ptr[0] = left;
        NewRoot->ptr[1] = right;
        
        // Update parent pointers of child nodes
        left->pptr = NewRoot;
        right->pptr = NewRoot;
        
        root = NewRoot; // Set the new root as the global root
        numLevels++; // Increment the number of levels in the B-tree
        
        return RC_OK; // Return success code
    }
    
    // Find the index of the left child node in the parent node
    while (index < pptr->KeCounts && pptr->ptr[index] != left)
        index++;
    
    // Check if there's enough space in the parent node to insert the new key and pointer
    if (pptr->KeCounts < sizeofNod - 1) {
        // Shift keys and pointers to the right to make space for the new key and pointer
        i = pptr->KeCounts;
        while (i > index) {
            pptr->keys[i] = pptr->keys[i - 1];
            pptr->ptr[i + 1] = pptr->ptr[i];
            i--;
        }
        // Insert the new key and pointer
        pptr->keys[index] = key;
        pptr->ptr[index + 1] = right;
        pptr->KeCounts++; // Increment the number of keys in the parent node
        return RC_OK; // Return success code
    }
    
    // If there's not enough space in the parent node, split it into two nodes
    RM_BtreeNode **tempNode = malloc((sizeofNod + 1) * sizeof(RM_BtreeNode *));
    Value *tempKeys = malloc(sizeofNod * sizeof(Value));
    
    // Check if memory allocation for temporary arrays is successful
    if (tempNode == NULL || tempKeys == NULL) {
        // Return memory error if allocation fails
        free(tempNode);
        free(tempKeys);
        return RC_MEM_ERROR;
    }
    
    // Copy keys and pointers into temporary arrays, inserting the new key and pointer
    // at the appropriate position
    i = 0;
    while (i < sizeofNod + 1) {
        if (i < index + 1)
            tempNode[i] = pptr->ptr[i];
        else if (i == index + 1)
            tempNode[i] = right;
        else
            tempNode[i] = pptr->ptr[i - 1];
        i++;
    }
    
    i = 0;
    while (i < sizeofNod) {
        if (i < index)
            tempKeys[i] = pptr->keys[i];
        else if (i == index)
            tempKeys[i] = key;
        else
            tempKeys[i] = pptr->keys[i - 1];
        i++;
    }
    
    // Calculate the middle location for splitting
    int middleLoc = (sizeofNod - 1) >> 1;
    middleLoc += (sizeofNod & 1);
    
    // Update the number of keys in the original parent node
    pptr->KeCounts = middleLoc - 1;
    
    // Copy keys and pointers from temporary arrays to the original parent node
    i = 0;
    while (i < middleLoc - 1) {
        pptr->ptr[i] = tempNode[i];
        pptr->keys[i] = tempKeys[i];
        i++;
    }
    pptr->ptr[i] = tempNode[i];
    
    // Create a new node for the second half of the split
    RM_BtreeNode *newNode = createNewNod(newNode);
    if (newNode == NULL) {
        // Return memory error if creating the new node fails
        free(tempNode);
        free(tempKeys);
        return RC_MEM_ERROR;
    }
    
    // Initialize the new node with keys and pointers from the temporary arrays
    newNode->KeCounts = sizeofNod - middleLoc;
    i = middleLoc;
    while (i <= sizeofNod) {
        newNode->ptr[i - middleLoc] = tempNode[i];
        newNode->keys[i - middleLoc] = tempKeys[i];
        i++;
    }
    
    // Update the parent pointer of the new node
    newNode->pptr = pptr->pptr;
    
    free(tempKeys); // Free memory allocated for temporary keys array
    free(tempNode); // Free memory allocated for temporary pointers array
    
    // Recursively insert the new parent node
    return insertParent(pptr, newNode, tempKeys[middleLoc - 1]);
}

RC shutdownIndexManager() {
    
    return RC_OK;
}

// Function to create a B-tree index file
RC createBtree(char *idxId, DataType keyType, int n) {
    SM_FileHandle fhandle; // Declare a file handle
    SM_PageHandle pageData; // Declare a page data handle

    // Check if the index ID is not NULL
    if (idxId != NULL) {
        RC rc; // Declare a variable to store return codes
        
        // Check if creating or opening the page file is successful
        if ((rc = createPageFile(idxId)) != RC_OK || (rc = openPageFile(idxId, &fhandle)) != RC_OK) {
            return rc; // If not, return the corresponding error code
        }
        
        // Allocate memory for page data
        pageData = (SM_PageHandle)malloc(sizeof(PAGE_SIZE));
        
        // Write the key type and n to the first page
        *((int *)pageData) = keyType;
        *((int *)(pageData + sizeof(int))) = n;
        
        // Write the first page to the file
        rc = writeCurrentBlock(&fhandle, pageData);
        
        // Check if writing the first page is successful
        if ((rc = writeCurrentBlock(&fhandle, pageData)) != RC_OK || (rc = closePageFile(&fhandle)) != RC_OK)
            return rc; // If not, return the corresponding error code
        
        // Free memory allocated for page data
        free(pageData);
        pageData = NULL;
        
        // Return success code
        return RC_OK;
    } else {
        // If index ID is NULL, return the appropriate error code
        return RC_IM_KEY_NOT_FOUND;
    }
}

// Function to open a B-tree index
RC openBtree(BTreeHandle **tree, char *idxId) {
    int type, n; // Declare variables to store type and n
    BM_PageHandle *page; // Declare a pointer to a BM_PageHandle struct
    BM_BufferPool *bm; // Declare a pointer to a BM_BufferPool struct
    RC rc; // Declare a variable to store return codes

    page = MAKE_PAGE_HANDLE(); // Allocate memory for a BM_PageHandle struct
    bm = MAKE_POOL(); // Allocate memory for a BM_BufferPool struct
    *tree = malloc(sizeof(BTreeHandle)); // Allocate memory for a BTreeHandle pointer

    // Initialize the buffer pool with specified parameters
    rc = initBufferPool(bm, idxId, 10, RS_FIFO, NULL);

    // Check if buffer pool initialization was successful
    if (rc == RC_OK) {
        // Pin the first page of the buffer pool
        rc = pinPage(bm, page, 0);

        // Check if pinning the page was successful
        if (rc == RC_OK) {
            // Retrieve the type from the page data
            memcpy(&type, page->data, sizeof(int));
            
            // Set the keyType of the B-tree handle to the retrieved type
            (*tree)->keyType = (DataType)type;

            // Move the page data pointer to read n
            page->data += sizeof(int);
            
            // Read n from the page data
            n = *((int*)page->data);

            // Move the page data pointer back to its original position
            page->data -= sizeof(int);

            // Allocate memory for B-tree management data
            RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)malloc(sizeof(RM_bTree_mgmtData));

            // Check if memory allocation was successful
            if (bTreeMgmt == NULL) {
                // If memory allocation failed, return the appropriate error code
                return RC_RM_NO_MORE_TUPLES; 
            }

            // Set the buffer pool of the B-tree management data
            bTreeMgmt->bp = bm;

            // Set the management data of the B-tree handle to the allocated B-tree management data
            (*tree)->mgmtData = bTreeMgmt;

            // Initialize the number of entries in the B-tree management data
            bTreeMgmt->num = 0;

            // Set the maximum number of keys in a node in the B-tree management data
            bTreeMgmt->maxKeNum = n;

            // Free memory allocated for the page
            free(page);
            page = NULL;

            // Return success code
            return RC_OK;
        } else {
            // If pinning the page failed, return the error code
            return rc;
        }
    } else {
        // If buffer pool initialization failed
        if (idxId == NULL) {
            // If idxId is NULL, return the appropriate error code
            return RC_IM_KEY_NOT_FOUND;
        } else {
            // If idxId is not NULL, return the error code returned by initBufferPool
            return rc;
        }
    }
}

// Function to close a B-tree index
RC closeBtree(BTreeHandle *tree) {
    // Check if the tree handle is not NULL
    if (tree != NULL) {
        // Retrieve the management data of the B-tree
        RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
        
        // Shut down the buffer pool associated with the B-tree
        RC rc = shutdownBufferPool(bTreeMgmt->bp);
        
        // Check if shutting down the buffer pool was successful
        if (rc != RC_OK) {
            return rc; // If not, return the corresponding error code
        }
        
        // Free the memory allocated for the B-tree management data
        free(bTreeMgmt);
        bTreeMgmt = NULL;
        
        // Set the index ID of the B-tree handle to NULL
        tree->idxId = NULL;
        
        // Free the memory allocated for the B-tree handle
        free(tree);
        tree = NULL;
        
        // Free the memory allocated for the root node of the B-tree
        free(root);
        root = NULL;
        
        // Return success code
        return RC_OK;
    } else {
        // Error handling for NULL input
        return RC_IM_KEY_NOT_FOUND;
    }
}

// Function to delete a B-tree index and its associated page file
RC deleteBtree(char *idxId) {
    // Check if the index ID is NULL
    return (idxId == NULL) ? 
        RC_IM_KEY_NOT_FOUND : // If index ID is NULL, return the appropriate error code
        (destroyPageFile(idxId) != RC_OK) ? 
            RC_IM_KEY_NOT_FOUND : // If destroying page file fails, return the appropriate error code
            RC_OK; // Otherwise, return success code
}

// Function to get the number of entries in the B-tree
RC getNumEntries(BTreeHandle *tree, int *result) {
    // Check if the tree handle or result pointer is NULL
    return (!tree || !result) ? 
        RC_IM_KEY_NOT_FOUND : // If either is NULL, return the appropriate error code
        (*result = ((RM_bTree_mgmtData *)tree->mgmtData)->num, RC_OK); // Otherwise, set the result to the number of entries and return success code
}

// Function to get the number of nodes in the B-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
    // Check if the tree handle or result pointer is NULL
    return (!tree || !result) ? 
        RC_IM_KEY_NOT_FOUND : // If either is NULL, return the appropriate error code
        (*result = numNodValue, RC_OK); // Otherwise, set the result to the number of nodes and return success code
}

// Function to get the key type of the B-tree
RC getKeyType(BTreeHandle *tree, DataType *result) {
    // Check if the tree handle or result pointer is NULL
    return (!tree || !result) ? 
        RC_IM_KEY_NOT_FOUND : // If either is NULL, return the appropriate error code
        (*result = tree->keyType, RC_OK); // Otherwise, set the result to the key type and return success code
}

// Function to find a key in the B-tree and retrieve its corresponding RID
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    // Check if the tree handle, key, or root is NULL
    if (!tree || !key || !root)
        return RC_IM_KEY_NOT_FOUND; // If any of them is NULL, return the appropriate error code
    
    // Initialize a pointer to the root node
    RM_BtreeNode *leaf = root;
    
    // Traverse the B-tree until reaching a leaf node
    while (!leaf->isLeaf) {
        // Initialize index variable
        int i = 0;
        
        // Serialize the key at the current index of the leaf node
        char *s = serializeValue(&leaf->keys[i]);
        
        // Serialize the search key
        char *s_s = serializeValue(key);
        
        // Iterate through the keys of the leaf node until finding the appropriate index
        while (i < leaf->KeCounts && strNoLarger(s, s_s)) {
            // Free the serialized key
            free(s);
            s = NULL;
            
            // Increment index
            i++;
            
            // Check if not at the last key of the leaf node
            if (i < leaf->KeCounts)
                // Serialize the next key
                s = serializeValue(&leaf->keys[i]);
        }
        
        // Free the serialized key
        free(s);
        s = NULL;
        
        // Free the serialized search key
        free(s_s);
        s_s = NULL;
        
        // Traverse to the next child node
        leaf = (RM_BtreeNode *)leaf->ptr[i];
    }
    
    // Reset index variable
    int i = 0;
    
    // Serialize the key at the current index of the leaf node
    char *s = serializeValue(&leaf->keys[i]);
    
    // Serialize the search key
    char *s_s = serializeValue(key);
    
    // Iterate through the keys of the leaf node until finding the key or reaching the end
    while (i < leaf->KeCounts && strcmp(s, s_s) != 0) {
        // Free the serialized key
        free(s);
        s = NULL;
        
        // Increment index
        i++;
        
        // Check if not at the last key of the leaf node
        if (i < leaf->KeCounts)
            // Serialize the next key
            s = serializeValue(&leaf->keys[i]);
    }
    
    // Free the serialized key
    free(s);
    s = NULL;
    
    // Free the serialized search key
    free(s_s);
    s_s = NULL;
    
    // If the key is not found, return appropriate error code
    if (i >= leaf->KeCounts)
        return RC_IM_KEY_NOT_FOUND;
    
    // If the key is found, retrieve its corresponding RID
    result->page = ((RID *)leaf->ptr[i])->page;
    result->slot = ((RID *)leaf->ptr[i])->slot;
    
    // Return success code
    return RC_OK;
}

// Function to insert a key and RID into the B-tree
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    // Check if the tree handle or the key is NULL
    return (!tree || !key) ? 
        RC_IM_KEY_NOT_FOUND : // If either is NULL, return the appropriate error code
        insertKeyHelper(tree, key, rid); // Otherwise, call the insertKeyHelper function
}

// Helper function to insert a key and RID into the B-tree
RC insertKeyHelper(BTreeHandle *tree, Value *key, RID rid) {
    // Initialize a pointer to the root node
    RM_BtreeNode *leaf = root;
    
    // Initialize index variable
    int i = 0;
    
    // Traverse the tree until reaching a leaf node
    while (leaf && !leaf->isLeaf) {
        // Serialize the key at the current index of the leaf node
        char *s = serializeValue(&leaf->keys[i]);
        
        // Serialize the new key
        char *s_s = serializeValue(key);
        
        // Iterate through the keys of the leaf node until finding the appropriate index for insertion
        for (; i < leaf->KeCounts && strNoLarger(s, s_s); i++) {
            // Free the serialized key
            free(s);
            s = NULL;
            
            // Check if not at the last key of the leaf node
            if (i < leaf->KeCounts - 1)
                // Serialize the next key
                s = serializeValue(&leaf->keys[i + 1]);
        }
        
        // Free the serialized key
        free(s);
        s = NULL;
        
        // Free the serialized new key
        free(s_s);
        s_s = NULL;
        
        // Traverse to the next child node
        leaf = (RM_BtreeNode *)leaf->ptr[i];
        
        // Reset index for the next level
        i = 0;
    }
    
    // Retrieve the management data of the B-tree
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    
    // Increment the total number of keys in the B-tree
    bTreeMgmt->num++;
    
    // Check if a leaf node is found
    return leaf ? 
        // If a leaf node is found, insert the key and RID into the leaf node
        insertLeaf(leaf, key, rid) : 
        // If no leaf node is found, insert the key and RID as a new root
        insertNewRoot(bTreeMgmt, key, rid);
}

// Function to insert a key and RID into a leaf node of the B-tree
RC insertLeaf(RM_BtreeNode *leaf, Value *key, RID rid) {
    // Initialize index variable
    int index = 0;
    
    // Serialize the key at the first index of the leaf node
    char *s = serializeValue(&leaf->keys[index]);
    
    // Serialize the new key
    char *s_s = serializeValue(key);
    
    // Iterate through the keys of the leaf node until finding the appropriate index for insertion
    for (; index < (*leaf).KeCounts && strNoLarger(s, s_s); index++) {
        // Free the serialized key
        free(s);
        s = NULL;
        
        // Check if not at the last key of the leaf node
        if (index < (*leaf).KeCounts - 1) 
            // Serialize the next key
            s = serializeValue(&leaf->keys[index + 1]);
    }
    
    // Free the serialized key
    free(s);
    s = NULL;
    
    // Free the serialized new key
    free(s_s);
    s_s = NULL;
    
    // Check if the leaf node is full
    return (*leaf).KeCounts >= sizeofNod - 1 ? 
        // If full, split the leaf node and insert the key and RID
        splitLeaf(leaf, key, rid, index) : 
        // If not full, insert the key and RID directly into the leaf node
        insertInLeaf(leaf, key, rid, index);
}

// Function to split a leaf node of the B-tree
RC splitLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index) {
    // Allocate memory for an array to store RID pointers
    uintptr_t *NodeRID = malloc(sizeofNod * sizeof(uintptr_t));
    
    // Initialize variables
    int middleLoc = 0, key_cnt = sizeofNod;
    
    // Allocate memory for an array to store keys
    Value *NodeKeys = malloc(key_cnt * sizeof(Value));

    // Loop through the keys of the leaf node
    for (int i = 0; i < key_cnt; i++) {
        // Check if the current index is less than the insertion index
        if (i < index) {
            // Store RID and key values before the insertion index
            NodeRID[i] = (uintptr_t)leaf->ptr[i];
            globalPos = ((RID *)NodeRID[i])->page;
            NodeKeys[i] = leaf->keys[i];
            // Set the middle location flag
            middleLoc = sizeofNod % 2 == 0;
        } 
        // Check if the current index is equal to the insertion index
        else if (i == index) {
            // Allocate memory for a new RID structure and set its values
            RID *newValue = (RID *)malloc(sizeof(RID));
            newValue->page = rid.page;
            newValue->slot = rid.slot;
            NodeRID[i] = (uintptr_t)newValue;
            NodeKeys[i] = *key;
        } 
        // For indices after the insertion index
        else {
            // Store RID and key values after the insertion index
            middleLoc = globalPos;
            NodeRID[i] = (uintptr_t)leaf->ptr[i - 1];
            globalPos = ((RID *)NodeRID[i])->page;
            NodeKeys[i] = leaf->keys[i - 1];
        }
    }

    // Calculate the middle location for splitting
    middleLoc = sizeofNod / 2 + 1;

    // Copy keys and pointers to the original leaf node until the middle location
    for (int i = 0; i < middleLoc; i++) {
        (*leaf).ptr[i] = (RID *)NodeRID[i];
        (*leaf).keys[i] = NodeKeys[i];
    }

    // Create a new leaf node
    RM_BtreeNode *newLeafNod = createNewNod(newLeafNod);
    
    // Set the isLeaf flag of the new leaf node to true
    newLeafNod->isLeaf = true;
    
    // Copy the parent pointer from the original leaf node to the new leaf node
    newLeafNod->pptr = leaf->pptr;
    
    // Calculate and assign the KeCounts for the new leaf node
    newLeafNod->KeCounts = sizeofNod - middleLoc;

    // Copy keys and pointers to the new leaf node after the middle location
    for (int i = middleLoc; i < sizeofNod; i++) {
        (*newLeafNod).ptr[i - middleLoc] = (RID *)NodeRID[i];
        (*newLeafNod).keys[i - middleLoc] = NodeKeys[i];
    }

    // Copy pointer from the original leaf node to the last position of the new leaf node
    newLeafNod->ptr[sizeofNod - 1] = (void *)((uintptr_t)leaf->ptr[sizeofNod - 1]);
    
    // Update KeCounts of the original leaf node
    leaf->KeCounts = middleLoc;
    
    // Assign the pointer to the new leaf node at the last position of the original leaf node
    leaf->ptr[sizeofNod - 1] = (void *)((uintptr_t)newLeafNod);

    // Free allocated memory for arrays
    free(NodeRID);
    NodeRID = NULL;
    free(NodeKeys);
    NodeKeys = NULL;

    // Insert parent node for the new leaf node
    RC rc = insertParent(leaf, newLeafNod, newLeafNod->keys[0]);
    
    // Return appropriate status code
    return rc != RC_OK ? rc : RC_OK;
}

// Function to insert a new key and RID in a leaf node of the B-tree
RC insertInLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index) {
    // Iterate from the last key position to the insertion index
    for (int i = (*leaf).KeCounts; i > index; i--) {
        // Copy the key from the previous position to the current position
        leaf->keys[i] = leaf->keys[i - 1];

        // Store the global position from the leaf
        globalPos = (uintptr_t)leaf->pos;

        // Copy the pointer from the previous position to the current position
        leaf->ptr[i] = leaf->ptr[i - 1];
    }

    // Allocate memory for a new RID structure
    RID *rec = (RID *) malloc(sizeof(RID));
    
    // Copy values from 'rid' to the newly allocated RID
    rec->slot = rid.slot;
    rec->page = rid.page;

    // Store the key and RID pointer in the leaf node at the insertion index
    leaf->keys[index] = *key;
    leaf->ptr[index] = rec;
    
    // Increment KeCounts in the leaf node
    leaf->KeCounts += 1;
    
    // Return success code
    return RC_OK;
}


// Function to insert a new root into the B-tree
RC insertNewRoot(RM_bTree_mgmtData *bTreeMgmt, Value *key, RID rid) {
    // Set the size of nodes in the B-tree
    sizeofNod = (*bTreeMgmt).maxKeNum + 1;
    
    // Create a new root node
    root = createNewNod(root);
    
    // Allocate memory for a new RID structure and set its values
    RID *rec = (RID *) malloc(sizeof(RID));
    rec->page = rid.page;
    rec->slot = rid.slot;
    
    // Set the pointer to the new record in the root node
    root->ptr[0] = rec;
    
    // Set the key in the root node
    root->keys[0] = *key;
    
    // Set the last pointer in the root node to NULL
    root->ptr[sizeofNod - 1] = NULL;
    
    // Set the root node as a leaf node
    root->isLeaf = true;
    
    // Increment the number of keys in the root node
    root->KeCounts++;
    
    // Return success code
    return RC_OK;
}


// Function to delete a key from the B-tree
RC deleteKey(BTreeHandle *tree, Value *ke) {
    // Check if the tree handle or the key is NULL
    if (!tree || !ke)
        return RC_IM_KEY_NOT_FOUND; // Return error code if either is NULL
    
    // Retrieve the management data of the B-tree
    RM_bTree_mgmtData *bTreeMgmt = (RM_bTree_mgmtData *)tree->mgmtData;
    
    // Decrement the total number of keys in the B-tree
    bTreeMgmt->num--;
    
    // Pointer to navigate to the leaf node
    RM_BtreeNode *leaf = root;
    
    // Initialize index variable
    int i = 0;

    // Loop until leaf node is reached or leaf is not a leaf node
    while (leaf && !leaf->isLeaf) {
        // Iterate through keys of the leaf node
        while (i < leaf->KeCounts && strNoLarger(serializeValue(&leaf->keys[i]), serializeValue(ke)))
            free(serializeValue(&leaf->keys[i++]));
        
        // Navigate to the next child node
        leaf = (RM_BtreeNode *)leaf->ptr[i];
        
        // Reset index
        i = 0;
    }
    
    // Variable to control loop iteration
    int cnter = 1;
    
    // Variable to control loop termination condition
    char val = 'y';

    // Loop based on cnter value
    while (cnter--) {
        // Check if val is 'n'
        if (val == 'n')
            return RC_WRITE_FAILED; // Return write failed error code
        
        // Iterate through keys of the leaf node
        while (i < leaf->KeCounts && strcmp(serializeValue(&leaf->keys[i]), serializeValue(ke)) != 0)
            free(serializeValue(&leaf->keys[i++]));
    }

    // Check if index is within bounds and deletion of node is successful
    if (i < leaf->KeCounts && deleteNode(leaf, i) != RC_OK)
        return RC_OK; // Return success code

    // Update the management data of the tree handle
    tree->mgmtData = bTreeMgmt;
    
    // Return success code
    return RC_OK;
}

// Function to open a tree scan
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // Check if the tree handle is NULL
    if (!tree)
        return RC_IM_KEY_NOT_FOUND; // Return error code if tree handle is NULL
    
    // Allocate memory for the scan handle
    *handle = calloc(1, sizeof(BT_ScanHandle));
    
    // Set the tree handle for the scan handle
    (*handle)->tree = tree;
    
    // Allocate memory for the management data of the scan handle
    (*handle)->mgmtData = calloc(1, sizeof(RM_BScan_mgmt));
    
    // Return success code
    return RC_OK;
}


// Function to retrieve the next entry during a tree scan
RC nextEntry(BT_ScanHandle *handle, RID *result) {
    // Check if the handle is NULL
    if (!handle)
        return RC_IM_KEY_NOT_FOUND; // Return error code if handle is NULL
    
    // Retrieve the scan management data from the handle
    RM_BScan_mgmt *scanMgmt = (RM_BScan_mgmt *)handle->mgmtData;
    
    // Variable to store the total number of entries in the tree
    int totalRes;
    
    // Retrieve the total number of entries in the tree
    RC rc = getNumEntries(handle->tree, &totalRes);
    
    // Return error code if retrieval of total entries fails
    if (rc != RC_OK)
        return rc;
    
    // Check if all entries have been scanned
    switch (scanMgmt->tot_Scan >= totalRes) {
        case true:
            return RC_IM_NO_MORE_ENTRIES; // Return no more entries error code
        case false:
            break;
    }
    
    // Pointer to navigate to the leaf node
    RM_BtreeNode *leaf = root;
    
    // Switch case based on the number of entries scanned
    switch (scanMgmt->tot_Scan) {
        case 0:
            // Navigate to the first leaf node
            while (!leaf->isLeaf)
                leaf = leaf->ptr[0];
            // Set the current node for scanning
            scanMgmt->cur = leaf;
            break;
        default:
            break;
    }
    
    // Switch case based on the current index in the current node
    switch (scanMgmt->index == scanMgmt->cur->KeCounts) {
        case true:
            // Move to the next leaf node
            scanMgmt->cur = (RM_BtreeNode *)(scanMgmt->cur->ptr[((RM_bTree_mgmtData *)handle->tree->mgmtData)->maxKeNum]);
            scanMgmt->index = 0;
            break;
        case false:
            break;
    }
    
    // Retrieve the RID of the current entry
    RID *ridRes = (RID *)scanMgmt->cur->ptr[scanMgmt->index++];
    
    // Increment the total scanned entries
    scanMgmt->tot_Scan++;
    
    // Assign page and slot values to the result
    result->page = ridRes->page;
    result->slot = ridRes->slot;
    
    // Free the RID pointer
    free(ridRes);
    
    // Update the scan management data in the handle
    handle->mgmtData = scanMgmt;
    
    // Return success code
    return RC_OK;
}

// Function to print the structure of a B-tree
void printBTreeStructure(RM_BtreeNode *rootNode) {
    // Check if the root node is NULL
    if (rootNode == NULL) {
        return; // If so, return without printing anything
    }
    
    // Allocate memory for the queue to perform level-order traversal
    RM_BtreeNode **queue = (RM_BtreeNode **)malloc(sizeof(RM_BtreeNode *) * numNodValue);
    
    // Initialize front and rear indices of the queue
    int front = 0, rear = 0;
    
    // Enqueue the root node
    queue[rear++] = rootNode;
    
    // Print opening square bracket for the entire tree
    printf("[");
    
    // Initialize variables to track levels and nodes in each level
    int levelCount = 0;
    int prevLevelSize = 1;
    int currLevelSize = 0;
    
    // Loop until the queue is empty
    while (front != rear) {
        // Calculate the number of nodes in the current level
        int levelSize = rear - front;
        
        // Initialize string to store keys at current level
        char levelString[MAX_LINE_LENGTH] = "";
        
        // Iterate through each node in the current level
        for (int i = 0; i < levelSize; i++) {
            // Dequeue the current node
            RM_BtreeNode *currentNode = queue[front++];
            
            // Serialize and concatenate keys of the current node to the levelString
            for (int j = 0; j < currentNode->KeCounts; j++) {
                char *serializedKey = serializeValue(&currentNode->keys[j]);
                strcat(levelString, serializedKey);
                free(serializedKey);
                if (j < currentNode->KeCounts - 1) {
                    strcat(levelString, ", ");
                }
            }
            
            // Enqueue children if current node is not a leaf
            if (!currentNode->isLeaf) {
                for (int j = 0; j < currentNode->KeCounts + 1; j++) {
                    queue[rear++] = (RM_BtreeNode *)currentNode->ptr[j];
                    currLevelSize++;
                }
            }
            
            // Append closing bracket if not the last node in the level
            if (i < levelSize - 1) {
                strcat(levelString, "] [");
            } else {
                strcat(levelString, "]");
            }
        }
        
        // Print new line and spaces to represent the structure of the tree
        if (levelCount > 0) {
            printf("\n");
            for (int i = 0; i < levelCount; i++) {
                printf(" ");
            }
            printf("/ ");
            for (int i = 0; i < prevLevelSize - 1; i++) {
                printf(" \\ ");
            }
        }
        
        // Print keys of the current level
        printf("%s", levelString);
        
        // Update variables for next level
        prevLevelSize = currLevelSize;
        currLevelSize = 0;
        levelCount++;
    }
    
    // Print new line and free the memory allocated for the queue
    printf("\n");
    free(queue);
}


// Function to close a tree scan and print the tree structure
RC closeTreeScan(BT_ScanHandle *handle) {
    RM_BScan_mgmt *mgmt;   

    // Check if the handle is NULL
    if (handle == NULL) {
        return RC_INVALID_HANDLE; // Return error code if handle is NULL
    }
    
    // Retrieve the management data from the handle
    mgmt = handle->mgmtData;

    // Free the memory allocated for mgmt
    if (mgmt != NULL) {
        free(mgmt);
        mgmt = NULL;
    }
    
    // Free the memory allocated for handle
    free(handle);
    handle = NULL;
    
    // Allocate memory for the result string
    char *result = (char *)malloc((numNodValue * MAX_LINE_LENGTH + 1) * sizeof(char));
    
    // Check if memory allocation was successful
    if (result == NULL) {
        return RC_OUT_OF_MEMORY; // Return error code if memory allocation failed
    }
    
    // Initialize the result string
    result[0] = '\0';
    
    // Traverse the tree and store the result in the result string
    walk(root, result);
    
    // Print the result
    printf("%s\n", result);
    printf("Total number of nodes: %d\n", numNodValue);
    printf("Number of levels: %d\n", numLevels);
    
    // Free the memory allocated for result
    free(result);
    
    // Return success code
    return RC_OK;
}


// Function to walk through the B-tree and generate a string representation
int walk(RM_BtreeNode *bTreeNode, char *result) {
    // Allocate memory for a line to represent the current node
    char *line = malloc(MAX_LINE_LENGTH);
    
    // Allocate memory for a temporary string
    char *t = malloc(MAX_T_LENGTH);
    
    // Initialize index variable
    int i = 0;
    
    // Format the line to include the position of the current node
    sprintf(line, "(%d)[", bTreeNode->pos);

    // Check if the current node is a leaf node
    if (bTreeNode->isLeaf) {
        // Iterate through the keys and pointers of the leaf node
        for (i = 0; i < bTreeNode->KeCounts; i++) {
            // Format the RID and concatenate it to the line
            sprintf(t, "%d.%d,", ((RID *)bTreeNode->ptr[i])->page, ((RID *)bTreeNode->ptr[i])->slot);
            strcat(line, t);
            
            // Serialize the key value and concatenate it to the line
            char *s = serializeValue(&bTreeNode->keys[i]);
            strcat(line, s);
            free(s);
            strcat(line, ",");
        }
        
        // Handle the last pointer separately
        if (bTreeNode->ptr[sizeofNod - 1] != NULL) {
            sprintf(t, "%d", ((RM_BtreeNode *)bTreeNode->ptr[sizeofNod - 1])->pos);
            strcat(line, t);
        }
        
        // Adjust line length and terminate with newline character
        line[(bTreeNode->ptr[sizeofNod - 1] != NULL) ? strlen(line) : strlen(line) - 1] = '\0';
        strcat(line, "]\n");
    } else {
        // For internal nodes, iterate through keys and pointers
        for (i = 0; i < bTreeNode->KeCounts; i++) {
            // Format pointer position and concatenate it to the line
            sprintf(t, "%d,", ((RM_BtreeNode *)bTreeNode->ptr[i])->pos);
            strcat(line, t);
            
            // Serialize the key value and concatenate it to the line
            char *s = serializeValue(&bTreeNode->keys[i]);
            strcat(line, s);
            free(s);
            strcat(line, ",");
        }
        
        // Additional logic block
        int time_record = 10;
        int scanned = 3;
        ((scanned == 1) && (time_record > 5)) ? printf("Can't write the block! ") : 0;
        
        // Retrieve the current node pointer
        RM_BtreeNode *currentNode = (RM_BtreeNode *)bTreeNode->ptr[i];
        
        // Check conditions for scanning and node existence
        bool isScannedAllowed = (scanned == 1 || scanned == 3);
        bool isNodeNotNull = (currentNode != NULL);
        
        // Handle scanning conditions
        if (isScannedAllowed && isNodeNotNull) {
            RM_BtreeNode *currentNode = (RM_BtreeNode *)bTreeNode->ptr[i];
            int nodePosition = currentNode != NULL ? currentNode->pos : -1; 
            char positionString[20];
            sprintf(positionString, "%d", nodePosition);
            strcat(line, t);
        } else {
            // If scanning conditions not met, adjust the line
            char *token = strtok(line, "\n");
            if (token != NULL)
                 strcpy(line, token);
        }
        strcat(line, "]\n");
    }
    
    // Concatenate the line to the result string
    strcat(result, line);
    
    // Free allocated memory
    free(line);
    free(t);
    
    // If the current node is not a leaf node, recursively walk through its children
    if (!bTreeNode->isLeaf) {
        for (i = 0; i < bTreeNode->KeCounts; i++) {
            walk(bTreeNode->ptr[i], result);
        }
    }
    
    // Return 0 indicating successful completion
    return 0;
}

// Depth First Search (DFS) function to traverse the B-tree and assign positions to nodes
int DFS(RM_BtreeNode *root) {
    // Check if the current node is NULL
    if (root == NULL)
        return 0; // If so, return 0
    
    // Assign a position value to the current node and increment the global position counter
    root->pos = globalPos++;
    
    // Check if the current node is not a leaf node
    if (!root->isLeaf) {
        // Traverse each child pointer of the current node
        for (int i = 0; i < root->KeCounts + 1; i++) {
            // Recursively call DFS for each child node
            DFS((RM_BtreeNode *)root->ptr[i]);
        }
    }
    
    // Return the updated global position value
    return globalPos;
}

// Function to print the contents of a B-tree
char *printTree(BTreeHandle *tree) {
    // Check if the tree handle is valid and if it contains data
    if (tree == NULL || tree->mgmtData == NULL)
        return NULL; // If not, return NULL
    // Retrieve the root node of the B-tree
    RM_BtreeNode *root = (RM_BtreeNode *)tree->mgmtData;
    // Calculate the length of the output string by performing a depth-first search (DFS) traversal of the tree
    int length = DFS(root);
    // Allocate memory for the output string based on the calculated length
    char *result = (char *)malloc((length + 1) * sizeof(char));
    // Reset the global position variable used during traversal
    globalPos = 0;
    // Traverse the tree and populate the output string
    walk(root, result);
    // Return the pointer to the output string
    return result;
}

