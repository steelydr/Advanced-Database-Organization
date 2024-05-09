Team Members Contribution Depala Rajeswari 36% Prishitha Lokareddy 34% Charan Reddy 30%
In btree_mgr.c file functions as below:-

1. `strNoLarger(char *c, char *c1)`:
   - Compares two strings lexicographically.
   - Returns true if the first string is lexicographically smaller than or equal to the second string.
   - Checks the lengths of the strings and compares characters one by one.
   - Used for comparing keys in the B-tree.

2. `createNewNod(RM_BtreeNode *thisNode)`:
   - Creates a new node for the B-tree.
   - Allocates memory for the node and its key and pointer arrays.
   - Initializes the node's fields with default values.
   - Returns the newly created node or NULL if memory allocation fails.

3. `deleteNode(RM_BtreeNode *bTreeNode, int index)`:
   - Deletes a key from a node in the B-tree.
   - Handles different cases for leaf and non-leaf nodes.
   - Calls helper functions for merging nodes and deleting leaf/non-leaf nodes.
   - Updates the root node if necessary.

4. `mergeNodes(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int position, int NumKeys, int *brotherNumKeys)`:
   - Merges two sibling nodes in the B-tree.
   - Copies keys and pointers from the sibling node to the current node.
   - Updates the key in the parent node.
   - Handles leaf and non-leaf node cases.

5. `deleteLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys)`:
   - Deletes a key from a leaf node in the B-tree.
   - Shifts keys and pointers to the left after the deleted key.
   - Frees the memory allocated for the pointer at the deleted index.

6. `deleteNonLeafNode(RM_BtreeNode *bTreeNode, int index, int NumKeys)`:
   - Deletes a key from a non-leaf node in the B-tree.
   - Shifts keys and pointers to the left after the deleted key.
   - Handles the case when the key to be deleted is not present in the node.

7. `mergeNodesAtZeroPosition(RM_BtreeNode *bTreeNode, RM_BtreeNode *brother, RM_BtreeNode *parentNode, int NumKeys)`:
   - Merges two sibling nodes in the B-tree when the left sibling has only one key.
   - Copies the key and pointer from the right sibling to the left sibling.
   - Updates the key in the parent node.
   - Handles leaf and non-leaf node cases.

8. `initIndexManager(void *mgmtData)`:
   - Initializes the index manager.
   - Sets the root of the B-tree to NULL.
   - Initializes various counters and values.

9. `shutdownIndexManager()`:
   - Shuts down the index manager.
   - Currently, it does not perform any specific action.

10. `createBtree(char *idxId, DataType keyType, int n)`:
    - Creates a new B-tree index file.
    - Opens or creates a new page file for the index.
    - Writes the key type and n (maximum number of keys per node) to the first page of the file.

11. `openBtree(BTreeHandle **tree, char *idxId)`:
    - Opens an existing B-tree index.
    - Initializes a buffer pool and associates it with the B-tree handle.
    - Retrieves the key type and maximum number of keys per node from the first page of the file.
    - Allocates memory for the B-tree management data and associates it with the tree handle.

12. `closeBtree(BTreeHandle *tree)`:
    - Closes a B-tree index.
    - Shuts down the buffer pool associated with the B-tree.
    - Frees the memory allocated for the B-tree management data and the root node.

13. `deleteBtree(char *idxId)`:
    - Deletes a B-tree index and its associated page file.
    - Checks if the index ID is valid and destroys the corresponding page file.

14. `getNumEntries(BTreeHandle *tree, int *result)`:
    - Retrieves the total number of entries (keys) in the B-tree.
    - Stores the result in the provided `result` pointer.

15. `getNumNodes(BTreeHandle *tree, int *result)`:
    - Retrieves the total number of nodes in the B-tree.
    - Stores the result in the provided `result` pointer.

16. `getKeyType(BTreeHandle *tree, DataType *result)`:
    - Retrieves the data type of the keys in the B-tree.
    - Stores the result in the provided `result` pointer.

17. `findKey(BTreeHandle *tree, Value *key, RID *result)`:
    - Finds a key in the B-tree and retrieves its corresponding record ID (RID).
    - Traverses the B-tree from the root to the leaf node containing the key.
    - Stores the RID in the provided `result` pointer if the key is found.

18. `insertKey(BTreeHandle *tree, Value *key, RID rid)`:
    - Inserts a new key and its associated record ID (RID) into the B-tree.
    - Calls the `insertKeyHelper` function to perform the insertion.

19. `insertKeyHelper(BTreeHandle *tree, Value *key, RID rid)`:
    - Helper function for inserting a new key and RID into the B-tree.
    - Traverses the B-tree from the root to the appropriate leaf node for insertion.
    - Handles the case when the root node is empty and creates a new root.
    - Calls the `insertLeaf` function to insert the key and RID into the leaf node.

20. `insertLeaf(RM_BtreeNode *leaf, Value *key, RID rid)`:
    - Inserts a new key and RID into a leaf node of the B-tree.
    - Finds the appropriate index for insertion based on the existing keys.
    - Handles the case when the leaf node is full by calling the `splitLeaf` function.
    - Otherwise, inserts the key and RID directly into the leaf node.

21. `splitLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index)`:
    - Splits a leaf node of the B-tree when it is full.
    - Creates a new leaf node and distributes the keys and RIDs between the two nodes.
    - Updates the pointers and keys in the parent node to accommodate the new leaf node.
    - Calls the `insertParent` function to insert a new parent node if necessary.

22. `insertInLeaf(RM_BtreeNode *leaf, Value *key, RID rid, int index)`:
    - Inserts a new key and RID into a leaf node of the B-tree.
    - Shifts the existing keys and pointers to make space for the new entry.
    - Allocates memory for a new RID structure and sets its values.
    - Updates the key count of the leaf node.

23. `insertNewRoot(RM_bTree_mgmtData *bTreeMgmt, Value *key, RID rid)`:
    - Inserts a new root node into the B-tree.
    - Sets the size of nodes based on the maximum number of keys allowed per node.
    - Creates a new root node and initializes it with the given key and RID.
    - Sets the root node as a leaf node.

24. `deleteKey(BTreeHandle *tree, Value *ke)`:
    - Deletes a key from the B-tree.
    - Traverses the B-tree from the root to the leaf node containing the key.
    - Calls the `deleteNode` function to perform the deletion.
    - Updates the management data of the B-tree handle.

25. `openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)`:
    - Opens a new tree scan for the B-tree.
    - Allocates memory for the scan handle and its management data.
    - Associates the B-tree handle with the scan handle.

26. `nextEntry(BT_ScanHandle *handle, RID *result)`:
    - Retrieves the next entry during a tree scan.
    - Handles the case when all entries have been scanned.
    - Traverses the B-tree from the root to the appropriate leaf node.
    - Retrieves the RID of the current entry and stores it in the provided `result` pointer.
    - Updates the scan management data in the handle.

27. `printBTreeStructure(RM_BtreeNode *rootNode)`:
    - Prints the structure of the B-tree in a level-order traversal format.
    - Performs a level-order traversal using a queue.
    - Serializes the keys in each node and prints them with appropriate spacing.
    - Handles leaf and non-leaf nodes differently.

28. `closeTreeScan(BT_ScanHandle *handle)`:
    - Closes a tree scan.
    - Frees the memory allocated for the scan handle and its management data.
    - Calls the `walk` function to generate a string representation of the B-tree.
    - Prints the string representation and the total number of nodes and levels in the B-tree.

29. `walk(RM_BtreeNode *bTreeNode, char *result)`:
    - Traverses the B-tree in a depth-first manner and generates a string representation.
    - Handles leaf and non-leaf nodes differently.
    - Serializes the keys and RIDs in leaf nodes.
    - Concatenates the string representation of each node to the `result` string.

30. `DFS(RM_BtreeNode *root)`:
    - Performs a depth-first search (DFS) traversal of the B-tree.
    - Assigns position values to each node during the traversal.
    - Used for calculating the length of the output string in the `printTree` function.

31. `printTree(BTreeHandle *tree)`:
    - Prints the contents of the B-tree.
    - Performs a depth-first search (DFS) traversal to calculate the length of the output string.
    - Allocates memory for the output string based on the calculated length.
    - Calls the `walk` function to populate the output string.
    - Returns a pointer to the output string.


In record_mgr.c we have modified and added thread safe operations :-

1. `markSystemCatalogDirty()`:
   - This function marks the catalog page as dirty in the buffer pool.
   - It does not require thread safety as it only operates on a single buffer pool handle.

2. `getSystemCatalog()`:
   - This function returns the system catalog stored in the catalog page handle.
   - It does not require thread safety as it only retrieves data from a single buffer pool handle.

3. `getPageHeader(BM_PageHandle* handle)`:
   - This function returns the page header from the page handle.
   - It does not require thread safety as it only operates on a single buffer pool handle.

4. `getTableByName(char* name)`:
   - This function retrieves the TableMetadata for a given table name.
   - It is thread-safe because it locks the `table_mutex` to ensure exclusive access while searching for the table in the system catalog.

5. `getTupleData(BM_PageHandle* handle)`:
   - This function retrieves the tuple data area from the page handle.
   - It is thread-safe because it locks the `tuple_mutex` to ensure exclusive access while getting the page header and slot array.

6. `getSlots(BM_PageHandle* handle)`:
   - This function retrieves the slot array from the page handle.
   - It is thread-safe because it locks the `slots_mutex` to ensure exclusive access while retrieving the slot array.

7. `getTableMetadata(RM_TableData* rel)`:
   - This function retrieves the TableMetadata from the RM_TableData structure.
   - It is thread-safe because it locks the `metadata_mutex` to ensure exclusive access while retrieving the TableMetadata.

8. `setFreePage(BM_PageHandle* handle)`:
   - This function is not implemented and is intended for handling overflow pages.
   - It does not require thread safety as it is not currently being used.

9. `getTupleDataAt(BM_PageHandle* handle, int recordSize, int index)`:
   - This function retrieves the pointer to the tuple data at a given index.
   - It is thread-safe because it locks the `tuple_data_mutex` to ensure exclusive access while getting the tuple data area.

10. `getFreePageNumber()`:
    - This function retrieves the next free page number from the system catalog.
    - It is thread-safe because it locks the `free_page_mutex` to ensure exclusive access while updating the free page list and the system catalog.

11. `closeSlotWalk(TableMetadata *table, BM_PageHandle **handle)`:
    - This function closes the slot walk by unpinning the page handle if necessary.
    - It does not require thread safety as it operates on a single page handle and does not access shared resources.

12. `getNextPage(TableMetadata *table, int pageNumber)`:
    - This function retrieves the next page number for a given page.
    - It is thread-safe because it locks the `next_page_mutex` to ensure exclusive access while retrieving the next page number.

13. `appendToFreeList(int pageNumber)`:
    - This function appends a page to the free page list in the system catalog.
    - It is thread-safe because it locks the `append_mutex` to ensure exclusive access while modifying the free page list and updating the system catalog.

14. `getNextSlotInWalk(TableMetadata *table, BM_PageHandle **handle, bool** slots, int *slotIndex)`:
    - This function retrieves the next available slot during a slot walk.
    - It does not require thread safety as it operates on local variables and does not access shared resources.

15. `initRecordManager(void *mgmtData)`:
    - This function initializes the record manager.
    - It is thread-safe because it locks the `init_mutex` to ensure exclusive access during initialization.

16. `validateSystemConfig()`:
    - This function validates the system configuration.
    - It does not require thread safety as it only performs validation checks on constants.

17. `handleResult(const char *message, RC result, const char *fileName)`:
    - This function handles and prints error messages.
    - It does not require thread safety as it only performs output operations.

18. `shutdownRecordManager()`:
    - This function shuts down the record manager.
    - It does not require thread safety as it only operates on a single buffer pool handle and does not access shared resources.

19. `createTable(char *name, Schema *schema)`:
    - This function creates a new table in the system catalog.
    - It is thread-safe because it locks the `create_table_mutex` to ensure exclusive access while modifying the system catalog and allocating pages.

20. `openTable(RM_TableData *rel, char *name)`:
    - This function opens an existing table and initializes the RM_TableData structure.
    - It is thread-safe because it locks the `open_table_mutex` to ensure exclusive access while retrieving the TableMetadata and allocating memory for the RM_TableData structure.

21. `closeTable(RM_TableData *rel)`:
    - This function closes an open table and frees associated memory.
    - It is thread-safe because it locks the `close_table_mutex` to ensure exclusive access while unpinning the page handle and freeing memory.

22. `deleteTable(char *name)`:
    - This function deletes a table from the system catalog and frees its pages.
    - It is thread-safe because it locks the `delete_table_mutex` to ensure exclusive access while modifying the system catalog and appending pages to the free list.

23. `getNumTuples(RM_TableData *rel)`:
    - This function retrieves the number of tuples in a table.
    - It is thread-safe because it locks the `get_num_tuples_mutex` to ensure exclusive access while retrieving the TableMetadata and the number of tuples.

24. `getNumPages()`:
    - This function retrieves the total number of pages in the system.
    - It is thread-safe because it locks the `get_num_pages_mutex` to ensure exclusive access while retrieving the system catalog and the total number of pages.

25. `getNumFreePages()`:
    - This function retrieves the number of free pages in the system.
    - It is thread-safe because it locks the `get_num_free_pages_mutex` to ensure exclusive access while traversing the free page list and counting the free pages.

26. `getNumTables()`:
    - This function retrieves the number of tables in the system.
    - It is thread-safe because it locks the `get_num_tables_mutex` to ensure exclusive access while retrieving the system catalog and the number of tables.

27. `insertRecord(RM_TableData *rel, Record *record)`:
    - This function inserts a new record into a table.
    - It is thread-safe because it locks the `insert_record_mutex` to ensure exclusive access while performing the slot walk and updating the table metadata.

28. `deleteRecord(RM_TableData *rel, RID id)`:
    - This function deletes a record from a table.
    - It is thread-safe because it locks the `delete_record_mutex` to ensure exclusive access while marking the slot as unoccupied and updating the system catalog.

29. `updateRecord(RM_TableData *rel, Record *record)`:
    - This function updates an existing record in a table.
    - It is thread-safe because it locks the `update_record_mutex` to ensure exclusive access while updating the tuple data and marking the page as dirty.

30. `startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)`:
    - This function starts a new scan on a table.
    - It is thread-safe because it locks the `start_scan_mutex` to ensure exclusive access while initializing the scan handle and allocating memory for the management data.

31. `getRecord(RM_TableData *rel, RID id, Record *record)`:
    - This function retrieves a record from a table.
    - It is thread-safe because it locks the `get_record_mutex` to ensure exclusive access while reading the tuple data and allocating memory for the record data.

32. `next(RM_ScanHandle *scan, Record *record)`:
    - This function retrieves the next record during a scan.
    - It is thread-safe because it locks the `next_mutex` to ensure exclusive access while evaluating the condition and retrieving the record data.

33. `closeScan(RM_ScanHandle *scan)`:
    - This function closes a scan and frees associated memory.
    - It does not require thread safety as it only operates on a single scan handle and does not access shared resources.

34. `getAttributeSize(Schema *schema, int attributeIndex)`:
    - This function retrieves the size of an attribute based on its data type.
    - It is thread-safe because it locks the `getAttributeSize_mutex` to ensure exclusive access while calculating the attribute size.

35. `getRecordSize(Schema *schema)`:
    - This function calculates the size of a record based on the schema.
    - It does not require thread safety as it only operates on a single schema and does not access shared resources.

36. `createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)`:
    - This function creates a new schema.
    - It is thread-safe because it locks the `createSchema_mutex` to ensure exclusive access while allocating memory and initializing the schema.

37. `freeSchema(Schema *schema)`:
    - This function frees the memory allocated for a schema.
    - It is thread-safe because it locks the `freeSchema_mutex` to ensure exclusive access while freeing the memory.

38. `createRecord(Record **record, Schema *schema)`:
    - This function creates a new record based on a schema.
    - It is thread-safe because it locks the `createRecord_mutex` to ensure exclusive access while allocating memory and initializing the record.

39. `freeRecord(Record *record)`:
    - This function frees the memory allocated for a record.
    - It is thread-safe because it locks the `freeRecord_mutex` to ensure exclusive access while freeing the memory.

40. `getAttr(Record *record, Schema *schema, int attributeNum, Value **value)`:
    - This function retrieves the value of an attribute from a record.
    - It is thread-safe because it locks the `getAttr_mutex` to ensure exclusive access while allocating memory and retrieving the attribute value.

41. `calculateOffset(Schema *schema, int attributeNum)`:
    - This function calculates the offset of an attribute within a record.
    - It does not require thread safety as it only operates on a single schema and does not access shared resources.

42. `setAttr(Record *record, Schema *schema, int attributeNum, Value *value)`:
    - This function sets the value of an attribute in a record.
    - It is thread-safe because it locks the `setAttr_mutex` to ensure exclusive access while modifying the attribute value.


