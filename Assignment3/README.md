The record_mgr.c code is an implementation of a record manager component for a database management system, providing functionality for creating, manipulating, and scanning tables and records.

Team Members Contribution
Depala Rajeswari 36%
Prishitha Lokareddy 34%
Charan Reddy 30%

1. **SystemCatalog\* getSystemCatalog();Depala Rajeswari**
   - This function retrieves the system catalog, which stores information about the tables in the database.
   - It returns a pointer to the SystemCatalog struct, which contains the total number of pages, the free page number, the number of tables, and the table metadata for each table.

2. **RC markSystemCatalogDirty();Charan Reddy**
   - This function marks the system catalog page in the buffer pool as dirty, indicating that it has been modified and needs to be written back to disk.
   - It returns a return code (RC) indicating the success or failure of the operation.

3. **int setFreePage(BM_PageHandle\* handle);Depala Rajeswari**
   - This function sets a page in the buffer pool as a free page, making it available for future use.
   - It is likely used for handling overflow pages, but the current implementation is a stub that always returns 1.

4. **TableMetadata\* getTableByName(char\* name);Prishitha Lokareddy**
   - This function retrieves the TableMetadata struct for the table with the given name.
   - It searches the system catalog for the table and returns a pointer to the corresponding TableMetadata struct, or NULL if the table is not found.

5. **int calculateOffset(Schema\* schema, int attributeNum);Depala Rajeswari**
   - This function calculates the offset of a specific attribute within a record, based on the schema and the attribute number.
   - It iterates through the attributes in the schema up to the desired attribute and sums the sizes of the previous attributes to determine the offset.

6. **int closeSlotWalk(TableMetadata\* table, BM_PageHandle\*\* handle);Depala Rajeswari**
   - This function closes a slot walk, which is a process of iterating through the slots (records) in a table.
   - If the page handle provided is not for the table's page, it unpins the page from the buffer pool.
   - It returns 0 if the operation is successful, or 1 if an error occurs.

7. **bool\* getSlots(BM_PageHandle\* handle);Prishitha Lokareddy**
   - This function returns a pointer to the slot array (a boolean array indicating the occupancy of each slot) in the given page handle.

8. **char\* getTupleData(BM_PageHandle\* handle);Prishitha Lokareddy**
   - This function returns a pointer to the tuple data area (the area where the actual record data is stored) in the given page handle.

9. **int getFreePageNumber();Depala Rajeswari**
   - This function retrieves a free page number from the system catalog and initializes the page header for the new page.
   - If there are no free pages available, it creates a new page and updates the system catalog accordingly.
   - It returns the page number of the free page.

10. **PageHeader\* getPageHeader(BM_PageHandle\* handle);Charan Reddy**
    - This function returns a pointer to the page header of the given page handle.
    - The page header contains information about the page, such as the next and previous page numbers and the number of slots.

11. **int appendToFreeList(int pageNumber);Depala Rajeswari**
    - This function appends a page to the free page list maintained in the system catalog.
    - It updates the next and previous page numbers in the free page list to include the new page.
    - It returns 0 if the operation is successful.

12. **int getNextSlotInWalk(TableMetadata\* table, BM_PageHandle\*\* handle, bool\*\* slots, int\* slotIndex);Depala Rajeswari**
    - This function retrieves the next available slot during a slot walk (an iteration over the slots in a table).
    - It updates the slot index and checks if the current page has any more available slots.
    - If the current page is exhausted, it moves to the next page and continues the slot walk.
    - It returns 0 if a free slot is found, -1 if there are no more slots, and a positive value if an error occurs.

13. **RC validateSystemConfig();Depala Rajeswari**
    - This function validates the system configuration, specifically checking if the page size is smaller than the system catalog size or the maximum number of tables is invalid.
    - It returns RC_OK if the configuration is valid, or RC_SYSTEM_CONFIG_ERROR if the configuration is invalid.

14. **void handleResult(const char\* message, RC result, const char\* fileName);Depala Rajeswari**
    - This function prints an error message if the provided result code is not RC_OK.
    - It includes the error message, the result code, and the file name (if provided) in the output.

15. **int getAttributeSize(Schema\* schema, int attributeIndex);Charan Reddy**
    - This function calculates the size of a specific attribute in the given schema, based on the data type of the attribute.
    - It returns the size of the attribute, or -1 if the attribute index is invalid or the data type is unknown.

16. **initRecordManager(void *mgmtData):Depala Rajeswari**
   - Validates the system configuration to ensure the page size and maximum table limit are valid.
   - Initializes the buffer pool and pins the catalog page, creating a new system schema if the file is new.
   - Marks the system catalog as dirty if the file is new or if the free page number or number of tables is changed.

17. **shutdownRecordManager():Prishitha Lokareddy**
   - Unpins the catalog page from the buffer pool.
   - Shuts down the buffer pool.
   - Returns the result of the buffer pool shutdown operation.

18. **createTable(char *name, Schema *schema):Prishitha Lokareddy**
   - Checks if the table already exists and if the catalog can hold another table.
   - Creates a new table metadata, initializing the table's page, and marking all slots as free.
   - Copies the attribute names, data types, and key data from the provided schema into the table metadata.
   - Updates the system catalog with the new table and marks it as dirty.

19. **openTable(RM_TableData *rel, char *name):Charan Reddy**
   - Retrieves the table metadata for the given table name.
   - Allocates memory for the `RM_TableData` structure, including the schema and page handle.
   - Copies the attribute data, data types, and key data from the table metadata into the `RM_TableData` structure.
   - Pins the table's page in the buffer pool.

20. **closeTable(RM_TableData *rel):Prishitha Lokareddy**
   - Unpins and forces the table's page to disk.
   - Frees the memory allocated for the schema and the page handle.

21. **deleteTable(char *name):Charan Reddy**
   - Finds the table with the matching name in the system catalog.
   - Adds the table's page chain to the free list.
   - Shifts the remaining table entries in the catalog down and decrements the number of tables.
   - Marks the system catalog as dirty.

22. **getNumTuples(RM_TableData *rel):Charan Reddy**
   - Retrieves the `TableMetadata` from the `RM_TableData` structure.
   - Returns the number of tuples stored in the table.
   - Handles invalid table data and metadata.

23. **getNumPages():Charan Reddy**
   - Retrieves the `SystemCatalog` and returns the total number of pages.
   - Handles the case where the system catalog pointer is `NULL`.

24. **getNumFreePages():Prishitha Lokareddy**
   - Uses the page handle header to traverse the free page list and count the number of free pages.
   - Returns the count of free pages.

25. **getNumTables():Depala Rajeswari**
    - Retrieves the `SystemCatalog` and returns the number of tables stored in it.
    - Handles the case where the system catalog pointer is `NULL`.
