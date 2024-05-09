1.initBufferPool:
-Allocates memory for the buffer pool metadata, including page frames and page table.
-Opens the specified page file for the buffer pool, allowing pages to be read from and written to disk.
-Initializes page frames, setting their fix counts, dirty flags, and other metadata.

2.shutdownBufferPool:
-Releases all allocated memory associated with the buffer pool, including metadata, page frames, and page table.
-Closes the page file associated with the buffer pool, ensuring no further read or write operations can be performed.
-Frees resources associated with the page table and page frames, ensuring proper cleanup and preventing memory leaks.

3.replacementFIFO:
-Implements the FIFO (First-In-First-Out) page replacement strategy, where the oldest page in the buffer pool is replaced when it is full.
-Selects a page frame for replacement based on the order in which pages were brought into the buffer pool.
-Returns an appropriate RC code indicating whether a page frame was successfully selected for replacement or if the buffer pool is full.

4.replacementLRU:
-Implements the LRU (Least Recently Used) page replacement strategy, where the least recently accessed page is replaced when the buffer pool is full.
-Selects a page frame for replacement based on the timestamp of the last access to each page.
-Returns a pointer to the selected page frame after eviction, ensuring proper replacement according to the LRU strategy.


5.pinPage:
-Pins a page in the buffer pool, either by retrieving it from disk or selecting a replacement page using the specified replacement strategy.
-Increments the fix count of the pinned page, preventing it from being replaced until it is unpinned.
-Returns an appropriate RC code indicating whether the page was successfully pinned or if an error occurred.

6.unpinPage:
-Decrements the fix count of a page in the buffer pool, allowing it to be replaced if necessary according to the replacement strategy.
-Ensures that the fix count of the page does not go below zero, maintaining proper reference counting.
-Returns an appropriate RC code indicating whether the page fix count was successfully decremented.

7.forcePage:
-Writes a dirty page back to disk if necessary, ensuring data consistency between the buffer pool and the page file.
-Updates metadata and page frame information after writing the page to disk, including incrementing the write IO count.
-Returns an appropriate RC code indicating whether the page was successfully written to disk.

8.getFrameContents, getDirtyFlags, getFixCounts:
-Retrieve information about the current state of the pages in the buffer pool, such as their page numbers, dirty flags, and fix counts.
-Return arrays containing this information, allowing external components to inspect the state of the buffer pool.
-Useful for monitoring and debugging purposes.

9.getNumReadIO, getNumWriteIO:
-Retrieve the number of read and write IO operations performed on the buffer pool, respectively.
-Provide insight into the I/O activity of the buffer pool, allowing performance analysis and optimization.

10.AL_get:
-Retrieves the ArrayList at index i from the provided hash table.
-Casts the management pointer to an ArrayList pointer.
-Returns a pointer to the ArrayList at the specified index.

11.AL_push:
-Adds a key-value pair to the ArrayList.
-Checks if the ArrayList is full and reallocates memory if needed.
-Inserts the key-value pair at the end of the ArrayList.
-Increments the size of the ArrayList.

12.AL_remoteAt:
-Removes an element at the specified index from the ArrayList.
-Shifts elements to the left to fill the gap created by removing the element.
-Decrements the size of the ArrayList.

13.initHashTable:
-Initializes a hash table with the specified size.
-Allocates memory for an array of ArrayLists based on the size.
-Initializes each ArrayList within the hash table with a default capacity and size.

14.getValue:
-Retrieves the value corresponding to a given key from the hash table.
-Calculates the hash value to determine the index in the hash table.
-Searches the ArrayList at the calculated index for the key.
-If found, updates the value and returns 0; otherwise, returns 1.

15.setValue:
-Sets the value corresponding to a given key in the hash table.
-Calculates the hash value to determine the index in the hash table.
-Searches the ArrayList at the calculated index for the key.
-If found, updates the value; otherwise, adds a new key-value pair to the ArrayList.

16.removePair:
-Removes a key-value pair from the hash table.
-Calculates the hash value to determine the index in the hash table.
-Searches the ArrayList at the calculated index for the key.
-If found, removes the key-value pair from the ArrayList.

17.freeHashTable:
-Frees the memory allocated for the hash table and its ArrayLists.
-Iterates through each ArrayList and frees the memory allocated for its list.
-Finally, frees the memory allocated for the hash table management structure.

18.getTimeStamp:
-Takes a pointer to BM_Metadata structure as input.
-Retrieves the timestamp from the metadata and increments it by one.
-Returns the incremented timestamp.

19.updateTimeStamp:
-Takes a pointer to BM_PageFrame and a pointer to BM_Metadata structures as input.
-Updates the timestamp of the given page frame by calling getTimeStamp with the provided metadata.

20.evictPage:
-Takes a pointer to BM_PageFrame, a pointer to HT_TableHandle, and a pointer to BM_Metadata structures as input.
-Removes the entry corresponding to the page frame from the page table.
-If the page frame is dirty, writes it back to the page file and increments the write counter in the metadata.

21.getAfterEviction:
-Takes a pointer to BM_BufferPool structure and an integer frame index as input.
-Retrieves the page frame at the given index from the buffer pool metadata.
-Updates the timestamp of the retrieved page frame using updateTimeStamp.
-If the page frame is occupied, evicts it from the page table using evictPage.
-Returns a pointer to the page frame at the specified index after eviction.

