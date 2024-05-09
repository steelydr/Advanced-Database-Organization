Team Members :-
* Lokareddy Prishitha
* Depala Rajeswari
* Charan Reddy

initStorageManager:
-Initializes the storage manager and prints the names of the team members.
-No specific storage-related operations are performed in this function.


createPageFile:
-Creates a new page file with the specified file name.
-Initializes an empty page within the file filled with null characters.
-Ensures that the write operation is successful by checking the number of bytes written.

openPageFile:
-Opens an existing page file and retrieves file information.
-Calculates the total number of pages in the file based on its size.
-Sets the current page position to zero.

closePageFile:
-Closes an open page file and prints size of the content in the file.

destroyPageFile:
-Destroys (deletes) an existing page file.
-Prints information about the creation and destruction times of the page file.
-Uses the remove function to delete the file.

writeBlock:
-Writes a specific block to the page file, handling various cases such as exceeding page size.
-Appends empty blocks if necessary to accommodate the data.
-Updates the current page position in the file handle.

writeCurrentBlock:
-Writes the contents of the current block in the file, as specified by the file handle.
-Utilizes the current page position to determine the block to be written.
-Updates the current page position in the file handle.

readBlock:
-Reads a specified block from the file into the provided memory page.
-Handles cases where the specified block is non-existing or reading fails.
-Updates the current page position in the file handle.

readFirstBlock:
-Reads the first block in the file and sets the current page position to zero.
-Calls the readBlock function with pageNum = 0.

readPreviousBlock:
-Reads the block preceding the current position and updates the current page position.
-Handles cases where the preceding block is non-existing or reading fails.
-Uses the readBlock function with the current position decremented by one.

readCurrentBlock:
-Reads the block at the current position as specified by the file handle.
-Uses the readBlock function with the current position.

readNextBlock:
-Reads the block succeeding the current position and updates the current page position.
-Handles cases where the succeeding block is non-existing or reading fails.
-Uses the readBlock function with the current position incremented by one.

readLastBlock:
-Reads the last block in the file and sets the current page position to total number of pages - 1.
-Calls the readBlock function with pageNum = totalNumPages - 1.

appendEmptyBlock:
-Appends an empty block to the end of the file.
-Increases the total number of pages and updates the global number of blocks.
-Utilizes fseek to move to the end of the file and writes an empty block.

ensureCapacity:
-Ensures that the file has the capacity for a specified number of pages.
-Appends empty blocks as needed to meet the required capacity in a loop.