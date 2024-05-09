#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<math.h>
#include<fcntl.h>
#include<string.h>
#include "storage_mgr.h"
#include <sys/statvfs.h>
#include <time.h>
#include <errno.h>

#define RC_MEM_ALLOC_ERROR 5
#define RC_FILE_OPEN_FAILED 8
#define RC_FILE_STAT_FAILED  9
#define RC_WRITE_NON_EXISTING_PAGE 10
#define RC_FILE_ACCESS_DENIED 12

// Initializing all variables 
SM_FileHandle* filehandle;
int globalCurPagePos = 0;
int numofblocks=0;
extern void initStorageManager (void) { 
    fputs("Lokareddy Prishitha\n", stdout);
    fputs("Depala Rajeswari\n", stdout);
    fputs("Charan Reddy\n", stdout);
}


//***************************** File Management Information Operation ****************************************//

//We create a structure which stores file management infrormation such as time created and destroyed
typedef struct{
    time_t created;
    time_t destroyed; 
}mgmtInfo;

//This function prints the File information of the storage manager
void printInfo(mgmtInfo* in) {
    printf("Page File created at  %s", ctime(&(in->created)));
    printf("Page File destroyed at  %s", ctime(&(in->destroyed)));
}

//***************************** End Of File Management Information Operation ****************************************//



extern int getBlockPos(SM_FileHandle *fHandle) {
    //Initially the current  page position is zero
    fHandle->curPagePos= globalCurPagePos;

    //We check if the value is less than zero or more than  number of pages then return an error
    if (fHandle->curPagePos < 0||  fHandle->curPagePos > numofblocks) {
        perror("It is an invalid Page Position");
        return -1;
    }
    if(fHandle->totalNumPages == numofblocks)
    {
      return 0;
    }
    return fHandle->curPagePos;
}


//*************************************** File Operations *******************************************// 

RC createPageFile(char *fileName) {

    // We call the management info structure to store the time when we create our page file 
    filehandle = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
    filehandle->mgmtInfo = (mgmtInfo*)malloc(sizeof(mgmtInfo));
    time(&(((mgmtInfo*)filehandle->mgmtInfo)->created));
    
    //Free memory
    free(filehandle->mgmtInfo);
    free(filehandle);
    filehandle = NULL;
    
    int fd = open(fileName, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (fd == -1) {
        return RC_FILE_NOT_FOUND;
    } else {
        //Create an empty page and fill it with null character
        char *emptyPage = (char *)malloc(PAGE_SIZE);
        
        for (int i = 0; i < PAGE_SIZE; i++) {
            emptyPage[i] = '\0';
        }

        //Get the size written into the file 
        ssize_t written = write(fd, emptyPage, PAGE_SIZE);

        close(fd);
        free(emptyPage);
        emptyPage = NULL;

        //Checks wether the number of bytes written to the page is equal to the size of page size
        if (written != PAGE_SIZE) {
            return RC_WRITE_FAILED;
        } else {
            return RC_OK;
        }
    }
    
}


RC openPageFile(char* fileName, SM_FileHandle* fHandle)
{
    FILE* fp = fopen(fileName, "r+");
    if (fp == NULL) {
        switch (errno) {
            case ENOENT:
                return RC_FILE_NOT_FOUND;
            case EACCES:
                return RC_FILE_ACCESS_DENIED;
            default:
                return RC_FILE_OPEN_FAILED;
        }
    }
    // Get the file size using fstat
    struct stat fileInfo;
    if (fstat(fileno(fp), &fileInfo) != 0) {
            fclose(fp);
           return RC_FILE_STAT_FAILED;
    }

    // Calculate the total number of pages
    int totalNumPages = fileInfo.st_size / PAGE_SIZE;
    if (fileInfo.st_size % PAGE_SIZE != 0) {
                totalNumPages++;
     }

     // Set metadata
     fHandle->fileName = strdup(fileName);
     fHandle->totalNumPages = totalNumPages;
     fHandle->curPagePos = 0;


    // Store the file pointer in `mgmtInfo` to use elsewhere
    fHandle->mgmtInfo = (void*)fp;

    return RC_OK;
}


RC closePageFile(SM_FileHandle *fHandle) {
    FILE *file = fopen(fHandle->fileName, "r+");

    fseek(file, 0, SEEK_END); 
    //We check for the file size before closing and written the file size
    long fileSize = ftell(file); 
    if (fileSize < 0) {
        perror("Error getting file size");
    } else {
        printf("File size: %ld bytes\n", fileSize);
    }
    if (fclose(file) == 0) {
        return RC_OK;
    } else {
        return RC_FILE_NOT_FOUND;
    }
    fclose(file);
    file = NULL; 
}

RC destroyPageFile(char *fileName) {

    // We call the management info structure to store the time when we destroy our page file
    filehandle = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
    filehandle->mgmtInfo = (mgmtInfo*)malloc(sizeof(mgmtInfo));
    time(&(((mgmtInfo*)filehandle->mgmtInfo)->destroyed));

    //Before destroying the page we call and print the file information using printInfo
    printInfo((mgmtInfo*)filehandle->mgmtInfo);
    
    //Free the memory
    free(filehandle->mgmtInfo);
    free(filehandle);

    if (remove(fileName) == 0) {
         printf("File deleted successfully");
        return RC_OK;
    } else {
        perror("Error deleting file");
        return RC_FILE_NOT_FOUND; 
    }
}

/**************************** End of File Operations ****************/



//************************** Write Operations ****************************************/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check the handle to see if pageNum is in range
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
        return RC_WRITE_NON_EXISTING_PAGE;

    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // check to see if the file pointer is valid
    if (fp == NULL)
        return RC_FILE_NOT_FOUND;

    // set the file position to the desired page
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0)
        return RC_SEEK_FAILED;

    // write the page data to the file
    size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
    if (bytesWritten != PAGE_SIZE)
        return RC_WRITE_FAILED;

    // update the file handle's information
    fHandle->curPagePos = pageNum;
    fHandle->totalNumPages = (pageNum + 1 > fHandle->totalNumPages) ? pageNum + 1 : fHandle->totalNumPages;

    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    FILE *file = fopen(fHandle->fileName, "r+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    fseek(file, fHandle->curPagePos, SEEK_SET);
    fwrite(memPage, sizeof(char), PAGE_SIZE, file);

    fHandle->curPagePos = ftell(file);

    fclose(file);
    file = NULL; 
    return RC_OK;
}

//************************ End Of Write Operations *****************************


//************************** Read Operations ***********************************


RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || pageNum > fHandle->totalNumPages) {
        printf("Some error");
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    if(globalCurPagePos > fHandle->totalNumPages)
       return RC_READ_FAILED;

    FILE *file = fopen(fHandle->fileName, "r");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    //We read the file from the begining of the file and read the content of the file
    // , if the content read is less then the page size then it returns error else close the file
    if (fseek(file, pageNum * PAGE_SIZE, SEEK_SET) == 0) {
        size_t read = fread(memPage, sizeof(char), PAGE_SIZE, file);
        if (read < PAGE_SIZE && feof(file) == 0) {
            fclose(file);
            file = NULL; 
            return RC_READ_FAILED;
        }
    } else {
        fclose(file);
        file = NULL; 
        return RC_READ_FAILED;
    }

    fHandle->curPagePos = ftell(file);

    fclose(file);
    file = NULL; 
    return RC_OK;
}

// Reading the first block in a file And set the current position to zero
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

//Reading the previous block in a file And set the position to currentposition-1
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int currentPosition = getBlockPos(fHandle);
    globalCurPagePos += currentPosition; 
    if (globalCurPagePos + 1 <= fHandle->totalNumPages || globalCurPagePos <0) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    globalCurPagePos -= 1;
    return readBlock(globalCurPagePos, fHandle, memPage);
}

// Reading current block in a file
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (numofblocks >= fHandle->totalNumPages) {
        globalCurPagePos=0;
    }
    return readBlock(globalCurPagePos, fHandle, memPage);
}

//Reading the previous block in a file And set the position to currentposition+1
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int currentPosition = getBlockPos(fHandle);
    globalCurPagePos = currentPosition; 
    if (globalCurPagePos + 1 >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    globalCurPagePos += 1;
    return readBlock(globalCurPagePos, fHandle, memPage);
}


// Reading the last block in a file And set the current position to totalnumberofpages-1
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

//****************************** End Of Read Operations ******************************


//******************************** Append Operation **********************************

RC appendEmptyBlock(SM_FileHandle *fHandle) {

    //Open file to be appended
    FILE *file = fopen(fHandle->fileName, "ab+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    //Initialize an empty block to be added with page size
    char *emptyBlock = malloc(PAGE_SIZE);  
    if (!emptyBlock) {
        fclose(file);
        return RC_MEM_ALLOC_ERROR;  
    }

    //Fill this empty block to null character
    memset(emptyBlock, '\0', PAGE_SIZE); 

    int isSeek = fseek(file, 0, SEEK_END);

    //Check wether an empty is written successfully or not
    if (isSeek == 0) {
        fwrite(emptyBlock, sizeof(char), PAGE_SIZE, file);
        printf("An empty block is appended\n");

        //Increment the number of pages by one
        fHandle->totalNumPages += 1;
        numofblocks+=1;
    } else {

        //Free the memory
        free(emptyBlock);
        fclose(file);
        return RC_WRITE_FAILED;
    }

    //Free the memory
    free(emptyBlock);
    fclose(file);
    file = NULL; 

    return RC_OK;
}

//**************************** End Of Append Operation *************************************

//**************************** Ensure Capacity *******************************************
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->fileName == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    struct statvfs stat;
    if (statvfs(fHandle->fileName, &stat) != 0) {
        perror("Error getting disk information");
    }
    
    //Get free space and total space on the disk
    unsigned long long free_space = stat.f_bsize * stat.f_bfree;
    unsigned long long total_space = stat.f_bsize * stat.f_blocks;

    double free_space_percentage = (double)free_space / total_space * 100;
    //CHECK wether you have enough space to store or append data ahead
    if (free_space_percentage < 1.0) {
        perror("Your disk is full");
    }
    FILE *file = fopen(fHandle->fileName, "a");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    //If numberofpages is more than the total number of pages then append empty blocks as required
    int pagesToAdd = numberOfPages - fHandle->totalNumPages;
    while (pagesToAdd > 0) {
        //If failed to append the block then return error
        if (appendEmptyBlock(fHandle) != RC_OK) {
            fclose(file);
            file = NULL; 
            return RC_FILE_NOT_FOUND;
        }
        pagesToAdd--;
    }
    fclose(file);
    file = NULL; 
    return RC_OK;
}

//********************************** END OF ALL THE OPERATIONS *************************************************************


