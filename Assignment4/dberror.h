#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096
#define FIRST_PAGE 0
#define ONE 1
#define ZERO 0
#define PAGE_OFFSET 1
#define booolean
/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_READ_FAILED 5
#define RC_BM_SPC_ALLOC_FAILED 6
#define RC_UNKNOWN_ERROR 7
#define RC_IVLD_PAGE_NUM 8
#define RC_BUFFER_IN_USE 9

#define RC_MELLOC_MEM_ALLOC_FAILED 10
#define RC_SCHEMA_NOT_INIT 11
#define RC_PIN_PAGE_FAILED 12
#define RC_UNPIN_PAGE_FAILED 13
#define RC_INVLD_PAGE_NUM 14
#define RC_IVALID_PAGE_SLOT_NUM 15
#define RC_MARK_DIRTY_FAILED 16
#define RC_BUFFER_SHUTDOWN_FAILED 17
#define RC_NULL_IP_PARAM 18
#define RC_FILE_DESTROY_FAILED 19
#define RC_OPEN_TABLE_FAILED 20

#define RC_SEEK_FAILED 101

#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205


#define RC_RM_SCHEMA_NOT_FOUND 206
#define RC_RM_WRONG_ATTRNUM 207

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

#define RC_CANNOT_GO_TO_PREV_PAGE 401
#define RC_CANNOT_GO_TO_NEXT_PAGE 402

typedef int boolean;
enum { FALSE, TRUE };



  
/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
