#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF1 "test_1_pagefile.bin"
#define TESTPF2 "test_2_pagefile.bin"
#define TESTPF3 "test_3_pagefile.bin"

/* prototypes for test functions */
static void testMultipleFiles(void);
static void testMultiplePageContent(void);
static void testEnsureCapacity(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();
  testMultipleFiles();
  testMultiplePageContent();
  testEnsureCapacity();

  return 0;
}

/* Try to create, open, and close a page file */
void
testMultipleFiles(void)
{
  SM_FileHandle fh1, fh2, fh3;
  SM_PageHandle ph = (SM_PageHandle) malloc(PAGE_SIZE);

  testName = "test creating, opening, and closing multiple files";

  TEST_CHECK(createPageFile (TESTPF1));
  TEST_CHECK(createPageFile (TESTPF2));
  TEST_CHECK(createPageFile (TESTPF3));
  
  TEST_CHECK(openPageFile (TESTPF1, &fh1));
  TEST_CHECK(openPageFile (TESTPF2, &fh2));
  TEST_CHECK(openPageFile (TESTPF3, &fh3));
  
  strcpy(ph, "FILE 1");
  TEST_CHECK(writeBlock (0, &fh1, ph));
  strcpy(ph, "FILE 2");
  TEST_CHECK(writeBlock (0, &fh2, ph));
  strcpy(ph, "FILE 3");
  TEST_CHECK(writeBlock (0, &fh3, ph));

  char *string1, *string2, *string3; 

  TEST_CHECK(readFirstBlock (&fh1, ph));
  ASSERT_TRUE(strcmp("FILE 1", (char *)ph) == 0, "file 1 was written correctly");
  TEST_CHECK(readFirstBlock (&fh2, ph));
  ASSERT_TRUE(strcmp("FILE 2", (char *)ph) == 0, "file 2 was written correctly");
  TEST_CHECK(readFirstBlock (&fh3, ph));
  ASSERT_TRUE(strcmp("FILE 3", (char *)ph) == 0, "file 3 was written correctly");
  
  TEST_CHECK(closePageFile (&fh1));
  TEST_CHECK(closePageFile (&fh2));
  TEST_CHECK(closePageFile (&fh3));

  TEST_CHECK(destroyPageFile (TESTPF1));
  TEST_CHECK(destroyPageFile (TESTPF2));
  TEST_CHECK(destroyPageFile (TESTPF3));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF1, &fh1) != RC_OK), "opening non-existing file should return an error.");
  free(ph);
  TEST_DONE();
}


/* Try to create multiple pages with different content */
void
testMultiplePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;

  testName = "test multiple page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF1));
  TEST_CHECK(openPageFile (TESTPF1, &fh));
  printf("created and opened file\n");
  
  strcpy(ph, "PAGE 0\n");
  TEST_CHECK(writeBlock (0, &fh, ph));
  TEST_CHECK(appendEmptyBlock (&fh));
  strcpy(ph, "PAGE 1\n");
  TEST_CHECK(writeBlock (1, &fh, ph));
  TEST_CHECK(appendEmptyBlock (&fh));
  strcpy(ph, "PAGE 2\n");
  TEST_CHECK(writeBlock (2, &fh, ph));
  TEST_CHECK(appendEmptyBlock (&fh));
  strcpy(ph, "PAGE 3\n");
  TEST_CHECK(writeBlock (3, &fh, ph));
  strcpy(ph, "PAGE 4\n");
  TEST_CHECK(appendEmptyBlock (&fh));
  TEST_CHECK(writeBlock (4, &fh, ph));

  ASSERT_TRUE((fh.totalNumPages == 5), "expect 5 pages in new file");

  // read back the pages
  TEST_CHECK(readCurrentBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 0\n", (char *)ph) == 0, "page 0 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 1\n", (char *)ph) == 0, "page 1 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 2\n", (char *)ph) == 0, "page 2 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 3\n", (char *)ph) == 0, "page 3 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 4\n", (char *)ph) == 0, "page 4 was written correctly");

  // read the next page after the last one
  ASSERT_TRUE((readNextBlock(&fh, ph) != RC_OK), "reading the next page should return an error.");

  // test first and last reads
  TEST_CHECK(readFirstBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 0\n", (char *)ph) == 0, "first page was written correctly");
  TEST_CHECK(readLastBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 4\n", (char *)ph) == 0, "last page was written correctly");

  // close the file
  TEST_CHECK(closePageFile (&fh));

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF1)); 
  free(ph); 
  
  TEST_DONE();
}

/* Try to create multiple pages using ensureCapacity */
void
testEnsureCapacity(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;

  testName = "Ensure Capacity";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF1));
  TEST_CHECK(openPageFile (TESTPF1, &fh));
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 pages in new file");
  printf("created and opened file\n");
  
  TEST_CHECK(ensureCapacity (5, &fh));
  ASSERT_TRUE((fh.totalNumPages == 5), "expect 5 pages in new file");
  printf("ensured length of 5\n");

  strcpy(ph, "PAGE 0\n");
  TEST_CHECK(writeBlock (0, &fh, ph));
  strcpy(ph, "PAGE 1\n");
  TEST_CHECK(writeBlock (1, &fh, ph));
  strcpy(ph, "PAGE 2\n");
  TEST_CHECK(writeBlock (2, &fh, ph));
  strcpy(ph, "PAGE 3\n");
  TEST_CHECK(writeBlock (3, &fh, ph));
  strcpy(ph, "PAGE 4\n");
  TEST_CHECK(writeBlock (4, &fh, ph));


  // read back the pages
  TEST_CHECK(readCurrentBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 0\n", (char *)ph) == 0, "page 0 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 1\n", (char *)ph) == 0, "page 1 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 2\n", (char *)ph) == 0, "page 2 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 3\n", (char *)ph) == 0, "page 3 was written correctly");
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_TRUE(strcmp("PAGE 4\n", (char *)ph) == 0, "page 4 was written correctly");

  // close the file
  TEST_CHECK(closePageFile (&fh));

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF1));  
  free(ph);
  TEST_DONE();
}
