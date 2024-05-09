#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

#include <stdio.h>
#include <stdlib.h>

// local functions
static void printStrat (BM_BufferPool *const bm);

// external functions
void printPoolContent(BM_BufferPool *const bm) {
    PageNumber *frameContent = getFrameContents(bm);
    bool *dirty = getDirtyFlags(bm);
    int *fixCount = getFixCounts(bm);

    printf("{");
    printStrat(bm);
    printf(" %i}: ", bm->numPages);

    for (int i = 0; i < bm->numPages; i++)
        printf("%s[%i%s%i]", ((i == 0) ? "" : ","), frameContent[i], (dirty[i] ? "x" : " "), fixCount[i]);
    
    printf("\n");

    // Free the allocated memory if necessary (depends on the implementation of getFrameContents, getDirtyFlags, and getFixCounts)
    free(frameContent); 
    free(dirty); 
    free(fixCount);
}


char *
sprintPoolContent (BM_BufferPool *const bm)
{
	PageNumber *frameContent;
	bool *dirty;
	int *fixCount;
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(256 + (22 * bm->numPages));
	frameContent = getFrameContents(bm);
	dirty = getDirtyFlags(bm);
	fixCount = getFixCounts(bm);

	for (i = 0; i < bm->numPages; i++)
		pos += sprintf(message + pos, "%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (dirty[i] ? "x": " "), fixCount[i]);

	return message;
	free(frameContent);
	free(dirty);
	free(frameContent);
	free(message);
}


void
printPageContent (BM_PageHandle *const page)
{
	int i;

	printf("[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		printf("%02X%s%s", page->data[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");
}

char *
sprintPageContent (BM_PageHandle *const page)
{
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(30 + (2 * PAGE_SIZE) + (PAGE_SIZE % 64) + (PAGE_SIZE % 8));
	pos += sprintf(message + pos, "[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		pos += sprintf(message + pos, "%02X%s%s", page->data[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");

	return message;
	free(message);
}

void
printStrat (BM_BufferPool *const bm)
{
	switch (bm->strategy)
	{
	case RS_FIFO:
		printf("FIFO");
		break;
	case RS_LRU:
		printf("LRU");
		break;
	case RS_CLOCK:
		printf("CLOCK");
		break;
	case RS_LFU:
		printf("LFU");
		break;
	case RS_LRU_K:
		printf("LRU-K");
		break;
	default:
		printf("%i", bm->strategy);
		break;
	}
}

