
CC=gcc
CFLAGS=-I.

all: test_assign3_1 test_assign3_2

test_assign3_1: test_assign3_1.o rm_serializer.o expr.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	$(CC) -o $@ $^ $(CFLAGS)

test_assign3_2: test_assign3_2.o rm_serializer.o expr.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o 
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	rm -f *.o test_assign3_1 test_assign3_2
	rm -f data.txt

