# Compiler and flags
CC := gcc
CFLAGS := -Wall -Wextra -std=c99

SRCS := test_assign1_1.c test_assign1_2.c test_assign1_3.c storage_mgr.c dberror.c

EXECS := test_assign1_1 test_assign1_2 test_assign1_3

all: $(EXECS)

test_assign1_1: test_assign1_1.c storage_mgr.c dberror.c
	$(CC) $(CFLAGS) -o test_assign1_1 test_assign1_1.c storage_mgr.c dberror.c

test_assign1_2: test_assign1_2.c storage_mgr.c dberror.c
	$(CC) $(CFLAGS) -o test_assign1_2 test_assign1_2.c storage_mgr.c dberror.c

test_assign1_3: test_assign1_3.c storage_mgr.c dberror.c
	$(CC) $(CFLAGS) -o test_assign1_3 test_assign1_3.c storage_mgr.c dberror.c

.PHONY: clean
clean:
	rm -f $(EXECS)

.PHONY: rebuild
rebuild: clean all

.PHONY: run1
run1: test_assign1_1
	./test_assign1_1

.PHONY: run2
run2: test_assign1_2
	./test_assign1_2

.PHONY: run3
run2: test_assign1_3
	./test_assign1_3
	
