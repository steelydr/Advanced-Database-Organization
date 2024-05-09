CC = gcc
CFLAGS = -Wall -Wextra -std=c99

all: test_assign2_1 test_assign2_2

test_assign2_1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o $@ $^

test_assign2_2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o test_assign2_1 test_assign2_2


# Link the object files to create executables
$(EXECS): %: %.o $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

.PHONY: rebuild
rebuild: clean all

.PHONY: run1
run1: test_assign2_1
	./test_assign2_1

.PHONY: run2
run2: test_assign2_2
	./test_assign2_2

