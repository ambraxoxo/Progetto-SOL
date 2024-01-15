CC = gcc
CFLAGS = -Wall -pedantic -g
LDFLAGS = -lpthread
objects = farm.o boundedqueue.o 

all: farm

farm: farm.o boundedqueue.o
	$(CC) $(CFLAGS) $(objects) -o farm $(LDFLAGS) 

farm.o: farm.c conn.h util.h
	$(CC) $(CFLAGS) -c farm.c

boundedqueue.o: boundedqueue.c boundedqueue.h
	$(CC) $(CFLAGS) -c boundedqueue.c

clean:
	rm -f farm $(objects) file* expected.txt
	rm -r testdir
run:
	make clean
	make 
	./test.sh
