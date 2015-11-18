
CC=g++
CFLAGS=-I.
LDFLAGS=-L.

DEPS=LRDataProvider.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

datafeeder: DataFeeder.o libLRDataProvider.a
	$(CC) -o datafeeder DataFeeder.o -I. -lpthread libLRDataProvider.a

libLRDataProvider.a: LRDataProvider.o MemTuples.o Tuple.o
	ar cru libLRDataProvider.a LRDataProvider.o MemTuples.o Tuple.o
	ranlib libLRDataProvider.a

.PHONY: clean

clean:
	rm *.o
	rm libLRDataProvider.a datafeeder
