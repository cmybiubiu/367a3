# ------------
# This code is provided solely for the personal and private use of
# students taking the CSC367 course at the University of Toronto.
# Copying for purposes other than this use is expressly prohibited.
# All forms of distribution of this code, whether as given or with
# any changes, are expressly prohibited.
#
# Authors: Bogdan Simion, Maryam Dehnavi, Alexey Khrabrov
#
# All of the files in this directory and all subdirectories are:
# Copyright (c) 2020 Bogdan Simion and Maryam Dehnavi
# -------------

CC = gcc
CFLAGS += -std=gnu11 -Wall -Werror -fopenmp -g3 -O3 -DNDEBUG
LDFLAGS += -lm -fopenmp

all: join-seq join-omp

data.o: data.h
join.o: join.h data.h
options.o: options.h
join-seq: time_util.h

join-seq: join-seq.o join.o data.o options.o hash.o
	$(CC) $^ -o $@ $(LDFLAGS)

join-omp: join-omp.o join.o data.o options.o hash.o
	$(CC) $^ -o $@ $(LDFLAGS)

hash: hash.o
		$(CC) $^ -o $@ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o join-seq join-omp
