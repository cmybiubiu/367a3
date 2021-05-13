// ------------
// This code is provided solely for the personal and private use of
// students taking the CSC367 course at the University of Toronto.
// Copying for purposes other than this use is expressly prohibited.
// All forms of distribution of this code, whether as given or with
// any changes, are expressly prohibited.
//
// Authors: Bogdan Simion, Maryam Dehnavi, Alexey Khrabrov
//
// All of the files in this directory and all subdirectories are:
// Copyright (c) 2020 Bogdan Simion and Maryam Dehnavi
// -------------

#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "hash.h"

struct hash_node{
	int key;
	struct hash_node *next;
};
typedef struct hash_node hash_node;

struct _hash_table_t {
	int size;
	struct hash_node **node;
};


// Create a hash table with 'size' buckets; the storage is allocated dynamically using malloc(); returns NULL on error
void init_node(struct hash_node* node){
	node->next = NULL;
	node->key = -1;
}

void create_node(struct hash_node* node, int key)
{
	node->key = key;
	node->next = NULL;

}

hash_table_t *hash_create(int size)
{
	assert(size > 0);

	hash_table_t *hash_table = malloc(sizeof(hash_table_t));
	hash_table->size = size;
	hash_table->node = (hash_node**) malloc(size * sizeof(hash_node));
	memset(hash_table->node, 0, size * sizeof(hash_node));
	return hash_table;
}

// Release all memory used by the hash table, its buckets and entries
void destroy_node(struct hash_node* node){
	if (node == NULL)
	{
		return;
	}

	destroy_node(node->next);
	free(node);
}

void hash_destroy(hash_table_t *table)
{
	assert(table != NULL);

	for (int i =0; i<table->size; i++)
	{
		destroy_node(table->node[i]);
	}

	free(table);
}


// Returns 0 if key is not found
int hash_get(hash_table_t *table, int key) {
	assert(table != NULL);
	int hash = key % table->size;
	struct hash_node *curr = table->node[hash];

	while (curr != NULL) {
		if (curr->key == key) {
			return 1;
		}
		curr = curr->next;
	}

	return 0;
}

// Returns 0 on success, -1 on failure
int hash_put(hash_table_t *table, int key) {
	assert(table != NULL);
	int hash = key % table->size;

	if (table->node[hash] && table->node[hash]->key) {
		struct hash_node *curr = table->node[hash];
		while (curr->next != NULL) {
			curr = curr->next;
		}
		curr->next = (hash_node*) malloc(sizeof(hash_node));
		create_node(curr->next, key);
	} else {
		table->node[hash] = (hash_node*) malloc(sizeof(hash_node));
		table->node[hash]->key = key;
		table->node[hash]->next = NULL;
	}
	return 0;
}
