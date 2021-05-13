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
#include <stddef.h>
#include <stdio.h>
#include "hash.h"
#include "join.h"


/* Nested loop implementation */
int join_nested(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int count = 0;

	for (int s = 0; s < students_count; ++s) {
		if (students[s].gpa > 3.0) {
			for (int t = 0; t < tas_count; ++t) {
				if (tas[t].sid == students[s].sid) {
					count ++;
				}
			}
		}
	}

	return count;
}

/* Sort-merge implementation. Assumes that records in both tables are already sorted by sid */
int join_merge(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int t = 0;
	int s = 0;
	int count = 0;

	while(t < tas_count && s < students_count) {
		if(tas[t].sid > students[s].sid){
			s ++;
		} else if(tas[t].sid < students[s].sid){
			t ++;
		} else {
			if (students[s].gpa > 3){
				// found a match for equi-join
				count ++;
			}

			int t_ = t + 1;
			while(t_ < tas_count && tas[t_].sid == students[s].sid){
				if (students[s].gpa > 3){
					count ++;
				}
				t_ ++;
			}

			int s_ = s + 1;
			while(s_ < students_count && tas[t].sid == students[s_].sid){
				if (students[s_].gpa > 3){
					count++;
				}
				s_ ++;
			}

			t = t_;
			s = s_;
		}
	}

	return count;
}

/* Hash join implementation */
int join_hash(const student_record *students, int students_count, const ta_record *tas, int tas_count)
{
	assert(students != NULL);
	assert(tas != NULL);

	int count = 0;

	hash_table_t *hash_table = hash_create(students_count);
	// We put student sid with above 3.0 gpa into hash table to reduce collision
	for (int s = 0; s < students_count; ++s) {
		if (students[s].gpa > 3.0) {
			hash_put(hash_table, students[s].sid);
		}
	}

	for (int t = 0; t < tas_count; ++t) {
		if(hash_get(hash_table, tas[t].sid)) {
			count ++;
		}
	}

	hash_destroy(hash_table);
	return count;

}
