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

#include <stdio.h>
#include <stdlib.h>

#include "join.h"
#include "options.h"
#include "time_util.h"
#include "hash.h"


int main(int argc, char *argv[])
{
	const char *path = parse_args(argc, argv);
	if (path == NULL) return 1;

	int students_count, tas_count;
	student_record *students;
	ta_record *tas;
	if (load_data(path, &students, &students_count, &tas, &tas_count) != 0) return 1;

	int result = 1;
	join_func_t *join_f = opt_nested ? join_nested : (opt_merge ? join_merge : join_hash);

	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	int count = join_f(students, students_count, tas, tas_count);
	clock_gettime(CLOCK_MONOTONIC, &end);

	if (count < 0) goto end;
	char *method = opt_nested ? "nested" : (opt_merge ? "merge" : "hash");
	// printf("count: %d\n", count);
	// printf("time: %f\n", timespec_to_msec(difftimespec(end, start)));
	// printf("data: %s\n", path);
	// printf("method:%s\n", method);


	FILE *f = fopen("test1.txt", "a");
	if (f == NULL) {
		printf("could not open output file\n");
		return -1;
	}
	fprintf(f, "method: %s data: %s time: %f\n", method, path, timespec_to_msec(difftimespec(end, start)));
	fclose(f);

	result = 0;

end:
	free(students);
	free(tas);
	return result;
}
