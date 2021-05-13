#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:10:00
#SBATCH --job-name a3_omp
#SBATCH --output=a3_omp_%j.out

#--cpus-per-task indicates the number of cores (threads) used in the OpenMP code

declare -a filearray=(
"../data/dataset0"
"../data/dataset1"
"../data/dataset2"
"../data/dataset3"
"../data/dataset4"
"../data/dataset5"
)

#-n:use Nested loop join, -m: use Sort-Merge join, -h use Hash join
join=-m

# Parallel option, mandatory for the OpenMP version, ignored by the sequential version.
#-r: use Fragment-and-Replication -s: use Symmetric partitioning
option=-s

#number of threads
nthreads=4

echo "#join=$join"
echo "#option=$option"
echo "#nthreads=$nthreads"


executable="./join-omp" 

for filename in "${filearray[@]}"
do
        echo "#filename = $filename"
	$executable $join $option -t 1 $filename
done

for filename in "${filearray[@]}"
do
        echo "#filename = $filename"
	$executable $join $option -t 2 $filename
done

for filename in "${filearray[@]}"
do
        echo "#filename = $filename"
	$executable $join $option -t 4 $filename
done

for filename in "${filearray[@]}"
do
        echo "#filename = $filename"
	$executable $join $option -t 8 $filename
done