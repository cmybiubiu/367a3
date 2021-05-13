#!/bin/bash 

#First load all related modules.  
#You can put the below two lines in a batch file. 
#But remember the modules might get unloaded so check you loaded modules frequently.
module load anaconda3/5.2.0
module load gcc/7.3.0
module load openmpi/3.1.1

#Compile the code
gcc -v
make clean
make

#Schedule your jobs with sbatch
sbatch job-a3-seq.sh


