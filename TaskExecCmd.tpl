#!/bin/csh -f
#  TaskExecCluster.cmd
#
#  SGE job for running TaskExecs
#
#  The following items pertain to this script
#  Use current working directory
#$ -cwd
#  input           = /dev/null
#  output          = $ExecWorkingDir/$Exec_N.log
#$ -o $ExecWorkingDir/$Exec_N.log
#  error           = Merged with joblog
#$ -j y
#  The following items pertain to the user program
#  user program    = /net/bamboo41/m2/anderson/Python/Python-2.5.1/common/bin/python
#  arguments       = $runCommand
#  program input   = Specified by user program
#  program output  = Specified by user program
#  Resources requested
##$ -l serial,memory=2048M,time=200:00:00
#$ -l serial
#  Name of application for log
#$ -v QQAPP=job
#  Email address to notify
#$ -M anderson@math.ucla.edu
#  Notify at beginning and end of job
#$ -m n
#  Job is not rerunable
#$ -r n
#  User priority
#$ -p 100
#
# Initialization for serial execution
#
  unalias *
  set qqversion = 
  set qqapp     = "job serial"
  set qqidir    = $ExecWorkingDir
  set qqjob     = $Exec_N
  set qqodir    = $ExecWorkingDir
  cd     $ExecWorkingDir
  source /usr/local/sgeqb/local/bin/qq.sge/qr.runtime
  if ($status != 0) exit (1)
#
  echo "SGE job for $Exec_N built $date"
  echo ""
  echo "  $Exec_N directory:"
  echo "    "$ExecWorkingDir
  echo "  Submitted to SGE:"
  echo "    "$qqsubmit
  echo "  SCRATCH directory:"
  echo "    "$qqscratch
#
  echo ""
  echo "$Exec_N started on:   "` hostname -s `
  echo "$Exec_N started at:   "` date `
  echo ""
#
# Run the user program
#
  echo /net/bamboo41/m2/anderson/Python/Python-2.5.1/common/bin/python "$runCommand" \>\& $Exec_N.output
  echo ""
  time /net/bamboo41/m2/anderson/Python/Python-2.5.1/common/bin/python $runCommand >& $ExecWorkingDir/$Exec_N_out
#
  echo ""
  echo "$Exec_N finished at:  "` date `
#
# Cleanup after serial execution
#
  source /usr/local/sgeqb/local/bin/qq.sge/qr.runtime
  exit (0)
