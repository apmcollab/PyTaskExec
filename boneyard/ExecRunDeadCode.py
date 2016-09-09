#    cmdParameters = {}
#    cmdParameters['execIndex']     = i;
#    cmdParameters['Exec_N']         = 'Exec_%d' % i
#    cmdParameters['Exec_N_out']     = 'Exec_%d_$JOB_ID.out' % i
#    cmdParameters['ExecWorkingDir'] = execDir
#    cmdParameters['runCommand']     = runCommandBase + ' -e ' + 'Exec_%d' % i
#   cmdParameters['date']           = time.asctime()  
#    TaskExecCmdFile                 = execDir + os.path.sep + 'TaskExec_%d.cmd' % i
#    r  = Template(cmdFileContents)
#    cmdFile = r.safe_substitute(cmdParameters)
#    cmdFile = cmdFile.replace("\r\n", "\n")
#   f = open(TaskExecCmdFile, "wb")
#    f.write(cmdFile)
#    f.close()
#
#   Submit a batch job to the cluster  
#   
#    print "Starting Exec # ", i
#    clusterSubmitCommand = '/usr/local/sgeb/bin/glinux/qsub'
#    clusterCommand       = clusterSubmitCommand + ' ' + TaskExecCmdFile
#    clusterCommand       = clusterCommand.split(' ')
#    os.spawnv(os.P_NOWAIT,clusterSubmitCommand,clusterCommand)
#    print "Executing: ",
#    for j in range(len(clusterCommand)):
#      print clusterCommand[j],
#    print ' '
#
# Sleep a rundome amount of time before submitting 
# the next job
# sleepTime = 1.0 + 5.0*random.random()
# time.sleep(sleepTime)