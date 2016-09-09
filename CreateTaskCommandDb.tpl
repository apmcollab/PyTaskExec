<?xml version="1.0" encoding="iso-8859-1"?>
<TaskBuilderData>

<jobData> 
<executableCommand   value = "$executableCommand"  />
<runFileName         value = "TaskCommand" />
<runFileTemplate     value = "$CreateTaskCommandTemplate"  type = "file" /> 
<taskTable           value = "$taskTable" />
<taskDirectory       value = "$taskDirectory" />
$outputHandlerScript
</jobData>

<runParameters>
<command  value = "command" />
</runParameters>

<outputData>
<clockTime value = "-1.0" />
</outputData>

</TaskBuilderData>


