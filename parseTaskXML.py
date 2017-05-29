import xml.dom.minidom
import random
import math
import sys
from copy import deepcopy

class parseTaskXML(object):
  """
  This class contains methods to perform operations associated on
  the XML file specifying the computational tasks. 
  
  """
  
  def __init__(self):
    self.taskList = []
    
  def getTypedValue(self,val='',valType=''):
    """
    This code returns a typed value associated with a unicode input
    string. The type is either explicitly specified, or determined by
    examining the string. 
    """ 
    if(val != ''):
      if(valType  == ''):
        s = str(val.strip())
        s = ''.join(s.split('+'))
        s = ''.join(s.split('-'))
        if s.isdigit():
          return int(val)
        s = ''.join(s.split('.'))
        s = ''.join(s.split('e'))
        s = ''.join(s.split('E'))
        if s.isdigit() : 
          return float(val)
        else: 
          return val.strip()
      else:
        if(valType =='int'):
          return int(val)
        elif(valType =='long'):
          return int(val)
        elif(valType =='float'):
          return float(val)
        elif(valType =='double'):
          return float(val)
        elif(valType =='string'):
          return val.strip()
    else:
      if(valType =='int'):
        return int(0)
      elif(valType =='long'):
        return int(0)
      elif(valType =='float'):
        return float(0.0)
      elif(valType =='double'):
        return float(0.0)
      elif(valType =='string'):
        return ''

  def hasChildNode(self,element,childName):
    """
    This routine determines of the element has a child node
    with a specified name. 
    """
    for child in element.childNodes:
      if(child.nodeName == childName): return True
    return False

  '''
  <!-- -->
  <!-- Sample task specification for random task generation -->
  <!-- -->

  <taskRanges>
  <Random>
    <distribution> chebyshev </distribution>   <!-- uniform, chebyshev --> 
    <sampleSize value = "10"  />
    <seed value = "314257879" />
  </Random>
  <BoxGate>
    <min>  -0.1 </min>
    <max>   0.0 </max>
  </BoxGate>
  <RightDot>
    <min>  -0.2</min>
    <max>   0.0 </max>
  </RightDot>
  <LeftDot>
    <min>  -0.4 </min>
    <max>   0.0 </max>
    </LeftDot>
  </taskRanges>
  
  '''
  def getRandomTaskList(self,taskRangesElement,runParameters,paramList):
  
    # Check to see if the parameter task list specifies a random task generation 
    
    continueFlag = False
    for taskElement in taskRangesElement.childNodes :
      if (taskElement.nodeType is xml.dom.Node.ELEMENT_NODE) and (taskElement.nodeName == "Random"):
        continueFlag = True
        
    if(continueFlag == False) : return False
    
    
    pMin = {}
    pMax = {}
    
    for taskElement in taskRangesElement.childNodes :
      if (taskElement.nodeType is xml.dom.Node.ELEMENT_NODE) and (taskElement.nodeName == "Random"):
        
        if(len(taskElement.getElementsByTagName('distribution')) == 0):
          distributionType = "uniform"
          
        else:  # For  <distribution value = "distributionType" />
          distributionElement = taskElement.getElementsByTagName('distribution')[0]
    
          if(len(distributionElement.getAttribute('value')) != 0):
            distributionType = (distributionElement.getAttribute('value')).strip()
            #print((distributionElement.getAttribute('value')).strip())
            
          else: # For <distribution> distributionType </distribution>
            distributionType = (distributionElement.childNodes[0].nodeValue).strip()
            #print((distributionElement.childNodes[0].nodeValue).strip())
          #
          # Check for supported specification of distribution 
   

        if( (distributionType.lower() != u'uniform'.lower()) and (distributionType.lower() != u'chebyshev'.lower())):
          print('parseTaskXML Error:')
          print('Unsupported random distribution')
          print('Distribution specified : ' + distributionType)
          print("Only uniform and chebyshev distributions currently specified")
          exit()
          
          
        if(len(taskElement.getElementsByTagName('sampleSize')) == 0):
          print('parseTaskXML Error:')
          print('sampleSize parameter not specified')
          exit()
        else:
          sampleSizeElement = taskElement.getElementsByTagName('sampleSize')[0]
          
          if(len(sampleSizeElement.getAttribute('value')) != 0): # For  specification <sampleSize value = "10" />
            sampleSize = self.getTypedValue((sampleSizeElement.getAttribute('value')).strip())
            #print(self.getTypedValue((sampleSizeElement.getAttribute('value')).strip()))
            
          
          else: # For specification <sampleSize> 1000 </sampleSize>
            sampleSize = self.getTypedValue((sampleCountElement.childNodes[0].nodeValue).strip())
            #print(self.getTypedValue((sampleCountElement.childNodes[0].nodeValue).strip()))
            
        if(len(taskElement.getElementsByTagName('seed')) == 0):
          intseed = 12876589876532
        else:
          seedElement = taskElement.getElementsByTagName('seed')[0]
          
          if(len(seedElement.getAttribute('value')) != 0):  # For  <seed value = "1234" />
            intSeed = self.getTypedValue( (seedElement.getAttribute('value')).strip())
            #print(self.getTypedValue( (seedElement.getAttribute('value')).strip()))
          
          else:   # For <seed> 1234 </seed>
            intSeed = self.getTypedValue((seedElement.childNodes[0].nodeValue).strip())                                           
            #print(self.getTypedValue((seedElement.childNodes[0].nodeValue).strip()))

                 
      if (taskElement.nodeType is xml.dom.Node.ELEMENT_NODE) and (taskElement.nodeName in runParameters):
        # Add parameter name that is being varied by assignment to a null value
        paramList[taskElement.nodeName] = ''
        
        # 
 
        for childElement in  taskElement.childNodes:
          if (childElement.nodeType is xml.dom.Node.ELEMENT_NODE):
            if(childElement.nodeName == 'min'):
                if(len(childElement.getAttribute('value')) == 0) :
                  pMinVal = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
                else:
                  pMinVal = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
            if(childElement.nodeName == 'max'): 
              if(len(childElement.getAttribute('value')) == 0) :
                pMaxVal = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
              else:
                pMaxVal = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
                
        pMin[taskElement.nodeName]  = pMinVal
        pMax[taskElement.nodeName]  = pMaxVal
    
    print("")
    print("-- Random Task List -- \ndistribution : " + distributionType)
    print("sampleSize   : " + "{0:d}".format(sampleSize))
    print("seed         : " + "{0:d}".format(intSeed))
    print("")
                
    randomGenArray = {}
    random.seed(intSeed)
    
    # Create random number generators for each component, using a random seed that
    # is created by the shared random number generator 
    
    for p in paramList :
      seed = random.randint(0,sys.maxint)
      randomGenArray[p] = random.Random()
      randomGenArray[p].seed(seed)
      
    # Create random data using a uniform distribution 
    
    if(distributionType.lower() == u'uniform'.lower()):
      for i in range(0,5):
        runP = deepcopy(runParameters)
        for p in paramList :
          runP[p] = randomGenArray[p].uniform(pMin[p],pMax[p])
        self.taskList.append(runP)
        
    # Create random data using  a chebyshev distribution 
       
    if(distributionType.lower() == u'chebyshev'.lower()):
      for i in range(0,sampleSize):
        runP = deepcopy(runParameters)
        for p in paramList :
          pVal    = randomGenArray[p].uniform(0.0,1.0)
          pVal    = math.sin((pVal - 0.5)*3.14159265358979323846)
          runP[p] = pMin[p] + 0.5*(pMax[p] - pMin[p])*(pVal + 1.0)
        self.taskList.append(runP) 
            
    # Create random data using a chebyshev distribution 

    return True
    

  def getMinMaxLoopList(self,parameter, element):
    """
    Creates the list of parameters and associated values 
    specified by the range from a min value to a max value
    using either a specified number of intervals or 
    a specified increment

    One can specify the ranges or increments using the syntax 
    similar to:

    <min> 5 </min> or <min value ="5" /> 

    In the first case, the value is specified as the value of the first childNode 
    and the second case the value is specificed as an attribute of the tag. 
    """
    valuesList = []
    pMin       = 0
    pMax       = 0
    intervals = 0
    increment = 0
    for childElement in  element.childNodes:
      if (childElement.nodeType is xml.dom.Node.ELEMENT_NODE):
        if(childElement.nodeName == 'min'):
          if(len(childElement.getAttribute('value')) == 0) :
            pMin = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
          else:
            pMin = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
        if(childElement.nodeName == 'max'): 
          if(len(childElement.getAttribute('value')) == 0) :
            pMax = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
          else:
            pMax = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
        if(childElement.nodeName == 'intervals'): 
          if(len(childElement.getAttribute('value')) == 0) :
            intervals = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
          else:
            intervals = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
        if(childElement.nodeName == 'increment'): 
          if(len(childElement.getAttribute('value')) == 0) :
            increment = self.getTypedValue(childElement.childNodes[0].nodeValue,\
                            childElement.getAttribute('type'))
          else:
            increment = self.getTypedValue(childElement.getAttribute('value'),\
                            childElement.getAttribute('type'))
    if(intervals != 0):
      dP = float(pMax - pMin)/float(intervals)
      for i in range(intervals+1):
        pVal = pMin + float(i)*dP 
        Values = {}
        Values[parameter]=pVal
        valuesList.append(Values)
    else:
      i = pMin
      while(i <= pMax):
        Values = {}
        Values[parameter]=i
        valuesList.append(Values)
        i += increment
    return valuesList 
   
  def getValuesLoopList(self,parameter,element):
    """
    Creates a list of parameter names and associated values
    specified by the <values> tag associated with a particular
    parameter.
    """
    valuesList = []
    for childElement in  element.childNodes:
      if (childElement.nodeType is xml.dom.Node.ELEMENT_NODE):
        if(childElement.nodeName == 'value'): 
          Values = {}
          Values[parameter]=self.getTypedValue(childElement.childNodes[0].nodeValue,\
                                        childElement.getAttribute('type'))
          for valueElement in childElement.childNodes:
              if (valueElement.nodeType is xml.dom.Node.ELEMENT_NODE):
                Values[valueElement.nodeName] = self.getTypedValue(valueElement.getAttribute('value'),\
                                                            valueElement.getAttribute('type'))
          valuesList.append(Values)
    return valuesList
   
  
  def generateTasks(self,taskElement,runParameters,taskCount,paramList):
    """
    This is a recursive function that generates tasks with respect to the
    parameter specified (by looking for the parameter limits, or collections
    of values) and recursively calls itself for any subparameters. The upshot
    is that the nesting of the xml parameter entries give rise to an 
    equivalent loop nesting of the tasks.  
    """
    if (taskElement.nodeType is xml.dom.Node.ELEMENT_NODE) and \
       (taskElement.nodeName in runParameters):
      valuesList = {}
      if(self.hasChildNode(taskElement,'min'))   : 
        valuesList = self.getMinMaxLoopList(taskElement.nodeName,taskElement)
      elif(self.hasChildNode(taskElement,'value')): 
        valuesList = self.getValuesLoopList(taskElement.nodeName,taskElement)
    #
    # Loop over parameter values
    #
      for i in valuesList: 
        for k in i.keys(): 
          runParameters[k]=i[k]
          if(not (k in paramList)): paramList[k] = ''
        hasSubParameter = False
        for childElement in  taskElement.childNodes:
          if (childElement.nodeType is xml.dom.Node.ELEMENT_NODE):
            if(childElement.nodeName in runParameters): 
              hasSubParameter = True
              self.generateTasks(childElement,runParameters,taskCount,paramList)
        if(hasSubParameter == False):
          taskCount[0]  +=1
          runP = deepcopy(runParameters)
          self.taskList.append(runP)
          #
          # call the task builder to set the tasks
          #
          #s= '%3d' % taskCount[0]
          #print s,  
          #for k in paramList.keys():
          #  print ' ',k,' ',runParameters[k], 
          #print ' '
      #
      # End of loop list 
      #
  def createDictFromXMLelement(self,dictElement):
    """
    This routine creates a python dictionary from an XML
    element that contains child nodes whose names specify a dictionary
    key and whose attribute "value" specifies the value. The
    type of the attribute value is either determined from it's
    format (e.g. a number containing a decimal point is cast to a float)
    or can be specified explicitly by setting the attribute "type".
    """
    dictArray = {}
    for dataVal in dictElement.childNodes:
      if(dataVal.nodeType is xml.dom.Node.ELEMENT_NODE):
        dictArray[dataVal.nodeName] = self.getTypedValue(dataVal.getAttribute('value'),\
                                                         dataVal.getAttribute('type'))
    return dictArray
       

