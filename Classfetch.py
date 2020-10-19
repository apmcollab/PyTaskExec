############################################################################
##                
## classfetch.py :renamed from classloader.py to prevent namespace problems 
##                
############################################################################
#
# How to import:
#
# from package.path.to.classfetch import _get_func, _get_class
#
# See below for an example of its usage
#
# Reference:
#
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/223972


import sys, types

def _get_mod(modulePath):
    try:
        aMod = sys.modules[modulePath]
        if not isinstance(aMod, types.ModuleType):
            raise KeyError
    except KeyError:
        # The last [''] is very important!
        aMod = __import__(modulePath, globals(), locals(), [''])
        sys.modules[modulePath] = aMod
    return aMod

def _get_func(fullFuncName):
    """Retrieve a function object from a full dotted-package name."""
    
    # Parse out the path, module, and function
    lastDot = fullFuncName.rfind(u".")
    funcName = fullFuncName[lastDot + 1:]
    modPath = fullFuncName[:lastDot]
    
    aMod = _get_mod(modPath)
    aFunc = getattr(aMod, funcName)
    
    # Assert that the function is a *callable* attribute.
    assert callable(aFunc), u"%s is not callable." % fullFuncName
    
    # Return a reference to the function itself,
    # not the results of the function.
    return aFunc

def _get_class(fullClassName, parentClass=None):
    """Load a module and retrieve a class (NOT an instance).
    
    If the parentClass is supplied, className must be of parentClass
    or a subclass of parentClass (or None is returned).
    """
    aClass = _get_func(fullClassName)
    
    # Assert that the class is a subclass of parentClass.
    if parentClass is not None:
        if not issubclass(aClass, parentClass):
            raise TypeError(u"%s is not a subclass of %s" %
                            (fullClassName, parentClass))
    
    # Return a reference to the class itself, not an instantiated object.
    return aClass


"""
#                           Example  
#
# Loading the class SampleClass by name and using it. It is assumed
# that the class SampleClass is defined in the file SampleClass.py
# (Java convention)
#
# Exception handling is used when the class fails to exist or
# when a requested member function isn't defined. 
#
    sampleClassName       = "SampleClass.SampleClass"
    
    try:
        sampleClass      = _get_class(sampleClassName)
    except ImportError, exception:
        print 'Failed to load class' + sampleClassName
        print exception.message
        exit()
    except AttributeError, exception:
        print 'Failed to load class' + sampleClassName
        print exception.message
        exit()
        
    try:
        aClass = sampleClass(2,3)
    except TypeError, exception:
        print 'Instantation of ' + sampleClassName + ' failed'
        print exception.message
        exit()
       
    try:
        print aClass.fun(100)
    except AttributeError,excepton:
        print 'Error: ' + exception.message
        exit()
        
    try:
        print aClass.f(100)
    except AttributeError,exception:
        print 'Error: ' + exception.message
        exit()
        
######################################################################
#                 Contents of SampleClass.py
######################################################################
class SampleClass:
    def __init__(self,a,b):
        self.a = 100
        self.b = 10
        
    def fun(self,c):
        return self.a+self.b*c
#########################
"""
