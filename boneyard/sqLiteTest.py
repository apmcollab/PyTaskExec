
from sqLite import sqLite
from sqLite import sqLiteError


def main(): 
    sqDB = sqLite("Test.db")
    sqDB.verbose = True
    sqDB.createConnection()
    sqCommand = 'create table Test (name, rank int);'
    try:
      sqDB.execute(sqCommand)
    except sqLiteError, e:
      print e.message
      
    
    sqCommand = 'Insert into Test (name, rank) values (\'bob\',10);'
    try :
      print sqDB.executeWithLock(sqCommand)
    except sqLiteError, e:
      print e.message
      
    sqCommand = 'select * from Test;'
    try :
      print sqDB.execute(sqCommand)
    except sqLiteError, e:
      print e.message
      
    
    sqDB.closeConnection()
    
    
#
# Stub for invoking the main() routine in this file 
#   
if __name__ == '__main__':
    main()