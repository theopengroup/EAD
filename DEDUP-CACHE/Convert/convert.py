#! /usr/bin/python
from pybloomfilter import BloomFilter

import hashlib
import sys, os
import operator
  


def main():
   #Check for command line arguments
   if len(sys.argv) != 2:
      print 'Usage: %s [trace file]' % os.path.basename(sys.argv[0])
      sys.exit(1)

   #Read arguments from command line
   inFile = sys.argv[1]


   bf1 = BloomFilter(100000000, 0.001, 'bf1')   
   bf2 = BloomFilter(100000000, 0.001, 'bf2')
     
   outputFileName="converted-"+sys.argv[1]
   f = open(outputFileName, "a")



   for line in open(inFile,'r'):
      if (line[0:2]=="W," or line[0:2]=="R,"):
         hash1=int(hashlib.sha1(line[2:]).hexdigest(), 16) % (10 ** 10)
         hash2=int(hashlib.md5(line[2:]).hexdigest(), 16) % (10 ** 10)
         if (bf1.add(hash1) and bf2.add(hash2)):
         	f.write('%s,%d\n' % (line[0],hash1*10000) )
         else:
        	   f.write('%s,%d\n' % (line[0],hash2*10000) )  
      elif(line==''):
         break
      else:
         pass
   f.close()


if __name__ == '__main__':
   main()
   
   
   
   
