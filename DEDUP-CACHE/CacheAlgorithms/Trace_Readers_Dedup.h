#include <iostream>
#include <vector>
#include <fstream>
#include <climits>


using namespace std;

void readDedupLine(string currentIOLine, long long & startPageNumber, long long & endPageNumber, int & readWriteFlag)
{
    string startStr;

    
    
    // [W,hash,offset=4096]
    
    // scan whole line for "write"
    if (currentIOLine[0]=='W')
        readWriteFlag=0; // Write
    else if (currentIOLine[0]=='R')
        readWriteFlag=1; // Read
    
    
    startStr  = currentIOLine.substr(2);
    
    startPageNumber   = atoll(startStr.c_str())/512;
    startPageNumber   = startPageNumber/8;
   
    
    
    endPageNumber     = (   atoll(startStr.c_str())   /  512   +cacheLineSize/512-1)/8;

    
}

