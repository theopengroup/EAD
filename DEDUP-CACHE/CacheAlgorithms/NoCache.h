#ifndef _NoCache_
#define _NoCache_

#include <iostream>


#include "Config.h"
using namespace std;



class NoCache
{
public:
    NoCache();
    bool                input(long newPageData, int readWriteFlag);   // hit=true, miss=false
    long                getWriteCounter();
    long                getReadCounter();
  
    
private:
   
    long                _writeCounter;
    long                _readCounter;

    
};

#endif



