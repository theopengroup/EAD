#include "NoCache.h"

NoCache::NoCache()
{
    _writeCounter=0;
    _readCounter=0;
}



bool NoCache::input(long newPageData, int readWriteFlag)
{
    if (readWriteFlag==1)
    {
        _readCounter++;
    }
    else
    {
        _writeCounter++;
    }
    return true;
}



long NoCache::getWriteCounter()
{
    
    return _writeCounter;
}


long NoCache::getReadCounter()
{
    
    return _readCounter;
}
