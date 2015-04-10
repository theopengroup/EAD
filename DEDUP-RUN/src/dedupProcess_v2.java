/*this version is a modified version of dedupProcess
in this version, the dup chunk entries will still be added into the storage,
i.e. number of containers are as the same as segment number
*/
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

public class dedupProcess_v2
{
	// only count I/O access to cache, index, estimateBase
    Map<String, Long> cache; 		// RAM Cache (C, for comparison)
	Map<String, long[]> index; 		// RAM IndexTable (T, for cached index table)
	Map<String, long[]> estimateBase;		// RAM Base (B, for estimation)
	Map<Integer,Integer> containerRecord; 
    int M; //#of seg to be compared/loaded, here is 1
	long totalChunkNum;
    int chunkSize;
    int segSize;
    long chunkNum;
    String[] hashRecord;
    int[] uniqueRecord;
    long[] containerTrack; //used for record in which container those dup chunks counterparts are
    int[] sampledRecord;
//	 int segNo;
    File sampledHash; //the input file, trace file
    File bloomfilterRecord;
	File[] containers;
    BloomFilter<String> bf;
	long containerNo;
	String storageFileFolder;
    long dup;
    long total;
	static int[] chunksizeRecord ;
	int sampleNumPerSeg ;
	long indexSize;
	int segAmount;
	boolean consecutiveDedup;
	int boundaryCheck;
	int sampleRate;
    
    // 0 - X
    dedupProcess_v2(Map<String, long[]> index, long indexSize, int M, Map<String, Long> cache,
                    int chunkSize, int segSize, String[] hashRecord, int[] uniqueRecord,int[] sampledRecord,
                    long[] containerTrack,String storageFileFolder,BloomFilter<String> bf,File bloomfilterRecord,
                    File sampledHash, Map<Integer, Integer> containerRecord, Map<String, long[]> estimateBase,
                    int sampleNumPerSeg, int  segAmount, boolean consectutiveDedup,int sampleRate)
    {
		this.sampledHash = sampledHash;
		this.index = index;
		this.indexSize = indexSize;
		this.M = M;  // //number of containers to be tracked in the index (num of segments to be compared)
		this.cache = cache;
		this.chunkSize =chunkSize;
		this.segSize = segSize;
		this.hashRecord = hashRecord;
		this.uniqueRecord = uniqueRecord;
		this.sampledRecord = sampledRecord;
		this.containerRecord = containerRecord;
		this.containerTrack = containerTrack;
		this.bf = bf;
		this.bloomfilterRecord = bloomfilterRecord;
//		this.segNo = segNo;
		this.chunkNum = segSize*1024/chunkSize;
		this.storageFileFolder = storageFileFolder;
        chunksizeRecord = new int[(int) chunkNum];
		 
        this.estimateBase = estimateBase;
        this.sampleNumPerSeg  = sampleNumPerSeg ;
        containers = new File(storageFileFolder).listFiles();
		 
        this.segAmount = segAmount;
        this.consecutiveDedup = consectutiveDedup;
        this.sampleRate = sampleRate;
	}
	
    
    
    // 1 - Y
    void dedup() throws IOException
    {
		 /*
		  * Load the input file into the index, 
		  * initialize the uniqueRecord,sampledRecord,hashRecord
		  * if sampled (in IndexTable), compare it to the index:match? extract FP from that container
		  */
		
        //
		Scanner loadIn;
		loadIn = new Scanner(sampledHash); // input trace file=sampledHash
		loadIn.nextLine();
		int i = 0;
        // read each line chunk in the traceFile
		while(loadIn.hasNextLine())
        {
            totalChunkNum++;
            // * curLine: infor *
            String[] infor = loadIn.nextLine().split(",");
            // System.out.println("the current i is: " + i); // i = lineNum
            // put the hashvalue into the RAM for the following comparison
            hashRecord[i] = infor[4];
            chunksizeRecord[i] = Integer.parseInt(infor[3]);
            total += Long.parseLong(infor[3]);
            long hashvalue = Long.parseLong(infor[4].substring(30),16);
            
            //
            if(check(hashvalue))
            {
                sampledRecord[i] = 1;
                // System.out.println("the key(hashvalue) is: " + infor[4]);
                
                // if index table T contains this hash
                // chunk hash is found in the cached indextable
                if(index.containsKey(infor[4]))
                {
                    long[] meta = new long[M+2];
                    meta = index.get(infor[4]);
                    meta[0]++; //counter value updates
                    int m = 1;
                    while(m<=M) //M=1 //number of containers to be tracked in the index (num of segments to be compared)
                    {
                    	if(meta[m]==0)
                    	{
                        	meta[m] = containers.length+1;
                        	break;
                    	}
                    	m++;
                	}
                
                    // Fetch segment meta info from disk to ram
                    /*
                     * Here we can set a threshold to limit the cache size!!
                     */
                    // cache size 
                    // M = 1 (#segment to load)
                    for(int ii = 1; ii <= M; ii++)
                    {
                        if(!containerRecord.containsKey(meta[ii]) && meta[ii]!= containers.length+1 && meta[ii]!=0)
                        {
                        	// DISK->RAM[containerRecord]
                            storageLoad(meta[ii]); // <- DISK to RAM, calculated in storageLoad function
                            // extract FPs from specified container
                            containerRecord.put((int) meta[ii], 1);
                            System.out.println("CR,"+meta[ii]+','+1);
                        }
                    }
				
                    index.put(infor[4],meta); //update the index
                    // not from disk

                }
            }
            //
            else  // if check(hashvalue)!
            {
                sampledRecord[i] = 0;
            }
            i++;
        }
		
		/*
		 * Put the samples into the sample base (B)
		 */
		for(int count = 1; count <= sampleNumPerSeg; count++)
        {
            int randomNum =(int)(Math.random()*(chunkNum-1)); // randomly pick a chunk from the segment as est sampleBaseChnk
            if(hashRecord[randomNum] != null && !estimateBase.containsKey(hashRecord[randomNum])) // if it exists and not ested
            {
                long estMeta[] = new long[2];
                estMeta[0] = 1;
                estMeta[1] = 0;
                estimateBase.put(hashRecord[randomNum], estMeta);
                // not from disk
            }
            else if(hashRecord[randomNum] == null)
            {
                count -- ;  //maybe the random num indicates the empty record because that the segment is not full
            }
            else if(hashRecord[randomNum] != null && estimateBase.containsKey(hashRecord[randomNum]))
            {
                long estMeta[] = new long[2];
                estMeta = estimateBase.get(hashRecord[randomNum]);
                estMeta[0]++;
                estimateBase.put(hashRecord[randomNum], estMeta);
                // not from disk
            }
		}
		
		
		/*
		 * deduplication process
		 */
		// hashRecord is new segment's hash record
		for(int k = 0; k < hashRecord.length;k++)
        {
			// estimation [ignore]
			if(estimateBase.containsKey(hashRecord[k]))
            {
				long estMeta[] = new long[2];
				estMeta = estimateBase.get(hashRecord[k]);
				estMeta[1] ++;
				estimateBase.put(hashRecord[k], estMeta);
			}
			// * dedup , for each chk's hash in the new segment (hashRecord)
			if(cache.containsKey(hashRecord[k])) // found in cache
            {
				dup+=chunksizeRecord[k];
				uniqueRecord[k] = 0; // not unique
				containerTrack[k] = cache.get(hashRecord[k]);   // disk ?
			}
            else // not found in cache
            {
				uniqueRecord[k] = 1;
				cache.put(hashRecord[k], (long)0); // for new chunks, update the cache 
				containerTrack[k] = 0;
			}
		}
//		System.out.println("The total data is: "+total);
//		System.out.println("The dup is: "+dup);
	 }
	 
		
    // 2 - Y
    // write the info into the storage, also put sample info to the index
    // RAM -> DISK (writeln)
    // DISK -> RAM (storageLoad)
	void storageWrite() throws IOException
    {
		//  visit the storage
		//	System.out.println("The limit on index size is: " + indexSize);
		//  print header
		PrintWriter writeIn ;
		if(containers.length == 0)
		{
			containerNo = 1;
			writeIn = new PrintWriter(new File(storageFileFolder + "/" + containerNo));
			//	writeIn.flush();
			writeIn.print("Seg No");
			writeIn.print(",");
			writeIn.print("File Name");
			writeIn.print(",");
			writeIn.print("Chunk No");
			writeIn.print(",");
			writeIn.print("Chunk Size");
			writeIn.print(",");
			writeIn.print("Hash Value");
			writeIn.print(",");
			writeIn.print("Sample");
			writeIn.print(",");
			writeIn.print("HittingCounter");
			writeIn.print(",");
			writeIn.println("containerTrack");
		}
		else
		{
			//locate the current available container
			containerNo = containers.length+1;		
			writeIn = new PrintWriter(new File(storageFileFolder + "/" + containerNo));
			//	writeIn.flush();
			writeIn.print("Seg No");
			writeIn.print(",");
			writeIn.print("File Name");
			writeIn.print(",");
			writeIn.print("Chunk No");
			writeIn.print(",");
			writeIn.print("Chunk Size");
			writeIn.print(",");
			writeIn.print("Hash Value");
			writeIn.print(",");
			writeIn.print("Sample");
			writeIn.print(",");
			writeIn.print("HittingCounter");
			writeIn.print(",");
			writeIn.println("containerTrack");
		}				
		Scanner dataIn = new Scanner(sampledHash);		// dataln is the trace file line
		dataIn.nextLine();
		int i = 0;
		while(dataIn.hasNextLine())
		{
			if(uniqueRecord[i]==1)
			{
				String[] infor = dataIn.nextLine().split(","); // infor is from trace, write to disk
				writeIn.print(infor[0]);
				writeIn.print(",");
				writeIn.print(infor[1]);
				writeIn.print(",");
				writeIn.print(infor[2]);
				writeIn.print(",");
				writeIn.print(infor[3]);
				writeIn.print(",");
				writeIn.print(infor[4]);
				writeIn.print(",");			
				
				System.out.print("WRITE_TO_DISK,"+infor[0]+','+infor[1]+','+infor[2]+','+infor[3]+','+infor[4]); //yang==========
				
				if(sampledRecord[i] == 1)
				{
					writeIn.print(infor[4]);//5th: sample tag
					writeIn.print(",");
					writeIn.print(0);
					System.out.print(','+infor[4]+",0"); //yang===============
					
					//					System.out.println("the current index size is: " + index.size());
					if(!bf.contains(infor[4]))
					{
						// index size 
						if((indexSize!=-1 && index.size()<indexSize) ||(indexSize==-1))
						{ 
							// only put the FPs that are not existing in the BF into the index
							//	System.out.println("the current index size is: " + index.size());
							long[] metaData = new long[M+2];
							metaData[0] = 0;
							metaData[1] = containerNo;
							metaData[M+1] = Long.parseLong(infor[0]);
							index.put(infor[4],metaData); 	//update the index
							// not from disk-----------trace file line to index
						}
					}
					
					writeIn.print(",");
//					writeIn.println(containerTrack[i]);
					writeIn.println(containerNo);
					System.out.println(','+containerNo); //yang=============
				}
				else
				{
					writeIn.print("NotSampled"); //5th: sample tage	
					writeIn.print(",");
					writeIn.print("0"); //6th: hitting counter
					writeIn.print(",");
//					writeIn.println(containerTrack[i]);
					writeIn.println(containerNo);
					System.out.println(",NotSampled,0,"+containerNo); //yang===========
				}
					
			}
			else if(uniqueRecord[i]==0)
			{ 
				//process on dup chunks
				String[] infor = dataIn.nextLine().split(",");
				writeIn.print(infor[0]);
				writeIn.print(",");
				writeIn.print(infor[1]);
				writeIn.print(",");
				writeIn.print(infor[2]);
				writeIn.print(",");
				writeIn.print(infor[3]);
				writeIn.print(",");
				writeIn.print(infor[4]);
				writeIn.print(",");
				if(sampledRecord[i] == 1)
				{
					writeIn.print("Dup"+infor[4]);//5th: sample tag
					System.out.print("WRITE_TO_DISK,"+infor[0]+','+infor[1]+','+infor[2]+','+infor[3]+','+infor[4]); //yang===========
				}
				else
				{
					writeIn.print("DupNotSampled");	
					System.out.print(",DupNotSampled"); //yang===========
				}
				writeIn.print(",");
				writeIn.print("0"); //6th: hitting counter
				writeIn.print(",");
//				writeIn.println(containerTrack[i]);
				writeIn.println(containerNo);
				System.out.println(",0,"+containerNo); //yang===========
			}
			else
			{
					System.out.println("error with writing data to storage");
			}
			i++;
		}
		writeIn.close();
	}

    // 3 - X
    long getTotalChunkNum()
    {
			return totalChunkNum;
    }
	
	// 4 - X
    long getTotal()
    {
        return total;
    }
		
    // 5 - X
    long getDup()
    {
        return dup;
    }
		

    // 6 - X
    long getCacheSize()
    {
        return cache.size();
    }
		
		
    // 7 - X
    // count the total number of lines(except the 1st line)
    static long getTotalLines(String file) throws IOException
    {
	    Scanner in = new Scanner(new File(file));
	    long lines = 0;
	    while(in.hasNextLine())
        {
	    	lines++;
	    	in.nextLine();
	    }
	    in.close();
	    return lines;
    }
		
    
    // 8 - X
    /*
    * save the index and estimateBase when we need to change the sampling rate
    * save them into the folder 'testData/indexCheckpoint'
    */
	void checkPoint() throws FileNotFoundException
    {
		/*
		 * save the index
		 */
		PrintWriter indexSave = new PrintWriter(new File("testData/indexCheckpoint/index"));
		Iterator<Entry<String, long[]>> iter = index.entrySet().iterator();
		while(iter.hasNext())
        {
			Entry<String, long[]> entry= iter.next();
			indexSave.print(entry.getKey());  //record the key value
			long indexMeta[] = new long[M+2];
			indexMeta = entry.getValue();
			for(int i=0; i<M+2;i++)
            {			//record all the other values in one entry
				indexSave.print(',');
				indexSave.print(indexMeta[i]);
			}
			indexSave.println();
        }
			
		/*
		 * save the estimateBase
		 */
		PrintWriter estimateSave = new PrintWriter(new File("testData/indexCheckpoint/estimateBase"));
		iter = index.entrySet().iterator();
		while(iter.hasNext())
        {
			Entry<String, long[]> entry= iter.next();
			estimateSave.print(entry.getKey());  //record the key value
			long estimateMeta[] = new long[2];
			estimateMeta = entry.getValue();						
			estimateSave.print(',');//record all the other values in one entry
			estimateSave.print(estimateMeta[0]);
			estimateSave.print(',');
			estimateSave.print(estimateMeta[1]);
			estimateSave.println();
		}
    }
	
    
    // 9 - Y 
	void loadCheckpoint() throws FileNotFoundException
    {
		/*
		 * load index info back into the index from the checkpoint file
		 * DISK->RAM
		 */
		Scanner indexSave = new Scanner(new File("testData/indexCheckpoint/index"));	
		while(indexSave.hasNextLine())
        {
			String indexEntry[] = indexSave.nextLine().split(",");
			String key = indexEntry[0];
			long[] indexMeta = new long[M+2];
			indexMeta[0] = Long.parseLong(indexEntry[1]);
            for(int i = 1; i <=M+1; i++)
            {
				indexMeta[i] = Long.parseLong(indexEntry[i+1]);
			}
			index.put(key, indexMeta);
			System.out.println("T"+','+key+','+indexMeta); // yang
		}

		/*
		 * load estimateBase info back into the estimateBase from the checkpoint file
		 */
		Scanner estimateSave = new Scanner(new File("testData/indexCheckpoint/estimateBase"));	
		while(estimateSave.hasNextLine())
        {
			String estimateEntry[] = indexSave.nextLine().split(",");
			String key = estimateEntry[0];
			long[] estimateMeta = new long[2];
			estimateMeta[0] = Long.parseLong(estimateEntry[1]);
			estimateMeta[1] = Long.parseLong(estimateEntry[2]);
			index.put(key, estimateMeta);
			System.out.println("T"+','+key+','+estimateMeta);//yang
		}
	}
    
    
	// 10 - Y
    // load the info into the memory, DISK->RAM[IndexTable]
	void storageLoad() throws IOException
    {
		/*
		 * Load the chunk's info to the index
		 */	
		Scanner loadIn;
		File[] containers = new File(storageFileFolder).listFiles();
		if(containers.length == 0)
        {
			System.out.println("The storage is empty");
		}
        else
        {
        	// iterate among files in storageFileFolder
			for(File file:containers)
            {
				loadIn = new Scanner(file);
				loadIn.nextLine();
				while(loadIn.hasNextLine())
                {
                    String[] infor = loadIn.nextLine().split(",");
                    long hashvalue = Long.parseLong(infor[4].substring(33),16);
                    if(check(hashvalue) && ((indexSize!=-1 && index.size()<indexSize) || (indexSize==-1)))
                    {
                        long[] metaData = new long[M+2];
                        metaData[0] = Long.parseLong(infor[6]);
                        metaData[1] = Long.parseLong(file.getName());
                        metaData[M+1] = Long.parseLong(infor[0]);
                        index.put(infor[5], metaData );
                        System.out.println('T'+','+infor[5]+','+ metaData); // yang
                    }
				}
			}
		}
    }
		
    
	// 11 - Y
    // load FPs in specified container, DISK->RAM[cache]
	void storageLoad(Long containerNo) throws FileNotFoundException
    {
        Scanner loadIn;
        File theContainer = new File(storageFileFolder+"/"+containerNo);
        loadIn = new Scanner(theContainer);
        loadIn.nextLine();
        while(loadIn.hasNextLine())
        {
            String[] infor = loadIn.nextLine().split(",");
			cache.put(infor[4], containerNo); // entries in cache have their container numbers
			//The chunk hash loaded into RAM(cache)
			 System.out.println("The chunk hash loaded into RAM(cache): "+infor[4]);
			 resultRecord.println("The chunk hash loaded into RAM(cache): "+infor[4]);
			 //--------------yufeng
			System.out.println('C'+','+infor[4]+','+ containerNo); // yang
        }
    }
		
	// 12 - X ???
    // method to check the lower n bits of fingerprint to determine boundary
	private boolean check(long fingerprint)
    {
		boundaryCheck = (int) (Math.log(sampleRate) / Math.log(2));
	    int i = 0;
	    boolean check = true;
	    do
        {
	        check = (fingerprint & (1L << i)) == 0;
	        if (!check)
            {
	            return false;
	        }
	        i++;
	    }
        while (i < boundaryCheck);
	    return true;
	}
	

}

