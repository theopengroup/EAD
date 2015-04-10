import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;


public class resample {
/*
 * Apply the new sampling rate (newSampleRate)
 * locate the segment  (int[] segRecord: '0':not located, '1': located) to record the segment has been located
 * resample , update the hashseg files by adding "resampling Tag" column
 * update the new index, delete the correspondent entries in old one
 */

static long extraDup = 0;//record size of dups found because of resampling
static int chunkSizeRecord[];
	
File  dirFile ;	
int chunkSize;
int segSize;	
int samplingRate;
int newSamplingRate;
int threshold;
String[] hashRecord;
File bloomfilterRecord;
Map<String, long[]> index;
int M;
String storageFileFolder;  //the folder where the storagefiles is
String reSampleStorageFolder;
Map<String, long[]> newIndex;
Map<Integer,Integer> containerRecord;
BloomFilter<String> bf;
int evict=0;//evict entries
//File fileInfo; //File infor record
//File chunkInfo;//chunk infor record

static int entryCount; //count for entry number
static int lowHit ;//count for entries which have low hitting rate
static int delteEntry ;//count for the entries that have been removed
static long afterIndex = 0;
static long currentIndex = 0;

Map<String, Long> cache;
static Map<String, Integer> tempIndex = new HashMap<String, Integer>();	//record the old index
static Map<Long,Integer> segRecord = new HashMap<Long, Integer>(); //record which containers have been processed

resample(Map<String, long[]> index,int M,Map<String, Long> cache,Map<Integer, Integer> containerRecord,int samplingRate,int newSamplingRate,int threshold
		,int chunkSize, int segSize, BloomFilter<String> bf,File bloomfilterRecord,String storageFileFolder
		, String reSampleStorageFolder,	 String[] hashRecord,double evict){
	this.index = index;
	this.M = M;
	this.samplingRate = samplingRate;
	this.newSamplingRate = newSamplingRate;
	this.threshold = threshold;
	this.storageFileFolder = storageFileFolder;
//	this.reSampleStorageFolder = reSampleStorageFolder;
	this.cache = cache;
	this.containerRecord = containerRecord;
	this.hashRecord = hashRecord;
	this.bf = bf;
	this.bloomfilterRecord = bloomfilterRecord;
	this.chunkSize =chunkSize;
	this.segSize = segSize;
	 chunkSizeRecord = new int[(int) segSize*1024/chunkSize];
    this.dirFile = new File(reSampleStorageFolder+"_"+newSamplingRate);
     dirFile.mkdir();
     delteEntry = 1;
     entryCount = 0;
     newIndex = new HashMap<String, long[]>();
//	this.fileInfo = fileManage;	
//	this.chunkInfo = chunkInfo;
     this.evict = (int)evict;
     lowHit=0;
}

/*
 * after resampling, the container tables will be saved in a new folder for rededup
 */
void resampleProcess() throws FileNotFoundException{
	//Scan entries in the index
	java.util.Iterator<Entry<String, long[]>> iter = index.entrySet().iterator();

	long conNo ;
	int evictCount=0;
	while(iter.hasNext()){
		entryCount++;
		long meta[] = iter.next().getValue();	
		if(meta[0]<=threshold && evictCount < samplingRate*evict){ //pick out entries which have low hitting rate
			lowHit++;
			evictCount++;
			conNo = meta[1];
			if(!segRecord.containsKey(conNo)){//if this segment has not been processed				
					segRecord.put(conNo, 1);//record the container that has been resampled
					//locate the specific container
					File conToResample = new File(storageFileFolder+"/"+ conNo);
					Scanner inputStream = new Scanner(conToResample);	
					//use another folder to record the new sampled de segment
	            
					PrintWriter out = new PrintWriter(dirFile+"/"+ conNo);
					out.println("Segment No,FileName,ChunkName,ChunkSize,HashValue,reSampleTag,Hitting Rate, ContainerNo");	
					//start to resample, put old indexes into the temp hash record for the future use
					inputStream.nextLine();
					while(inputStream.hasNextLine()){
						String[] ary= inputStream.nextLine().split(",");
						out.print(ary[0]);
						out.print(",");
						out.print(ary[1]);
						out.print(",");
						out.print(ary[2]);
						out.print(",");
						out.print(ary[3]);
						out.print(",");
						out.print(ary[4]);
						out.print(",");		
					long hash = Long.parseLong(ary[4].substring(30),16);
					if(((ary[5].substring(0,3).equals("Not"))) && check(hash)){//if the chunk was not sampled, process it
															// otherwise, put it into the temp hash record
						out.print("Re"+ary[4]); 
	    				out.print(",");
	                    out.print((long)0);
	    				out.print(",");
	    				out.println(ary[7]);	
					}else if(ary[5].substring(0,6).equals("DupNot") && check(hash)){
						out.print("DupRe"+ary[4]); 
	    				out.print(",");
	                    out.print((long)0);
	    				out.print(",");
	    				out.println(ary[7]);
					}else{ //if(!ary[5].substring(0,3).equals("Not") || !ary[5].substring(0,6).equals("DupNot")){	
						out.print(ary[5]); 
	    				out.print(",");
	                    out.print(ary[6]);
	    				out.print(",");
	    				out.println(ary[7]);			
					}
					}
					out.close();
			}		
		}
	}

	
}



/*
 * use new storage files to rededup, calculate the new dedup rate
 */
void reDedup() throws IOException{
	cache.clear();
	 currentIndex = index.size();
	// scan all the modified containers
	File files[] = dirFile.listFiles();
	for (File file : files){
		boolean reclaim = false; //tag used for whether reclaim correspodent old entries in the old index
		Scanner containerContent = new Scanner(file);	
		containerContent.nextLine();
		int i = 0;
		while(containerContent.hasNextLine()){
			String line[] = containerContent.nextLine().split(",");

			hashRecord[i] = line[4];  //store the hashvalues

			if(line[5].substring(0, 3).equals("Not") || line[5].substring(0, 2).equals("Re")){
			chunkSizeRecord[i] = Integer.parseInt(line[3]) ;
			}else{
			chunkSizeRecord[i] = 0; 	
			}
			
			if(!line[5].substring(0,3).equals("Not") && !line[5].substring(0,2).equals("Re") && !line[5].substring(0,3).equals("Dup")){
			//old sample, put it into the tempindex; in the case they will be removed
				tempIndex.put(line[4], 0);// tempindex record the old index&container No	
			}else if(line[5].substring(0,2).equals("Re")){
			//new sample, do the dedup	
//				System.out.println("Now are processing file " + file.getName() +" line: " + i);
//				System.out.println("Hashvalue is " + line[4] +" samplingtag is: " + line[5]);
//				System.out.println("NewIndex size is: " + newIndex.size());
				if(newIndex.containsKey(line[4])){
					reclaim = true;  //old indexes should be removed
					long[] meta = new long[M+2];
					meta = newIndex.get(line[4]);
					meta[0]++; //counter value updates
					int m = 1;
					while(m<=M){
						if(meta[m]==0){
							meta[m] = Long.parseLong(line[7]); 
							break;
						}
						m++;
					}
					newIndex.put(line[4],meta); //update the index
					/*
					 * Here we can set a threshold to limit the cache size!!
					 */
					for(int ii = 1; ii <= M; ii++){
						if(!containerRecord.containsKey(meta[ii])){
						reStorageLoad(meta[ii]);  //extract FPs from specified container
						containerRecord.put((int) meta[ii], 1);
						}
						}

				}else{
					long[] meta = new long[M+2];
					meta[0] = 0;
					meta[1] = Long.parseLong(file.getName());
					meta[M+1] = Long.parseLong(line[0]);
					newIndex.put(line[4],meta); //update the index
				}
			}
			
			i++;	
		}
		//remove the entries from the old index
		if(reclaim == true){
		java.util.Iterator<Entry<String, Integer>> iter = tempIndex.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<String, Integer> entry= iter.next();
			if(index.containsKey(entry.getKey())){
			index.remove(entry.getKey());
			bf.add(entry.getKey());  //add FPs which have been removed into the BloomFilter		
			delteEntry++;
//			System.out.println("removed entries: "+delteEntry);
			}
		}
		tempIndex.clear();
		}
//		System.out.println("Deleted entries are: "+delteEntry);
		for(int k = 0; k < hashRecord.length;k++){
			if(cache.containsKey(hashRecord[k])){
				extraDup+=chunkSizeRecord[k];
			}else{
				cache.put(hashRecord[k], (long)0); //update the cache 
			}
		}
		
	}
	afterIndex = index.size();
	
	//merge the index and new index:
	index.putAll(newIndex);
	del(dirFile);
}





//return the extra dup size
long getExtradup(){
	return extraDup;
}

long getSpaceReclaimed(){
	return (afterIndex-currentIndex);
}

long getLowhit(){
	return lowHit;
}

long getIndexSize(){
	return entryCount;
}

long getNewIndexSize(){
	return newIndex.size();
}

//method to check the lower n bits of fingerprint to determine boundary
private boolean check(long fingerprint) {
	int boundaryCheck = (int) (Math.log(newSamplingRate) / Math.log(2));
    int i = 0;
    boolean check = true;
    do {
        check = (fingerprint & (1L << i)) == 0;
        if (!check) {
            return false;
        }
        i++;
    } while (i < boundaryCheck);
    return true;
}

//load FPs in specified container
void reStorageLoad(Long containerNo) throws FileNotFoundException{
	Scanner loadIn;
	File theContainer = new File(storageFileFolder+"/"+containerNo);
			loadIn = new Scanner(theContainer);
			loadIn.nextLine();
			while(loadIn.hasNextLine()){
			String[] infor = loadIn.nextLine().split(",");	
//			if(!infor[5].substring(0, 3).equals("Dup")){
			cache.put(infor[4], containerNo); // entries in cache have their container numbers
//			}
			}	
}
 void del(File f) throws IOException{           
	if(f.exists() && f.isDirectory()){
	   if(f.listFiles().length==0){//del it if it's empty 
	      f.delete();  
	    }else{ 
	       File delFile[]=f.listFiles();  
	        int i =f.listFiles().length;  
	        for(int j=0;j<i;j++){  
	           if(delFile[j].isDirectory()){  
	               del(delFile[j]);  
	            }  
	           delFile[j].delete();     
	        }  
	    }      
	} 
	System.out.println(f.getAbsolutePath() + " has been deleted");
}
}

