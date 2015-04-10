import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Scanner;

/*
 * load the info from the storage 
 * the format of storage is :
 * 
 * "container No, seg No., file name, chunk No, hashvalue, sampling tag"
 * 6 columns
 * load the sampled hashvalues and its container No to the hashmap
 * when the sampled Fps are hitted, calculates its lnie number and extract all the fps in this
 * container to do the comparison, calculate the dedup rate
 * use a seperate file for each container
 */
public class storage_v2 {
	Map<String, Long> cache;
	Map<String, long[]> index;
//	Map<String, Long> counter ;
	String storageFileFolder;
	long containerNo;
	 int chunkSize;
	 int segSize;
	 long chunkNum;
//	 String[] hashRecord;
	 int[] uniqueRecord;
	 int[] sampledRecord;
	 File sampledHash;
	
	storage_v2(Map<String, long[]> index, Map<String, Long> cache, String storageFileFolder, int chunkSize, int segSize,
			int[] uniqueRecord,int[] sampledRecord,File sampledHash){
		this.sampledHash = sampledHash;
		this.index = index;
		this.cache = cache;
		this.storageFileFolder = storageFileFolder;	
		this.chunkSize =chunkSize;
		this.segSize = segSize;
//		this.hashRecord = hashRecord;
		this.sampledRecord = sampledRecord;
		this.uniqueRecord = uniqueRecord;
		this.chunkNum = segSize/chunkSize*1024;
	}
	
	//load the info into the memory
	void storageLoad() throws IOException{
		/*
		 * Load the chunk's info to the index
		 */	
		Scanner loadIn;
		File[] containers = new File(storageFileFolder).listFiles();
		if(containers.length == 0){
			System.out.println("The storage is empty");
		}else{
			for(File file:containers){
				loadIn = new Scanner(file);
				loadIn.nextLine();
				while(loadIn.hasNextLine()){
				String[] infor = loadIn.nextLine().split(",");
				if(!infor[5].equals("N/A")){
				    long[] metaData = new long[] {Long.parseLong(infor[6]),Long.parseLong(file.getName()),Long.parseLong(infor[0])};
//					metaData[0] = Long.parseLong(infor[6]);  //counter number
//					metaData[1] = Long.parseLong(file.getName()); //container number
				index.put(infor[5], metaData );
//				counter.put(infor[5], Long.parseLong(infor[6]));
				}
				}
			}
		}
	}
	
	//load FPs in specified container
	void storageLoad(Long containerNo) throws FileNotFoundException{
		Scanner loadIn;
		File theContainer = new File(storageFileFolder+"/"+containerNo);
				loadIn = new Scanner(theContainer);
				loadIn.nextLine();
				while(loadIn.hasNextLine()){
				String[] infor = loadIn.nextLine().split(",");		
				cache.put(infor[4], containerNo);
				}	
	}
	
	
	//write the info into the storage
	void storageWrite() throws IOException{
		//visit the storage
		PrintWriter writeIn ;
		File[] containers = new File(storageFileFolder).listFiles();
		int tag = 0;
		//tag=0:the container is empty, be able to store chunkNum chunks
		//tag=1:the container is not empty, be able to store chunkLeft(=chunkNum-currentRecordLines) chunks
		long chunkLeft = 0;
		if(containers.length == 0){
			containerNo = 1;
			writeIn = new PrintWriter(new File(storageFileFolder + "/" + containerNo));
//			writeIn.flush();
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
			writeIn.println("HittingCounter");
		}else{//locate the current available container
		containerNo = containers.length;
		//if the last container is full
		if(getTotalLines(storageFileFolder + "/" + containerNo) == chunkNum+1 ){
			containerNo ++;//if this container is full, create a new one
			writeIn = new PrintWriter(new File(storageFileFolder + "/" + containerNo));
//			writeIn.flush();
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
			writeIn.println("HittingCounter");
		}else{
			writeIn = new PrintWriter(new FileOutputStream(new File(storageFileFolder + "/" + containerNo),true));
			tag=1;
			chunkLeft = chunkNum - getTotalLines(storageFileFolder + "/" + containerNo) + 1;
			//			writeIn.flush();
		}
		}	
		Scanner dataIn = new Scanner(sampledHash);		
		dataIn.nextLine();
		long entryCount = 0;
		for(int i = 1; i < uniqueRecord.length;i++){
			entryCount++;
			if(uniqueRecord[i]==1){
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
				writeIn.print(infor[5]);
				
				if(sampledRecord[i] == 1){
					writeIn.print(",");
					writeIn.println(infor[6]);
				}else{
					writeIn.println();
				}
				
			}else if(uniqueRecord[i]==0){
				dataIn.nextLine();
			}else{
				System.out.println("error with writing data to storage");
			}
			//If the current container is full, open another one
			if((tag == 0 && entryCount == chunkNum)||(tag == 1 && entryCount == chunkLeft)){
				containerNo ++;//if this container is full, create a new one
				writeIn = new PrintWriter(new File(storageFileFolder + "/" + containerNo));
//				writeIn.flush();
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
				writeIn.println("HittingCounter");
			}
			
		}
		writeIn.close();
	}

	
	
	
	//count the total number of lines(except the 1st line)
	static long getTotalLines(String file) throws IOException {
        Scanner in = new Scanner(new File(file));
        long lines = 0;
        while(in.hasNextLine()){
        	lines++;
        	in.nextLine();
        }
        in.close();
        return lines-1;
    }

}
