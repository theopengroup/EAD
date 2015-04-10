import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;


public class ElasticDedup {

	/**
	 * Wang, Yufeng
	 * 2013Spring 
	 */
	public static double downSampleTrig=0.9; //when this percentage of index is occupied, downsample
	public static long indexEntryNum = 100000; //The maximum size of index(2011papersolution) or initial index size============
//	public static double migrateTrig = 0.9;//when dedup rate falls below this percentage of estimated dedup rate,migrate
	public static int migratePara = 2; // the new sampling rate is migratePara times of original one
	public static boolean onlyDownsample = false;
	public static boolean downsampleWithMigrate = false;
	
	public static int currentHighestR = 1; //when resample, the new rate is larger than this, migrate, otherwise, current memory is enough
	public static boolean migrate = false;
	
	public static int M = 1; //number of containers to be tracked in the index (num of segments to be compared)
	public static double expectRaio = 4;  //used to calculate the number of samples in the estimation 3:1,5:1
	public static double requiredRatio = 0.9;//when dedup rate falls below this percentage of estimated dedup rate,migrate
	public static int sampleRate = 1;
	public static int newSampleRate = 20;
	public static int chunksize = 8;
	public static int segsize = 16;
	public static int waveChoice = 1;
	public static boolean reSampleTag = false;  //tag of determine whether to resample the first wave of data : -r
	public static boolean secWave = false;  //tag of determine whether to continue processing the second wave of data : -F
	public static int modeChoice = 0;
	public static long indexSize=-1;
	public static boolean indexLimit = false;
	public static String resultRecordFolder = "testData/resultRecord";
	public static File[] recordFiles = new File(resultRecordFolder).listFiles();
	public static PrintWriter resultRecord; 
	public static double initialTotal = 0;
	public static double initialDUp = 0;
	public static long storageLimit = 400*1024*1024; //400MB=========================
	
	public static int times=10;
	
	
	public static int segAmount = 100;  //segment, every waveAmount segments of data, calculate dedup rate  & record index size
	public static boolean consecutiveDedup = false;
	
	static long starttime;
	static long endtime;	
	
	public static File fileManage = new File("testData/FileManager/fileInforRecord.txt");
	public static File ChunkManage = new File("testData/chunkManage/chunkInfo");
	public static File bloomfilterRecord = new File("testData/BloomFilter/removedChunk");
	public static String storageFileFolder = "testData/storageFolder";
	public static String reSampleStorageFolder = "testData/restorageFolder";
	
	public static String outputpath1 = "testData/outputSegSampleWave1";  //folder where store the input data table
	public static String outputpath2 = "testData/outputSegSampleWave2";  //folder where store the input data table
	
	public static BloomFilter<String> bf = new BloomFilter<String>(0.1, 150000);
	public static Map<String, long[]> index = new HashMap<String, long[]>();
	
	public static Map<String, long[]> estimateBase = new HashMap<String, long[]>(); //record infor for estimation

	public static long totalChunkNum = 0;
	public static double estiRatio = 0;
	public static double measuredRatio = 0;
	
	public static long fileTotal;
	public static Map<String, Long> cache = new HashMap<String, Long>();

	public static String[] hashRecord;
	public static int[] uniqueRecord;
	public static int[] sampledRecord;
	public static long[] containerTrack;
	public static long total;
	public static long dup;	
	public static long extraDup;	

	public static Map<Integer,Integer> containerRecord = new HashMap<Integer, Integer>();; //record the containers whose FPs have been extracted
	
	public static void main(String[] args) throws IOException {
	
			int i = 0;
			String arg = args[i];
//			System.out.println("The first input is: " + arg);
			i++;
			while(i<args.length){
			if(arg.equals("-c")){
				modeChoice = 1;
				if(i < args.length){
				chunksize = Integer.parseInt(args[i]);
				}else{System.out.println("Chunksize required (KB)");
				}
				i++;
				if(i < args.length){
				segsize = Integer.parseInt(args[i]);
				}else{System.out.println("Segment size required (MB)");
				}
				break;
			}else if(arg.equals("-s")){
				modeChoice = 2;
				if(i < args.length){
					chunksize = Integer.parseInt(args[i]);
					}else{System.out.println("Chunksize required (KB)");
					}
					i++;
				if(i < args.length){
					segsize = Integer.parseInt(args[i]);
					}else{System.out.println("Segment size required (MB)");
					}
					i++;
				if(i < args.length){
				waveChoice = Integer.parseInt(args[i]);
				}else{System.out.println("Wave choice needed (1/2)");
				}
				i++;
				if(i < args.length){
				sampleRate = Integer.parseInt(args[i]);
				}else{System.out.println("Sampling Rate required (Input an integer 1/R)");
				}	
				break;
			}else if(arg.equals("-e")){
				modeChoice=6;
				if(i < args.length){
					chunksize = Integer.parseInt(args[i]);
				}else{System.out.println("Chunksize required (KB)");
					}
					i++;
				if(i < args.length){
					segsize = Integer.parseInt(args[i]);
				}else{System.out.println("Segment size required (MB)");
					}
					i++;
				if(i < args.length){
					times = Integer.parseInt(args[i]);
				}else{System.out.println("how many times do you want to measure accuracy of estimation");
				}
					i++;
				if(i < args.length){
					expectRaio = Double.parseDouble(args[i]);
					System.out.println("The expected Ratio is: " + expectRaio);
				}else{System.out.println("Expected ratio for estimation required");
					}
					i++;
				if(i < args.length){
					segAmount = Integer.parseInt(args[i]);
				}else{System.out.println("per how many segments do you wonna get a statistics");
				}	
				break;
			}else if(arg.equals("-d")){
				modeChoice = 3;
//				System.out.println("The chunk size is: " + args[i]);
				if(i < args.length){
					chunksize = Integer.parseInt(args[i]);
					}else{System.out.println("Chunksize required (KB)");
					}
				i++;
				if(i < args.length){
					segsize = Integer.parseInt(args[i]);
					}else{System.out.println("Segment size required (MB)");
					}
				i++;
				if(i < args.length){
				sampleRate = Integer.parseInt(args[i]);
				}else{System.out.println("Sampling Rate required (Input an integer 1/R)");
				}
				i++;
				if(i < args.length){
					expectRaio = Double.parseDouble(args[i]);
					System.out.println("The expected Ratio is: " + expectRaio);
					}else{System.out.println("Expected ratio for estimation required");
					}
				i++;
				if(i < args.length){
					requiredRatio= Double.parseDouble(args[i]);
					}else{System.out.println("User required dedup ratio for estimation required");
					}
				i++;
				if(i < args.length){
					M = Integer.parseInt(args[i]);
					}else{System.out.println("Num of segments to be compared required");
					}
				i++;
				
				if(i < args.length && args[i].equals("-i")){
					indexLimit = true;
					i++;
					if(i < args.length){
						indexLimit=true;
						indexSize = Long.parseLong(args[i]);
					}else{System.out.println("index maximum size required (Input an integer 1/R)");
					System.exit(0);
					}
					i++;
				}
				
				if(i < args.length && args[i].equals("-c")){
					consecutiveDedup = true;
					i++;
					if(i < args.length){
						segAmount = Integer.parseInt(args[i]);
					}else{System.out.println("PLS input per how many segments do you wonna do the dedup");
					System.exit(0);
					}
					i++;
				}
				
				if(i < args.length && args[i].equals("-v")){
					modeChoice = 4;
					System.out.println("modeChoice: "+modeChoice);
					onlyDownsample = true;
					i++;
					if(i < args.length){
						downSampleTrig = Double.parseDouble(args[i]);
						i++;
						if(i < args.length){
							indexEntryNum = Long.parseLong(args[i]);
							storageLimit = indexEntryNum*chunksize*1024;
						}else{System.out.println("PLS input the initial number of index entries");System.exit(1);}
					}else{System.out.println("PLS input the trigger percentage for downsampling");
					System.exit(0);
					}
					i++;
				}else if(i < args.length && args[i].equals("-u")){
					modeChoice = 5;
					System.out.println("modeChoice: "+modeChoice);
					downsampleWithMigrate = true;
					i++;
					if(i < args.length){
						downSampleTrig = Double.parseDouble(args[i]);
						i++;
						if(i < args.length){
							indexEntryNum = Long.parseLong(args[i]);
							storageLimit = indexEntryNum*chunksize*1024;
							i++;
							
										if(i < args.length){
											migratePara = Integer.parseInt(args[i]);
										}else{System.out.println("PLS input the fraction of new sampling rate compared with original one");System.exit(1);}
								
						}else{System.out.println("PLS input the initial number of index entries");System.exit(1);}
					}else{System.out.println("PLS input the trigger percentage for downsampling");
					System.exit(0);
					}
					i++;
				}
				
				
				if(i < args.length && args[i].equals("-r")){
				reSampleTag = true;
				i++;
				if(i < args.length){
				newSampleRate = Integer.parseInt(args[i]);
				}else{System.out.println("New Sampling Rate required (Input an integer 1/R)");
				System.exit(0);
				}
				i++;
				}else{
				reSampleTag = false; 
				System.out.println("No resampling"); 
				}
				if(i < args.length && args[i].equals("-F")){
					secWave = true;
					i++;
					if(i < args.length){
					newSampleRate = Integer.parseInt(args[i]);
					}else{System.out.println("New Sampling Rate required (Input an integer 1/R)");
					}				
				}else{secWave = false; System.out.println("No processing on the 2 nd wave of data"); 
				}
				break;
				}			
else{
			System.out.println("PLS input the correct command! (start with '-c'/'-s'/'-d') ");
			System.exit(0);
		}
		}	
	
		
		
		
		

		//		
//		System.out.println("Choose the model:\n" +
//						"'1': chunk the inputfile\n" +
//						"'2': segment and sampling chunks\n" +
//						"'3': deduplication");
			switch(modeChoice){
			case 1:
				System.out.println("Chunk Mode: \nThe chunk size is: " + chunksize
									+ "\nThe segment size is: " + segsize);
//				System.out.println(fileManage.getPath());
				starttime = System.currentTimeMillis();
		        PrintWriter fileInfo = new PrintWriter(fileManage); 
	            PrintWriter ChunkMan = new PrintWriter(ChunkManage);	
			String outputDirec = "testData/outputRawChunks/";

	        File[] inputDirec = new File("testData/inputRawFiles").listFiles();
	        //record the info of input files
	        int fileCount = 0;
	        fileInfo.flush();
	        fileInfo.print("File count,File name,Expected size(KB),Chunk Number\n");
	        for (File inputfile : inputDirec) {
//	        	System.out.println(inputfile.getName());
	    		fileCount++;
		        fileInfo.print(fileCount);
		        fileInfo.print(",");
		        fileInfo.print(inputfile.getName());
		        fileInfo.print(",");
		        fileInfo.print(chunksize);
		        fileInfo.print(",");
	            System.out.println("This file is " + inputfile.length()/1024 +"KB");
	            
	            //Create a folder for each input file
	            mkdir(outputDirec + inputfile.getName()); 
//	            outputDirec = outputDirec + inputfile.getName();
//	            System.out.println("The path is: " + outputDirec);    

	    		fileProcessing f1 = new fileProcessing(inputfile,outputDirec + inputfile.getName(),chunksize,fileInfo,ChunkMan);
	    		f1.chunkFile();
	        }
    		ChunkMan.close();
	        fileInfo.close();
			endtime = System.currentTimeMillis();
	        System.out.println("The chunksize is " + chunksize + " KB. There're " + fileCount + " input files processed");
	        System.out.println("Processing time is: " + (endtime - starttime)/1000 +" Sec");
	        break;
	        
		case 2:
			System.out.println("Segmentation Mode: \nThe chunk size is: " + chunksize
					+ "\nThe segment size is: " + segsize
					+ "\nProcessing " + waveChoice +" st/nd wave of data");		
//			System.out.println("Please specify which wave you are processing? '1' means the first wave, '2' means the second wave");
			String outputpath = null;
//			waveChoice = in.nextInt();
			if(waveChoice == 1){
			outputpath = outputpath1;
			}else if(waveChoice == 2){
			outputpath = outputpath2;	
			}else{
				System.out.println("Input error, PLS input 1 or 2");
			}
			System.out.println(outputpath);
			
			
			File hashinput = new File("testData/outputHashvalue/ChunkHashInfo.txt");

//			System.out.println("Please input the value R (R=1/samplingRate): ");
//			sampleRate = in.nextInt();     
			
			starttime = System.currentTimeMillis();
	        segSample_v2 f2 = new segSample_v2(hashinput,outputpath,segsize,fileManage,ChunkManage);
	        //System.out.println("the sampling rate is "+sampleRate);
	        f2.seg_sample();	
			endtime = System.currentTimeMillis();
	        System.out.println("Processing time is: " + (endtime - starttime)/1000 +" Sec");
			break;
			

	case 3:
		/*
		 * Part I: deduplication
		 */

		resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
		resultRecord.flush();
		resultRecord.println("Deduplication Mode: \nThe chunk size is: " + chunksize + " KB"
				+ "\nThe segment size is: " + segsize + " MB"
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate
				+"\nThe segments to be compared(M) are: " + M);
		
		System.out.println("Deduplication Mode: \nThe chunk size is: " + chunksize
				+ "\nThe segment size is: " + segsize
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate);	
		dedupProcess(1);
		
		if(reSampleTag ==true){
		/*
		 * Prat II, change the sampling rate, resample and rededup 
		 */
			resultRecord.println("\nResampling ... The new sampling rate is: " + (double)1/newSampleRate
					+ "\nThe expected dup reduction ratio is: " + expectRaio
					+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
			
			System.out.println("Resampling ... The new sampling rate is: " + (double)1/newSampleRate
					+ "\nThe expected dup reduction ratio is: " + expectRaio
					+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
			resampleProcess(newSampleRate);	
		}else{
			System.out.println("No resampling");
		}
		/*
		 * Do the deduplication again
		 */
		if(secWave == true){
			
			resultRecord.println("\nDeduplication Mode: \nThe chunk size is: " + chunksize
					+ "\nThe segment size is: " + segsize
					+ "\nProcessing 2 nd wave of data"
					+"\nThe new sampling rate is: " + (double)1/newSampleRate);
			
			System.out.println("Deduplication Mode: \nThe chunk size is: " + chunksize
					+ "\nThe segment size is: " + segsize
					+ "\nProcessing 2 nd wave of data"
					+"\nThe new sampling rate is: " + (double)1/newSampleRate);
			
		dedupProcess(2);
		}else if(secWave == false){
			System.out.println("No 2 nd wave");
		}				
		resultRecord.close();
		break;
	/*
	 * case4: using only downsampling when the index is approaching full, entry mod 2 = 0
	 */
	case 4:
		System.out.println("Now the mode with only downsampling");
		
		resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
		resultRecord.flush();
		resultRecord.println("Deduplication Mode with only downsampling: \nThe chunk size is: " + chunksize + " KB"
				+ "\nThe segment size is: " + segsize + " MB"
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate
				+"\nThe segments to be compared(M) are: " + M
				+"\nThe downsampleTrig value is: "+downSampleTrig
				+"\nThe maximum entry # are: "+indexEntryNum);
		
		System.out.println("Deduplication Mode: \nThe chunk size is: " + chunksize
				+ "\nThe segment size is: " + segsize
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate);
		dedupWithDownsample();
		resultRecord.close();
		break;
	/*
	 * case5: the solution, downsampling by eliminate entries with lower counter values
	 * 		  while the dedup rate drops below the requiered value, increase sampling rate
	 */
	case 5:
		System.out.println("Now the mode of elasticity: downsample & index migrate");
		
		resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
		resultRecord.flush();
		resultRecord.println("Deduplication Mode with elasticity: \nThe chunk size is: " + chunksize + " KB"
				+ "\nThe segment size is: " + segsize + " MB"
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate
				+"\nThe segments to be compared(M) are: " + M
				+"\nThe downsampleTrig value is: "+downSampleTrig
				+"\nThe maximum entry # are: "+indexEntryNum
				+"\nWhen dedup rate is below "+requiredRatio+" of estimate rate, do migrate"
				+"\nThe new samplerate is "+migratePara+" times of original one");
		
		System.out.println("Deduplication Mode: \nThe chunk size is: " + chunksize
				+ "\nThe segment size is: " + segsize
				+ "\nProcessing 1 st wave of data"
				+"\nThe sampling rate is: " + (double)1/sampleRate);
		dedupElasitic();
		resultRecord.close();
		break;
		
	case 6:
		System.out.println("Measure the accuracy of estimation:");
		
		resultRecord = new PrintWriter(resultRecordFolder+"/"+(recordFiles.length+1));
//		resultRecord.flush();
		resultRecord.println("Estimation error test: \nThe chunk size is: " + chunksize + " KB"
				+ "\nThe segment size is: " + segsize + " MB");
		
		System.out.println("Estimation error test Mode: \nThe chunk size is: " + chunksize
				+ "\nThe segment size is: " + segsize);
		File[] incomingFile = null;
		incomingFile = new File(outputpath1).listFiles();
		int sampleNum = (int) ((Math.log(2)+Math.log(10000))/2/0.0001/Math.pow(expectRaio,2));
		resultRecord.println("Pick out total " + sampleNum + " samples for estimation; total "+times+" times");	
    	System.out.println("Pick out total " + sampleNum + " samples for estimation");
		int sampleNumPerSeg = sampleNum/incomingFile.length; // samples per segment	
		for(int timeCount = 1; timeCount <=times; timeCount++){
			System.out.println("The "+timeCount+" th of estimation");
		estimateBase.clear();
		if(estimateBase.isEmpty()){
		int segmentNo = 1;
		for(segmentNo=1;segmentNo<=incomingFile.length;segmentNo++){
			File segFile = new File(outputpath1+"/Segment_"+ segmentNo);
			hashRecord = new String[segsize*1024/chunksize];
			estimationCal(hashRecord,segFile,sampleNumPerSeg,estimateBase);
			if(consecutiveDedup=true && segmentNo%segAmount == 0){	
				resultRecord.print(getEstimateRatio(1)*100);
				resultRecord.print(",");
			}
		}
		resultRecord.print(getEstimateRatio(1)*100);
		resultRecord.println();
		}else{
			System.out.println("estimateBase is not empty");
		}
		}
		resultRecord.close();
		
		break;
			}	
		
	}

	static void resampleProcess(int newR) throws IOException{
		/*
		 * Determine the threshold for resampling
		 */
		long indexsize = index.size();
		int threshold = -1;
		long c = 0;
		estiRatio = getEstimateRatio(1);
		measuredRatio = (double)dup/total/estiRatio;
		
		resultRecord.println("Total chunks are: " + totalChunkNum
				+ "\nOriginal samplingrate is: " + (double)1/migratePara/newR
				+"\nCurrent sampling rate is: " + (double)1/newR
				+ "\nEstimated ratio is: " + estiRatio
				+"\nMeasured rate is " + (double)dup/total
				+ "\nRequired normalized ratio is: " + requiredRatio
				+ "\nCurrent normalized ratio is: " + measuredRatio
				);
		resultRecord.println("\nThe amount of evicted chunks is: " +(double)1/migratePara/newR*totalChunkNum*estiRatio*(requiredRatio - measuredRatio));

		
		System.out.println("Total chunks are: " + totalChunkNum
							+ "\nsamplingrate is: " + (double)1/newR
							+ "\nEstimated ratio is: " + estiRatio
							+ "\nRequired normalized ratio is: " + (double)requiredRatio
							+ "\nCurrent normalized ratio is: " + measuredRatio);	
		System.out.println("The amount of evicted chunks is: " +(double)1/newR*totalChunkNum*estiRatio*(requiredRatio - measuredRatio));
		double evict = (double)1/newR*totalChunkNum*estiRatio*(requiredRatio - measuredRatio);
		while(c<(double)1/newR*totalChunkNum* estiRatio*(requiredRatio - measuredRatio)){
			threshold+=1 ;
		Iterator<Entry<String, long[]>> iter = index.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, long[]> entry= iter.next();
			long meta[] = new long[M+2];
			meta = entry.getValue();
			if(meta[0] == threshold){
				c++;
	//			System.out.println("Picking the qualified index entries " + c);
			}

		}
		}
		resultRecord.println("The threshold for resampling is: " + threshold);
		
		System.out.println("The threshold for resampling is: " + threshold);
			
		resample RdP = new resample(index,M,cache,containerRecord,newR*migratePara,newR,threshold
									,chunksize,  segsize, bf, bloomfilterRecord,storageFileFolder
									,reSampleStorageFolder,hashRecord,evict);
		RdP.resampleProcess();
		RdP.reDedup();	
		extraDup+=RdP.getExtradup();	
		dup += extraDup;
		resultRecord.println("The total data is: "+total+"\nThe extra duplicates are: "+RdP.getExtradup()+
				"\nCurrent dup is: " + (double)(dup)+
				"\nThe duplication rate is : " + (double)(dup)/total*100 +"%\n"
					+RdP.getLowhit()+" entries are picked out, "
					+RdP.getSpaceReclaimed()+ " entries in the index have been removed"
					+"\nOriginal index size is: "+ indexsize
					+"\nCurrent index size is; "+ (RdP.getNewIndexSize()+RdP.getIndexSize()-RdP.getSpaceReclaimed()));
		
		System.out.println("The total data is: "+total+"\nThe extra duplicates are: "+RdP.getExtradup()+
						"\nThe duplication rate is : " + (double)(dup)/total*100 +"%\n" +
							RdP.getLowhit()+" entries are picked out, "+RdP.getSpaceReclaimed()
							+ " entries in the index have been removed\nCurrent index size is; "
							+ (RdP.getNewIndexSize()+RdP.getIndexSize()-RdP.getSpaceReclaimed()));
//		System.out.println("The current BloomFilter size is: " + bf.size());
	}
	
	static void dedupProcess(int wave) throws IOException{
//		System.out.println("Please input the value R (R=1/samplingRate): ");
//		Scanner in  = new Scanner(System.in);
//		sampleRate = in.nextInt();  

//		int segCount = 0;//count for # of segments have been processed
		int memoryPolicy = 5;
		resultRecord.println("Total " + storageLoad() + " entries added into the index");
		File[] incomingFile = null;
		if(wave == 1){	
		incomingFile = new File(outputpath1).listFiles();
		initialTotal = total;
		initialDUp = dup; 
		}else if(wave == 2){
		incomingFile = new File(outputpath2).listFiles();	
		}
		int cacheSizeControl = 0;
		int sampleNum = (int) ((Math.log(2)+Math.log(10000))/2/0.0001/Math.pow(expectRaio,2));
		
		resultRecord.println("Pick out total " + sampleNum + " samples for estimation");
		
		System.out.println("Pick out total " + sampleNum + " samples for estimation");
		int sampleNumPerSeg = sampleNum/incomingFile.length; // samples per segment
		
		for(int i = 1; i <=incomingFile.length; i++){
			File file = null;
			if(wave == 1){
			file = new File(outputpath1+"/Segment_"+ i);
			}else if(wave == 2){
			file = new File(outputpath2+"/Segment_"+ i);	
			}
			hashRecord = new String[segsize*1024/chunksize];
			uniqueRecord = new int[segsize*1024/chunksize];
			sampledRecord = new int[segsize*1024/chunksize];
			containerTrack = new long[segsize*1024/chunksize];
		cacheSizeControl++;
		if(cacheSizeControl==memoryPolicy ){  //cache size control
		containerRecord.clear();
		cache.clear();
		cacheSizeControl=0;
		}
		
		dedupProcess_v2 dP = new dedupProcess_v2(index,indexSize, M,cache,chunksize, segsize, hashRecord,
				 uniqueRecord,sampledRecord,containerTrack,storageFileFolder,bf, bloomfilterRecord, 
				 file,containerRecord,estimateBase,sampleNumPerSeg,segAmount,consecutiveDedup,sampleRate);		
		dP.dedup();
		total += dP.getTotal();
		dup += dP.getDup();
		
		/*
		 * periodically output statistics (every 'segAmount' segments, we do one time's dedup)
		 */
		if(consecutiveDedup=true && i%segAmount == 0){	
			resultRecord.println();
			resultRecord.println("The culmulative segments are: " + i +
								"\nCurrent data amount is: "+total+
								"\nThe duplicates are: "+dup+
								"\nThe current index size is: " + index.size()+
								"\nThe deduplication rate is : " + (double)dup/total*100 +"%"+
								"\nThe estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			resultRecord.println();
			resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
		}
		
		totalChunkNum += dP.getTotalChunkNum();
		dP.storageWrite(); 	

		System.out.println("The current cache size is: " + dP.getCacheSize());
		}
		
		resultRecord.println("The current index size is: " + index.size());
		
		System.out.println("The current index size is: " + index.size());
//		System.out.println("The current BloomFilter size is: " + bf.size());		
		
		if(wave == 1){
		resultRecord.println("The total data is: "+total+"\nThe duplicates are: "+dup+
					 "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
			
		System.out.println("The total data is: "+total+"\nThe duplicates are: "+dup+
				"\nThe sampling rate is: " + sampleRate + "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
		measuredRatio = (double)dup/total;
		}else if(wave == 2){
			
			resultRecord.println("The total data is: "+total);	
			resultRecord.println("The second wave data is: " + (total - initialTotal) + "\nduplicates are: " + (dup - initialDUp)+
									"\nThe deduplication rate for second wave is: " + (dup-initialDUp)/(total-initialTotal)*100 + "%");
			resultRecord.println("\nThe overall duplicates are: "+(dup+extraDup)+"\nThe deduplication rate is : " + (double)(dup+extraDup)/total*100 +"%");	
			
			System.out.println("The total data is: "+total+"\nThe duplicates are: "+(dup+extraDup)+"\nThe deduplication rate is : " + (double)(dup+extraDup)/total*100 +"%");	
		}
			estiRatio = getEstimateRatio(wave);
			
			resultRecord.println("The estimated dedup rate is: " + getEstimateRatio(wave)*100 +" %");
			
			System.out.println("The estimated dedup rate is: " + getEstimateRatio(wave)*100 +" %");

	
			indexStatistic();
			
		
		
	}
	
	/*
	 * dedup Process with downsampling mechanism
	 * using recursive
	 * always process files in outputSegment1
	 */
	
	static void dedupWithDownsample() throws IOException{

		dup=0;
		int memoryPolicy = 5;
		resultRecord.println("Total " + storageLoad() + " entries added into the index");
		File[] incomingFile = null;
		incomingFile = new File(outputpath1).listFiles();
		initialTotal = total;
		initialDUp = dup; 
		int cacheSizeControl = 0;
		int sampleNum = (int) ((Math.log(2)+Math.log(10000))/2/0.0001/Math.pow(expectRaio,2));
		
		resultRecord.println("Pick out total " + sampleNum + " samples for estimation");
		
		System.out.println("Pick out total " + sampleNum + " samples for estimation");
		int sampleNumPerSeg = sampleNum/incomingFile.length; // samples per segment
		
//		for(int i = 1; i <=incomingFile.length; i++){
		
		for(int segmentNo=1;segmentNo<=incomingFile.length;segmentNo++){
//			if(index.size() >= downSampleTrig*indexEntryNum){
			while((total-dup)>= downSampleTrig*storageLimit){
				   sampleRate = 2*sampleRate;
				   storageLimit = storageLimit*2;
				
				resultRecord.println("The current storage size is: " + (total-dup)+
						  "\nIt's approaching " + downSampleTrig + "of supported: "+ storageLimit);
				resultRecord.println("Index is gonna be full, downsampling...");
				resultRecord.println("Index size before downsampling: "+ index.size());
				indexShrink();
				resultRecord.println("Index size after downsampling: "+ index.size());
				resultRecord.println("\nDownsampling ... The new sampling rate is: " + (double)1/sampleRate);
			}
//			}
//			consecutiveDownSample(segmentNo,sampleRate,cacheSizeControl,memoryPolicy,sampleNumPerSeg);
			File file = null;
			file = new File(outputpath1+"/Segment_"+  segmentNo);
			hashRecord = new String[segsize*1024/chunksize];
			uniqueRecord = new int[segsize*1024/chunksize];
			sampledRecord = new int[segsize*1024/chunksize];
			containerTrack = new long[segsize*1024/chunksize];
		cacheSizeControl++;
		if(cacheSizeControl==memoryPolicy ){  //cache size control
		containerRecord.clear();
		cache.clear();
		cacheSizeControl=0;
		}
		
		dedupProcess_v2 dP = new dedupProcess_v2(index,indexSize, M,cache,chunksize, segsize, hashRecord,
				 uniqueRecord,sampledRecord,containerTrack,storageFileFolder,bf, bloomfilterRecord, 
				 file,containerRecord,estimateBase,sampleNumPerSeg,segAmount,consecutiveDedup,sampleRate);		
		dP.dedup();
		total += dP.getTotal();
		dup += dP.getDup();
		if(consecutiveDedup=true && segmentNo%segAmount == 0){	
			resultRecord.println();
			resultRecord.println("The culmulative segments are: " + segmentNo +
								"\nCurrent data amount is: "+total+
								"\nThe duplicates are: "+dup+
								"\nThe current index size is: " + index.size()+
								"\nThe deduplication rate is : " + (double)dup/total*100 +"%"+
								"\nThe estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			resultRecord.println();
			resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
		}
		
		totalChunkNum += dP.getTotalChunkNum();
		dP.storageWrite(); 	

//		System.out.println("The current cache size is: " + dP.getCacheSize());
		}
			
		resultRecord.println("The current index size is: " + index.size());
		
		System.out.println("The current index size is: " + index.size());
//		System.out.println("The current BloomFilter size is: " + bf.size());		
		

		resultRecord.println("The total data is: "+total+"\nThe duplicates are: "+dup+
					 "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
			
		System.out.println("The total data is: "+total+"\nThe duplicates are: "+dup+
				"\nThe sampling rate is: " + sampleRate + "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
		measuredRatio = (double)dup/total;

			estiRatio = getEstimateRatio(1);
			
			resultRecord.println("The estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			
			System.out.println("The estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
			/*record # of entries with different hitting number */
			indexStatistic();
			
		
		
	}
	//dedup with downsampling ends here
	
	
	static void dedupElasitic() throws IOException{
		dup=0;
		int memoryPolicy = 5;
		resultRecord.println("Total " + storageLoad() + " entries added into the index");
		File[] incomingFile = null;
		incomingFile = new File(outputpath1).listFiles();
		initialTotal = total;
		initialDUp = dup; 
		int cacheSizeControl = 0;
		int sampleNum = (int) ((Math.log(2)+Math.log(10000))/2/0.0001/Math.pow(expectRaio,2));
		
		resultRecord.println("Pick out total " + sampleNum + " samples for estimation");
		
		System.out.println("Pick out total " + sampleNum + " samples for estimation");
		int sampleNumPerSeg = sampleNum/incomingFile.length; // samples per segment
		
//		for(int i = 1; i <=incomingFile.length; i++){
		for(int segmentNo=1;segmentNo<=incomingFile.length;segmentNo++){
//			if(index.size() >= downSampleTrig*indexEntryNum){
			while((total-dup)>= downSampleTrig*storageLimit){
				   sampleRate = 2*sampleRate;
				   storageLimit = storageLimit*2;
				
				resultRecord.println("The current storage size is: " + (total-dup)+
						  "\nIt's approaching " + downSampleTrig + "of supported: "+ storageLimit);
				resultRecord.println("Index is gonna be full, downsampling...");
				resultRecord.println("Index size before downsampling: "+ index.size());
				indexShrink();
				resultRecord.println("Index size after downsampling: "+ index.size());
				resultRecord.println("\nDownsampling ... The new sampling rate is: " + (double)1/sampleRate);
			}
//			}
//			consecutiveDownSample(segmentNo,sampleRate,cacheSizeControl,memoryPolicy,sampleNumPerSeg);
			File file = null;
			file = new File(outputpath1+"/Segment_"+  segmentNo);
			hashRecord = new String[segsize*1024/chunksize];
			uniqueRecord = new int[segsize*1024/chunksize];
			sampledRecord = new int[segsize*1024/chunksize];
			containerTrack = new long[segsize*1024/chunksize];
		cacheSizeControl++;
		if(cacheSizeControl==memoryPolicy ){  //cache size control
		containerRecord.clear();
		cache.clear();
		cacheSizeControl=0;
		}
		
		dedupProcess_v2 dP = new dedupProcess_v2(index,indexSize, M,cache,chunksize, segsize, hashRecord,
				 uniqueRecord,sampledRecord,containerTrack,storageFileFolder,bf, bloomfilterRecord, 
				 file,containerRecord,estimateBase,sampleNumPerSeg,segAmount,consecutiveDedup,sampleRate);		
		dP.dedup();
		total += dP.getTotal();
		dup += dP.getDup();
		if(consecutiveDedup=true && segmentNo%segAmount == 0){	
			resultRecord.println();
			resultRecord.println("The culmulative segments are: " + segmentNo +
								"\nCurrent data amount is: "+total+
								"\nThe duplicates are: "+dup+
								"\nThe current index size is: " + index.size()+
								"\nThe deduplication rate is : " + (double)dup/total*100 +"%"+
								"\nThe estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			resultRecord.println();
			resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
		
			if((double)dup/total < requiredRatio*getEstimateRatio(1) && sampleRate >=migratePara && downsampleWithMigrate == true){
			    //resultRecord.println("The current sampleRate: "+sampleRate+" is larger than migratePara "+migratePara);
					resultRecord.println("\nSampling rate: "+(double)1/sampleRate+" is too low");
					System.out.println("\nSampling rate: "+(double)1/sampleRate+" is too low");
					sampleRate = sampleRate/migratePara; //The minimum value is 1, so the original value needs to be larger than 2
					resultRecord.println("\nResampling ... " 
								+"current dedup rate is: "+ (double)dup/total*100+"% ,less than "+requiredRatio+" of estimated rate "+ getEstimateRatio(1)*100+"% "
								+"The new sampling rate is: " + (double)1/sampleRate
								+ "\nThe estimated dedup rate is : " + getEstimateRatio(1)*100 + "%"
								+"\nCurrent dedup rate is: "+ dup/total*100 + "%"
								+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
						
					System.out.println("Resampling ... The new sampling rate is: " + (double)1/sampleRate
								+ "\nThe expected dup reduction ratio is: " + expectRaio
								+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
					resampleProcess(sampleRate);	
						
					if(index.size()>=indexEntryNum/2){
						indexEntryNum = (long) (migratePara*indexEntryNum); //increase the index size(add RAM size)
					}//resultRecord.println("\nThe current index entries are "+indexEntryNum+" ,it's " + migrateTrig +" times of original one");
					}
		}
		
		totalChunkNum += dP.getTotalChunkNum();
		dP.storageWrite(); 	

//		System.out.println("The current cache size is: " + dP.getCacheSize());
		}			
		resultRecord.println("The current index size is: " + index.size());
		
		System.out.println("The current index size is: " + index.size());
//		System.out.println("The current BloomFilter size is: " + bf.size());		
		

		resultRecord.println("The total data is: "+total+"\nThe duplicates are: "+dup+
					 "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
		
		resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
		System.out.println("The total data is: "+total+"\nThe duplicates are: "+dup+
				"\nThe sampling rate is: " + sampleRate + "\nThe deduplication rate is : " + (double)dup/total*100 +"%");
		measuredRatio = (double)dup/total;

			estiRatio = getEstimateRatio(1);
			
			resultRecord.println("The estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
			
			System.out.println("The estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
		/*record # of entries with different hitting number */
			indexStatistic();
		
		
	}
	
	
	
	
	
	
static void consecutiveDownSample(int segmentNo, int R,int cacheSizeControl,int memoryPolicy,int sampleNumPerSeg) throws IOException{
		
		File file = new File(outputpath1+"/Segment_"+ segmentNo);
		hashRecord = new String[segsize*1024/chunksize];
		uniqueRecord = new int[segsize*1024/chunksize];
		sampledRecord = new int[segsize*1024/chunksize];
		containerTrack = new long[segsize*1024/chunksize];
		cacheSizeControl++;
		if(cacheSizeControl==memoryPolicy ){  //cache size control
				containerRecord.clear();
				cache.clear();
				cacheSizeControl=0;
		}
	
	dedupProcess_v2 dP = new dedupProcess_v2(index,indexSize, M,cache,chunksize, segsize, hashRecord,
			 uniqueRecord,sampledRecord,containerTrack,storageFileFolder,bf, bloomfilterRecord, 
			 file,containerRecord,estimateBase,sampleNumPerSeg,segAmount,consecutiveDedup,R);		
	dP.dedup();
	total += dP.getTotal();
	dup += dP.getDup();
	/*
	 * periodically output statistics (every 'segAmount' segments, we do one time's dedup)
	 */
	if(consecutiveDedup=true && segmentNo%segAmount == 0){	
		resultRecord.println();
		resultRecord.println("The culmulative segments are: " + segmentNo +
							"\nCurrent data amount is: "+total+
							"\nThe duplicates are: "+dup+
							"\nThe current data in total is: "+total+
							"\nThe current sampling rate is: " + (double)1/R +
							"\nThe current index size is: " + index.size()+
							"\nThe deduplication rate is : " + (double)dup/total*100 +"%"+
							"\nThe estimated dedup rate is: " + getEstimateRatio(1)*100 +" %");
		//Then calculate the efficiency
		resultRecord.println("####The dedup efficiency is: " + (double)dup/(indexEntryNum-index.size())+" dup/wasted entry");	
		resultRecord.println();

	if((double)dup/total < requiredRatio*getEstimateRatio(1) && R >=migratePara && downsampleWithMigrate == true){
    //resultRecord.println("The current sampleRate: "+sampleRate+" is larger than migratePara "+migratePara);
		resultRecord.println("\nSampling rate: "+(double)1/R+" is too low");
		System.out.println("\nSampling rate: "+(double)1/R+" is too low");
		R = R/migratePara; //The minimum value is 1, so the original value needs to be larger than 2
		sampleRate = R;
		resultRecord.println("\nResampling ... " 
					+"current dedup rate is: "+ (double)dup/total*100+"% ,less than "+requiredRatio+" of estimated rate "+ getEstimateRatio(1)*100+"% "
					+"The new sampling rate is: " + (double)1/R
					+ "\nThe estimated dedup rate is : " + getEstimateRatio(1)*100 + "%"
					+"\nCurrent dedup rate is: "+ dup/total*100 + "%"
					+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
			
		System.out.println("Resampling ... The new sampling rate is: " + (double)1/R
					+ "\nThe expected dup reduction ratio is: " + expectRaio
					+ "\nThe user required dedup rate is: " + requiredRatio*100 + "%");
		resampleProcess(R);	
			
		if(index.size()>=indexEntryNum/2){
			indexEntryNum = (long) (migratePara*indexEntryNum); //increase the index size(add RAM size)
		}//resultRecord.println("\nThe current index entries are "+indexEntryNum+" ,it's " + migrateTrig +" times of original one");
		}
	}
	
	totalChunkNum += dP.getTotalChunkNum();
	dP.storageWrite(); 	//update index and storagefolder

	}




	static double getEstimateRatio(int wave){
		double ratioI = 0;
		java.util.Iterator<Entry<String, long[]>> iter = estimateBase.entrySet().iterator();
		long count=0;
		while(iter.hasNext()){
			Map.Entry<String, long[]> entry= iter.next();
			long estMeta[] = new long[2];
			estMeta = entry.getValue();
			ratioI += estMeta[0]/estMeta[1];
			count+=estMeta[0];
		}
//		System.out.println("There're " + count + " sampled entries");
		ratioI = 1 - ratioI/count/wave;
		return ratioI;
	}
	
	
	//load the info into the memory
	// disk to RAM
static long storageLoad() throws IOException
{
		/*
		 * Load the chunk's info to the index
		 */	
		long count=0;
		Scanner loadIn;
		File[] containers = new File(storageFileFolder).listFiles();
		if(containers.length == 0)
		{
			return 0;
		}
		else
		{
			for(File file:containers)
			{
				loadIn = new Scanner(file);
				loadIn.nextLine();
				while(loadIn.hasNextLine())
				{
					String[] infor = loadIn.nextLine().split(",");
					//				System.out.println("Now processing line "+ lineCount + "The temp is "+temp);
					//					long hashvalue = Long.parseLong(infor[4].substring(33),16);
					//				if(check(hashvalue) || (infor[5].substring(0,3).equals("Dup"))){
					if(!infor[5].substring(0,3).equals("Dup")&&!infor[5].substring(0,3).equals("Not"))
					{
						if((indexLimit==true&&index.size()<indexSize) || indexLimit==false)
						{
							if((onlyDownsample==true && index.size()<indexEntryNum) 
									|| (downsampleWithMigrate==true && index.size()<indexEntryNum) )
							{
								long[] metaData= new long[M+2];
								metaData[0] = 0;
								metaData[1] = Long.parseLong(file.getName());
								metaData[M+1] = Long.parseLong(infor[0]);
								index.put(infor[4], metaData );
								count++;
							}
						}
					}
				}
			}
			System.out.println("Total " + count + " entries added into the index");
		}
		return count;
}
	
	 public static double log(double value, double base) {

		 return Math.log(value) / Math.log(base);
		 
		}
	 
	 static void indexShrink(){
			/*
			 * remove half old entries in the index
			 */
		 System.out.println("Index size before shrinking: "+index.size());
		 resultRecord.println("Index size before shrinking: "+index.size());
		 int max = index.size()/2;
		 int count = 0;	 
		 Iterator<Entry<String, long[]>> iter = index.entrySet().iterator();
		 while (iter.hasNext()) {
		     Map.Entry<String, long[]> entry = iter.next();
		     long fingerprint = Long.parseLong(entry.getKey().substring(30),16);
		     if(check(fingerprint)){
		    	 //Here print out the removed item:
				 System.out.println("Removed Entry from indexTable: "+entry.getKey());
				 resultRecord.println("Removed Entry from indexTable: "+entry.getKey());
		    	 //------------yufeng
		         iter.remove();
		         count++;
		         if(count>=max)
		        	 break;
		     }
		 }

		System.out.println("Index size after shrinking: "+index.size());	   
		resultRecord.println("Index size before shrinking: "+index.size());
		 
	 }
	
		public static  void mkdir(String mkdirName)
    {
        try
        {
            File  dirFile = new File(mkdirName);     //mkdirName: path of folder
            boolean bFile   = dirFile.exists();
            if( bFile == true )
            {
               System.out.println("The folder exists.");
            }
            else
            {
               System.out.println("The folder do  not exist,now trying to create a one...");

               bFile = dirFile.mkdir();
               if( bFile == true )
               {
                  System.out.println("Create successfully!");
                  System.out.println("Create the folder");
               }
               else
               {
                   System.out.println("Disable to make the folder,please check the disk is full or not.");
                   System.out.println(" Create folder Failed. Make sure enough space");
//                   System.exit(1);
               }
            }
        }
        catch(Exception err)
        {
            System.err.println("ELS - Chart : error");
            err.printStackTrace();
        }
    }
	
	static void estimationCal(String[] hashRecord,File segFile,int sampleNumPerSeg,Map<String, long[]> estimateBase ) throws FileNotFoundException {
		 /*
		  * Load the input file into the index, 
		  * initialize the uniqueRecord,sampledRecord,hashRecord
		  * if sampled, compare it to the index:match? extract FP from that container
		  */	 
		 long chunkNum=segsize*1024/chunksize;;
		Scanner loadIn;
//		System.out.println("The segfile is: " + segFile.getAbsolutePath());
		loadIn = new Scanner(segFile);
		loadIn.nextLine();
		int i=0;
		while(loadIn.hasNextLine()){
			String[] infor = loadIn.nextLine().split(",");
//			System.out.println("the current i is: " + i);
			hashRecord[i] = infor[4]; //put the hashvalue into the RAM for the following comparison
			i++;
		}
		loadIn.close();
			/*
			 * Put the samples into the sample base
			 */
			for(int count = 1; count <= sampleNumPerSeg; count++){
			int randomNum =(int)(Math.random()*(chunkNum-1)); 
			if(hashRecord[randomNum] != null && !estimateBase.containsKey(hashRecord[randomNum])){
				long estMeta[] = new long[2];
				estMeta[0] = 1;
				estMeta[1] = 0;
				estimateBase.put(hashRecord[randomNum], estMeta);
			}else if(hashRecord[randomNum] == null){
				count -- ;  //maybe the random num indicates the empty record because that the segment is not full
			}else if(hashRecord[randomNum] != null && estimateBase.containsKey(hashRecord[randomNum])){
				long estMeta[] = new long[2];
				estMeta = estimateBase.get(hashRecord[randomNum]);
				estMeta[0] ++;
				estimateBase.put(hashRecord[randomNum], estMeta);
			}
			}
			
			
			/*
			 * deduplication process
			 */
				for(int k = 0; k < hashRecord.length;k++){
					//estimation
					if(estimateBase.containsKey(hashRecord[k])){
						long estMeta[] = new long[2];
						estMeta = estimateBase.get(hashRecord[k]);
						estMeta[1] ++;
						estimateBase.put(hashRecord[k], estMeta);
					}
				}
		
	}
	
	static void indexStatistic(){
		/*record # of entries with different hitting number */
		long entryRcord[] = new long[7];
		Iterator<Entry<String, long[]>> iter = index.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, long[]> entry= iter.next();
			long meta[] = new long[M+2];
			meta = entry.getValue();
			if(meta[0]<=5){
			entryRcord[(int) meta[0]]++;	
			}else{
				entryRcord[6]++;
			}
		}
		int i=0;
		for(i=0;i<6;i++){
			resultRecord.println("# of entries with counter " +i+" : " + entryRcord[i]);
		}
			resultRecord.println("# of entries with counter larger than " +i +" : " +entryRcord[i]);
	}
	private static boolean check(long fingerprint) {
		int boundaryCheck = 1;
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
}
