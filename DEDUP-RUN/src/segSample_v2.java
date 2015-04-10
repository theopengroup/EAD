import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;


/*
 * This class is able to read hash info of chunks, 
 * group them into segments based on the average chunk size and segment size
 * Also choose out the sampled hash value based on sampling rate. (1/R)
 */
public class segSample_v2 {
	
	int segsize;
	int chunksize;
	int chunkNumber = 0 ;
	File inputhashfile;
	String outputpath;


	File fileInfo;
	File chunkInfo;
	
	segSample_v2(File inputfile, String outputpath, int segsize, File fileManage,File chunkInfo) throws FileNotFoundException{
		this.inputhashfile = inputfile;
		this.outputpath = outputpath;
		this.segsize = segsize;

		this.fileInfo = fileManage;	
		this.chunkInfo = chunkInfo;
//		fileInfo.nextLine();
	}
	
	//Read the hashvalues in, group them into segments
	void seg_sample() throws IOException{

		//File manage record is not empty		
		try {
			Scanner inputStream = new Scanner(inputhashfile);
			Scanner inputfileInfo = new Scanner(fileInfo);
			Scanner inputchunkInfo = new Scanner(chunkInfo);
			int chunkCount = 0; //used for segment count
			
			File[] existSeg = new File(outputpath).listFiles();			
			int segCount = existSeg.length+1;
		
			int chunkCount2 = 0; //used for read next item in the fileInfoRecord
			//Use a CSL file to record information
			PrintWriter out = new PrintWriter(outputpath+"/Segment_"+ segCount) ;
			out.flush();
			out.println("Segment No,FileName,ChunkName,ChunkSize,HashValue,SampleTag,Hitting Counter");		
		
			inputfileInfo.nextLine();
			String[] infor = inputfileInfo.nextLine().split(",");
//			System.out.println("The current line is " + infor[0]);
			chunksize = Integer.parseInt(infor[2]);
			String name = infor[1];
			int chunkNo = Integer.parseInt(infor[3]);
//			int fileCount = Integer.parseInt(infor[0]);
			chunkNumber = segsize*1024/chunksize;
			boolean hookGuarantee = false;   //guarantee every segment has a sampled chunk
			while(inputStream.hasNextLine()){					
				chunkCount ++;
				chunkCount2 ++;
				//turn the string into array of name + hashvalue
				String[] ary= inputStream.nextLine().split("= ");
				String[] lineChunkInfo = inputchunkInfo.nextLine().split(",");

//				System.out.println("the hashvalue is: "+ary[1]);
//				long hashvalue = Long.parseLong(ary[1].substring(33),16);
				out.print(segCount);
				out.print(",");
				out.print(name);
				out.print(",");
				out.print(chunkCount2);
				out.print(",");
				out.print(lineChunkInfo[2]);
				out.print(",");
				out.print(ary[1]);
				out.println();		
				
//				if (check(hashvalue))//if the lower n bits of fingerprint are zero
//                {
//					hookGuarantee = true;
//                    out.print(ary[1]);
//    				out.print(",");
//                    out.println((long)0);
//                }else if (chunkCount == chunkNumber && hookGuarantee == false){
//                	out.print(ary[1]);
//    				out.print(",");
//                    out.println((long)0);
//                }else{
//                	out.println("N/A");
//                }
				
				if(chunkCount == chunkNumber){
					segCount ++; //increase the tag of segment
					chunkCount = 0; //set chunkCouter to zero
					out.close();
					out = new PrintWriter(outputpath+"/Segment_"+ segCount);
					out.println("Segment No,FileName,ChunkName,ChunkSize,HashValue,SampleTag,Hitting Counter");
					hookGuarantee = false;
//					chunkCount++;
				}
				if(chunkCount2 == chunkNo && inputfileInfo.hasNextLine()){
					infor = inputfileInfo.nextLine().split(",");
					name = infor[1];
					chunkCount2 = 0;
					chunkNo = Integer.parseInt(infor[3]);
//					fileCount = Integer.parseInt(infor[0]);
					
				}
				
			}
			System.out.println("Segmentation and Sampling finished! Total " + segCount + " Segments.\nThe results are recoreded in "
								+ outputpath);
			out.close();
			inputStream.close();
			inputchunkInfo.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	

	
	
	

	
}
