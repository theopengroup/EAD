import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Hashtable;



public class fileProcessing {

int boundaryCheck;
@SuppressWarnings("rawtypes")
Hashtable table;
File inputfile;

//File outputfile;
String outputAdd;
int expectSize;  //KB
PrintWriter fileMan;
PrintWriter ChunkMan;

fileProcessing(File inputfile, String outputAdd, int size,PrintWriter fileMan,PrintWriter ChunkMan){
	this.inputfile =inputfile;
	this.expectSize=size*1024;
//	this.outputfile = outputfile;
	this.outputAdd=outputAdd.concat("/");
	this.fileMan = fileMan;
	this.ChunkMan = ChunkMan;
}

void chunkFile(){
	int chunkTag = 1;
	int window_size=48; //byte
//	int max = (int)inputfile.length();
	int max = expectSize*2;
	int min = expectSize/2;
    RabinHashFunction64 rabin_fingerprint = new RabinHashFunction64();
    long bytesLeft = inputfile.length();
    System.out.println("The input file size is: " + bytesLeft);
    try {
        FileInputStream fis = new FileInputStream(inputfile.getPath());
        byte[] buffer1 = new byte[window_size - 1]; 
        //buffer which holds all bytes in window except the last byte
        byte buffer2; //buffer holds last byte of window
        
        byte[] window = new byte[window_size]; //holds all bytes in the current window
        byte[] chunk = new byte[max];
        while (bytesLeft > 0)//continue to break up the file while there are still bytes left
        {

            int counter = 0; //counter will hold the size of the chunk
            for(int i = 0; i < chunk.length; i++) //clear the chunk
                chunk[i] = 0;
            if (bytesLeft <= min) { //if the bytes left is less than or equal to the minimum chunk size, create this as a chunk
                chunk = new byte[(int) bytesLeft];
                fis.read(chunk);
                counter = chunk.length;
            }
            else {
                for (int i = 0; i < (min - window_size); i++) { 
                	//read in bytes up to minimum chunk size minus the window size
                    chunk[i] = (byte) fis.read();
                    counter++;
                }
                fis.read(buffer1); //fill first buffer with window_size - 1 bytes
                
                for (int i = 0; i < buffer1.length; i++) { //add these bytes to the chunk
                    chunk[counter] = buffer1[i];
                    counter++;
                }
                boolean bound = false; //boundary flag
                while (!bound && counter != max)
                	//continue looping until a bound marked or the max size is reached
                {
                    buffer2 = (byte) fis.read(); //read in the last byte of the window
                    if (buffer2 != -1) {//if last byte contains data
                        for (int i = 0; i < buffer1.length; i++) //fill the window until the last byte
                        {
                            window[i] = buffer1[i];
                        }
                        window[window_size - 1] = buffer2; //add the last byte to the window
                        long fingerprint = rabin_fingerprint.hash(window); //calculate the fingerprint of the window
                        if (check(fingerprint))//if the lower n bits of fingerprint are zero
                        {
                            bound = true;
                        }
                        //chunk.add(buffer2); //add the last byte to the chunk
                        chunk[counter] = buffer2;
                        counter++;
                        for (int i = 0; i < buffer1.length; i++) //shift the window right
                        {
                            buffer1[i] = window[i + 1];
                        }
                    } else //if the last byte did not contain data, end of file is reached and set boundary
                    {
                        bound = true;
                    }
                }

            }

                      
          OutputStream out = new FileOutputStream(outputAdd+chunkTag);
 //           System.out.println("The size of this chunk is: " + counter);    
          ChunkMan.print(inputfile.getName());
          ChunkMan.print(",");
          ChunkMan.print(chunkTag);
          ChunkMan.print(",");
          ChunkMan.println(counter);
           out.write(chunk, 0, counter);
           

            out.close();
            chunkTag++;            
//            for(int i = 0; i < chunk.length; i++)
//                System.out.print((char)chunk[i]);
//            System.out.println();
            bytesLeft -= counter;
            
        }
        fileMan.print(chunkTag-1);
        fileMan.println();
        fis.close();
        //System.out.println("dupes within file is " + dupes + " number of stored chunks is " + table.size());
    } catch (FileNotFoundException e) {
        System.out.println(e);
    } catch (IOException e) {
        System.out.println(e);
    }
//    System.out.println("Chunking finished! The results are recorded in "+outputAdd);
}

//method to check the lower n bits of fingerprint to determine boundary
private boolean check(long fingerprint) {
	boundaryCheck = (int)  (Math.log(expectSize) / Math.log(2));
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




