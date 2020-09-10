package com.splicemachine.cs.databasemigration.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang.StringUtils;


public class ParseSqoopAndImportLogFile {

    /*
     * 15/12/09 17:49:58 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-mapr-1509
     * 15/12/09 17:49:10 DEBUG orm.ClassWriter: Table name: ACARRIN.JZM_ADHOC_ZIP
     * ..
     * 15/12/09 17:49:41 INFO mapreduce.Job: Job job_1449608316379_0032 completed successfully
 
                Map input records=50978
                Map output records=50978
                Input split bytes=87
                Spilled Records=0
                Failed Shuffles=0
                Merged Map outputs=0
                GC time elapsed (ms)=37
                CPU time spent (ms)=5960
                Physical memory (bytes) snapshot=368021504
                Virtual memory (bytes) snapshot=1790476288
                Total committed heap usage (bytes)=904396800
        File Input Format Counters
                Bytes Read=0
        File Output Format Counters
                Bytes Written=928705
15/12/09 17:49:42 INFO mapreduce.ImportJobBase: Transferred 0 bytes in 24.9303 seconds (0 bytes/sec)
15/12/09 17:49:42 INFO mapreduce.ImportJobBase: Retrieved 50978 records.
15/12/09 17:49:42 DEBUG util.ClassLoaderStack: Restoring classloader: sun.misc.Launcher$AppClassLoader@60dbf04d
15/12/09 17:49:42 INFO util.AppendUtils: Appending to directory JZM_ADHOC_ZIP
15/12/09 17:49:42 DEBUG util.AppendUtils: Filename: _SUCCESS ignored
15/12/09 17:49:42 INFO fs.MapRFileSystem: Cannot rename across volumes, falling back on copy/delete semantics
15/12/09 17:49:42 DEBUG util.AppendUtils: Filename: part-m-00000 repartitioned to: part-m-00000
15/12/09 17:49:42 DEBUG util.AppendUtils: Deleting temporary folder 237f70a50ad44a29a4f169cc2f788a4e_ACARRIN.JZM_ADHOC_ZIP
Sqoop Export Successful: ACARRIN.JZM_ADHOC_ZIP duration: 37

 
     */
    
    
    //
    /*
    splice> elapsedtime on;
    splice> call syscs_util.import_data('MDBADMIN','ABINITIO_COMMIT_TABLE',null,'/data/sqoop/MDBADMIN/ABINITIO_COMMIT_TABLE/',',',null,'yyyy-MM-dd HH:mm:ss.S','yyyy-MM-dd HH:mm:ss.S',null,1,'/bad/MDBADMIN/ABINITIO_COMMIT_TABLE');
    numFiles   |numTasks   |numRowsImported     |numBadRecords
    -----------------------------------------------------------------
    1          |0          |852                 |0

    1 row selected
    ELAPSED TIME = 8361 milliseconds
    
    splice> call syscs_util.import_data('MDBLOOKUP','PRODUCT',null,'/data/sqoop/MDBLOOKUP/PRODUCT/',',',null,'yyyy-MM-dd HH:mm:ss.S','yyyy-MM-dd HH:mm:ss.S',null,1,'/bad/MDBLOOKUP/PRODUCT');
ERROR SE009: Too many bad records in import

    
*/
    public static final String DELIM = ",";
    
    //Directory where the csv file should be written to
    String outputPath = null;
    String inputPathOrFile = null;
    PrintWriter out = null;
    
    public static void main(String[] args) {
        System.out.println(" **** Beginning process");
        if(args.length != 2) {
            System.out.println("You need to pass in a directory or a file and the output directory");
            return;
        }
        ParseSqoopAndImportLogFile gc = new ParseSqoopAndImportLogFile();
        try {
            gc.outputPath = args[1];
            System.out.println("Output Path:" + gc.outputPath);
            gc.setup();
            gc.beginProcess(args[0]);
            gc.cleanup();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(" **** End process");
    }
    
    public ParseSqoopAndImportLogFile() {
        super();
    }
    
    public void setup() throws FileNotFoundException {
        File outputFile = new File(outputPath);
        if(outputFile.isFile()) {
            if(!outputFile.getParentFile().exists()) {
                outputFile.getParentFile().mkdirs();
            }
            out = new PrintWriter(outputFile);
        } else {
            if(!outputFile.exists()) {
                outputFile.mkdirs();
            }
            out = new PrintWriter(new File(outputPath,  "import_log_results.csv"));
        }
        out.print("Schema Name");
        out.print(DELIM);
        out.print("Table Name");
        out.print(DELIM);
        out.print("Sqoop Records Read");
        out.print(DELIM);
        out.print("Sqoop Records Written");
        out.print(DELIM);
        out.print("Sqoop Bytes Written");
        out.print(DELIM);
        out.print("Sqoop Duration End Time");
        out.print(DELIM);
        out.print("Number of Files");
        out.print(DELIM);
        out.print("Number of Tasks");
        out.print(DELIM);
        out.print("Number of Rows Imported");
        out.print(DELIM);
        out.print("Number of Bad Rows");
        out.print(DELIM);
        out.print("Elapsed Time (ms)");
        out.print(DELIM);
        out.print("Success");
        out.print(DELIM);
        out.println("Error Message");
    }
    
    public void cleanup() {
        out.close();
    }
    
    public void beginProcess(String directoryOrFile) throws IOException {
        
        File f = new File(directoryOrFile);
        if(f.isDirectory()) {
            File[] files = f.listFiles();
            for(File curFile : files) {
                beginProcess(curFile.getPath());
            }
        } else {
            System.out.println("processing file:" + f.getName());
            processFile(f);
        }
    }
    
    public void writeResultOut(SpliceSqoopImportDetails result) {
        out.print(result.schemaName);
        out.print(DELIM);
        out.print(result.tableName);
        out.print(DELIM);
        out.print(result.sqoopInputRecords);
        out.print(DELIM);
        out.print(result.sqoopOutputRecords);
        out.print(DELIM);
        out.print(result.bytesWritten);
        out.print(DELIM);
        out.print(result.sqoopDuration);
        out.print(DELIM);
        out.print(result.numFiles);
        out.print(DELIM);
        out.print(result.numTasks);
        out.print(DELIM);
        out.print(result.numRowsImported);
        out.print(DELIM);
        out.print(result.numBadRecords);
        out.print(DELIM);
        out.print(result.elapsedtime);
        out.print(DELIM);
        out.print(result.success);
        out.print(DELIM);
        out.println(result.errorMessage);
    }
    
    public void processFile(File fullFile) throws IOException {
        String line = null;
        SpliceSqoopImportDetails currentImport = null;
        
        BufferedReader br = new BufferedReader(new FileReader(fullFile));
        while((line = br.readLine()) != null ) {
            
            //Check to see if the line contains an import
            String temp = line.toLowerCase();
            if(temp.indexOf("table name:") > -1) {
                //Table name: ACARRIN.JZM_ADHOC_ZIP
                if(currentImport != null) {
                    writeResultOut(currentImport);
                    currentImport = new SpliceSqoopImportDetails();
                } else {
                    currentImport = new SpliceSqoopImportDetails();
                }
                
                currentImport.setFullTableName(getSubString(line,":"));
            } else if(temp.indexOf("map input records=") > -1) {
                //Map input records=50978
                currentImport.sqoopInputRecords = getSubString(line,"=");
                              
            } else if(temp.indexOf("map output records=") > -1) {
                //Map output records=50978
                currentImport.sqoopOutputRecords = getSubString(line,"=");
                              
            } else if(temp.indexOf("bytes Written=") > -1) {
                //Bytes Written=928705
                currentImport.bytesWritten = getSubString(line,"=");
                              
            } else if(temp.indexOf("mapreduce.importjobbase: transferred") > -1) {
                //mapreduce.ImportJobBase: Transferred 0 bytes in 24.9303 seconds (0 bytes/sec)
                int indexOfBytes = line.indexOf(" bytes in ");
                int indexOfSeconds = line.indexOf("seconds");
                
                if(indexOfBytes > -1 && indexOfSeconds > -1) {
                    currentImport.sqoopDuration = line.substring(indexOfBytes + 10,indexOfSeconds);
                }
                              
            } else if(temp.indexOf("call syscs_util.import_data") > -1) {

                
                //Now we want to parse out the fields
                int indexOfParen = line.indexOf("(");
                temp = line.substring(indexOfParen+1);

                String[] fields = StringUtils.split(temp,",");
                currentImport.schemaName = fields[0].replace("'", "");
                currentImport.tableName = fields[1].replace("'", "");
                
            } else if (line.startsWith("numFiles")) {
                line = br.readLine();
                line = br.readLine();
                String[] vals = StringUtils.split(line,"|");
                if(vals.length != 4) {
                    System.out.println("Error splitting on a pipe");
                } else {
                    currentImport.numFiles = vals[0].trim();
                    currentImport.numTasks = vals[1].trim();
                    currentImport.numRowsImported = vals[2].trim();
                    currentImport.numBadRecords = vals[3].trim();
                    if(currentImport.numBadRecords.equals("0")) {
                        currentImport.success = true;
                    }
                }
            } else if (line.startsWith("ELAPSED TIME")) {
                //ELAPSED TIME = 8361 milliseconds
                int indexOfEquals = line.indexOf("=");
                int indexOfMilliseconds = line.indexOf(" milliseconds");
                if(indexOfEquals > -1 && indexOfMilliseconds > -1) {
                    currentImport.elapsedtime = line.substring(indexOfEquals+1,indexOfMilliseconds);
                }
            } else if (line.startsWith("ERROR ")) {
                currentImport.errorMessage = line;
            }

        }
        writeResultOut(currentImport);
        br.close();
    }
    
    public String getSubString(String line, String delim) {
        String temp = "";
        int indexOfDelim = line.indexOf(delim);
        if(indexOfDelim > -1) {
            temp = line.substring(indexOfDelim).trim();
        }
        return temp;
    }
    
}

class SpliceSqoopImportDetails {
    String schemaName = null;
    String tableName = null;
    String sqoopInputRecords = "0";
    String sqoopOutputRecords = "0";
    String bytesWritten = "0";
    String sqoopDuration = null;
    String numFiles = "0";
    String numTasks = "0";
    String numRowsImported = "0";
    String numBadRecords = "0";
    String elapsedtime = "0";
    boolean success = false;
    String errorMessage = "";
    
    public void setFullTableName(String fullName) {
        if(fullName != null) {
            String[] nameParts = StringUtils.split(fullName, ".");
            if(nameParts.length == 2) {
                schemaName = nameParts[0];
                tableName = nameParts[1];
            } else {
                schemaName = nameParts[0];
            }
        }
    }
}
