package com.splicemachine.cs.databasemigration.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.lang.StringUtils;


public class ParseImportLogFile {

    
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
            System.out.println("You need to pass in a directory or a file and the output directory or filename");
            return;
        }
        ParseImportLogFile gc = new ParseImportLogFile();
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
    
    public ParseImportLogFile() {
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
    
    public void writeResultOut(SpliceImportDetails result) {
        out.print(result.schemaName);
        out.print(DELIM);
        out.print(result.tableName);
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
        SpliceImportDetails currentImport = null;
        
        BufferedReader br = new BufferedReader(new FileReader(fullFile));
        while((line = br.readLine()) != null ) {
            
            //Check to see if the line contains an import
            String temp = line.toLowerCase();
            if(temp.indexOf("call syscs_util.import_data") > -1) {               
                if(currentImport != null) {
                    writeResultOut(currentImport);
                    currentImport = new SpliceImportDetails();
                } else {
                    currentImport = new SpliceImportDetails();
                }
                
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
    
}

class SpliceImportDetails {
    String schemaName = null;
    String tableName = null;
    String numFiles = null;
    String numTasks = null;
    String numRowsImported = null;
    String numBadRecords = null;
    String elapsedtime = null;
    boolean success = false;
    String errorMessage = "";
}
