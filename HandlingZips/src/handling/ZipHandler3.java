package handling;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class ZipHandler3 {
	public String tiffToHDFS(String zipLocalFilePath, String dirHDFS) throws IOException {
        ArrayList<String> tiffsLocalFilePaths = new ArrayList<String>();
        String tiffInHDFS = "";
        TiffUnzipper unzipper = new TiffUnzipper();
        try {
        	tiffsLocalFilePaths = unzipper.unzip(zipLocalFilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
        if (tiffsLocalFilePaths.isEmpty()) {
        	System.out.println("No tiff found!");
        }
        else if (tiffsLocalFilePaths.size() == 1) {
        	System.out.println("Only one tiff found, procceding as usual");
        	tiffInHDFS = tiffLocalToHDFS(tiffsLocalFilePaths.get(0), dirHDFS);
        }
        else {
        	System.out.println("More than one tiffs found, wait for file-manipulation");
        	for (int i = 0; i < tiffsLocalFilePaths.size(); i++) {
        		String[] stringParts = tiffsLocalFilePaths.get(i).split("-");
        		int parts = stringParts.length;
            	if (stringParts[3].equals("vv") || stringParts[parts - 1].equals("001")){
            		tiffInHDFS = tiffLocalToHDFS(tiffsLocalFilePaths.get(i), dirHDFS);
            		System.out.println("Tiff with vv polarization will be stored in HDFS");
            	}
            	else {
            		File unwantedTiff = new File(tiffsLocalFilePaths.get(i));
            		if (unwantedTiff.exists()) {
            			unwantedTiff.delete();
            		}
            		File origZip = new File(zipLocalFilePath);
            		String origName = origZip.getName();
            		String parentName = origZip.getParent();
            		String origPath = origZip.getAbsolutePath();
            		String alterNamePath = origPath + "ORIG";
            		File alteredZip = new File(alterNamePath);
            		if(origZip.renameTo(alteredZip)) {
            	        System.out.println("renamed");
            	    } 
            		else {
            	         System.out.println("Error");
            		}
                    Zip zip = new Zip();
                    String[] nameParts = origName.split("\\.");
                    String safeName = nameParts[0] + ".SAFE";
                    String dirToZip = parentName + File.separator + safeName;
                    String finalName = parentName + File.separator + nameParts[0] + ".zip";
                    System.out.println("Directory to be zipped:  " + dirToZip);
                    System.out.println("Final Zip's name:  " + finalName);
                    zip.compressDirectory(dirToZip, finalName);
            	}
        	}
        }
        return tiffInHDFS;

    }

      private String tiffLocalToHDFS(String tiffLocalFilePath, String HDFSdir) throws IOException {
//        //System.out.println("");
//        System.out.println("~~~ Initiating Local To HDFS ~~~");
//        //System.out.println("IMPORTING TO HDFS THE FILE:");
//        //System.out.println(tiffLocalFilePath);
//        //*** Copying the extracted tiff to HDFS
//        //Tiff from local FS -> IpnutStream -> (target) HDFS
//        InputStream instream = new BufferedInputStream(new FileInputStream(tiffLocalFilePath));
//        //Get configuration of Hadoop system
//        Configuration conf = new Configuration();
//        //conf.set("fs.default.name", "hdfs://localhost:54310");
//        //System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));	//check code
//        //Finding tiff's name
//        //System.out.println("");
//        //System.out.println("TIFF'S NAME:");
//        File tiffFile = new File(tiffLocalFilePath);
//        String tiffName = tiffFile.getName();
//        //System.out.println(tiffName);
//        //System.out.println("");
//        //Defining tiff's destination in HDFS
//        String tiffInHDFS = HDFSdir + File.separator + tiffName;
//        //Creating tiff's place in HDFS
//        FileSystem fs = FileSystem.get(URI.create(tiffInHDFS), conf);
//        OutputStream out = fs.create(new Path(tiffInHDFS));
//        //Copy file from local to HDFS
//        IOUtils.copyBytes(instream, out, 65536, true);
//        //checking
//        //System.out.println("TIFF IS COPIED TO HDFS AND HAS THE HDFS-FILEPATH: ");
//        //System.out.println(tiffInHDFS);
//        //System.out.println("");
//        //System.out.println("~~~ Completing Local To HDFS ~~~");
//        //System.out.println("");
//        //System.out.println("");
//        return tiffInHDFS;
    	  return "if this is returned, everything is fine";
//
      }

//    public Product findTargetProduct(String zipLocalFilePath) {
//        //System.out.println("");
//        System.out.println("~~~ Initiating Extract TP ~~~");
//        File zipFile = new File(zipLocalFilePath);
//        //Product targetProduct = processImages2(zipFile);
//        MyRead myRead = null;
//        Product targetProduct = null;
//        try {
//            myRead = new MyRead(zipFile, "read");
//            targetProduct = myRead.getTargetProduct();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        return targetProduct;
//    }

}
