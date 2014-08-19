/**
 * Copyright (C) 2010-2014 Think Big Analytics, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package thinkbig.util;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

/**
 * Generic Utility class 
 * @author sandipandey
 *
 */
public class Util
{	
	static Pattern s3UriPattern = Pattern.compile("s3[n]*://(.*)//(.*)", Pattern.CASE_INSENSITIVE);
   
	/**
	 * get an Amazon s3 client
	 * @param credentialFile AWS credential properties file
	 * @return an s3 client, if credential is valid, otherwise return null
	 */
	public static AmazonS3 getAmazonS3Client(String credentialFile) {
		
		AmazonS3 s3 = null;
		
		try {
			s3 = new AmazonS3Client(new PropertiesCredentials(new File(credentialFile)));
		} catch (FileNotFoundException e) {
			System.out.println("File '" + credentialFile + "' not found!");	
		} catch (IOException e) {
			System.out.println("Could not open File '" + credentialFile + "'!");	
		} catch (Exception e) {
			System.out.println("Could not start Amazon Client!");
		}
		
		return s3;
	}
	
	/**
	 * returns (and prints) all (only non-empty objects if excludeBlanks is true) objects 
	 * in an S3 bucket as list of Strings
	 * null if no such bucket exists
	 * @param s3
	 * @param bucketName
	 * @param excludeBlanks
	 * @return
	 */
    public static List<String> getAllObjectKeysInBucket(AmazonS3 s3, String bucketName, boolean excludeBlanks) {
    	
    	// check if the client is valid
    	if (s3 == null) {
    		System.out.println("Not a valid S3 Client");
    		return null;
    	}
    	// check if the bucket exists
	if (!s3.doesBucketExist(bucketName)) {
		System.out.println("The bucket '" + bucketName + "' does not exist!");	
		return null;
	}
    	System.out.println("Listing objects in bucket '" + bucketName + "' ");
	ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
	if (objectListing == null) {
		return null;
	}
	List<String> objectKeys = new ArrayList<String>();
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
		String key = objectSummary.getKey();
		if (!excludeBlanks || objectSummary.getSize() > 0) {
			objectKeys.add(key);
			System.out.println(" - " + key + "  "  +  "(size = " + objectSummary.getSize() + ")");
		}
	}
        return objectKeys;
    }  
    
    /**
	 * returns (and prints) all objects in an S3 bucket as list of Strings
	 * @param s3
	 * @param bucketName
	 * @return
	 */
	public static List<String> getAllObjectKeysInBucket(AmazonS3 s3, String bucketName) {
		return getAllObjectKeysInBucket(s3, bucketName, false);
	}	   
	
    /**
     * get all the objects in the folder from the S3 bucket
     * @param s3
     * @param bucketName
     * @param folderName
     * @return list of objects
     */
    public static List<String> getAllObjectsInfolder(AmazonS3 s3, String bucketName, String folderName) {
    	List<String> objectKeys = getAllObjectKeysInBucket(s3, bucketName);
    	if (objectKeys == null) {
    		return null;
    	}
    	List<String> filteredObjects = new ArrayList<String>();
    	for (String object: objectKeys) {
			if (object.indexOf(folderName) == 0) {
				filteredObjects.add(object);
			}
		}
    	return filteredObjects;
    }
    
    /**
     * Check if all the (top level) objects in the list exist in the S3 bucket
     * @param s3
     * @param bucketName
     * @param objectKeys
     * @return true, if all objects in the list exist
     */
    public static boolean checkIfAllObjectsExist(AmazonS3 s3, String bucketName, List<String> objectKeys) {
    	
    	// check if the client is valid
    	if (s3 == null) {
    		System.out.println("Not a valide S3 Client");
    		return false;
    	}
    	
    	List<String> objectsInBucket = getAllObjectKeysInBucket(s3, bucketName);
    	if (objectsInBucket == null) {
    		return false;
    	}
    	for (String object:objectKeys) {
	    	if (!objectsInBucket.contains(object)) {
				System.out.println("'" + object + "' does not exist in the bucket '" + bucketName + "' on S3!");
				return false;
			}
    	}
    	return true;
    } 
    
    /**
     * Check if all the S3 Uris exist
     * @param s3
     * @param s3Uris
     * @return true, if all Uris in the list exist
     */
    public static boolean checkIfUrisExist(AmazonS3 s3, List<String> s3Uris) {
    	
    	// check if the client is valid
    	if (s3 == null) {
    		System.out.println("Not a valide S3 Client");
    		return false;
    	}
    	
    	boolean found = true;
    	for (String s3Uri:s3Uris) {
    		  Matcher matcher = s3UriPattern.matcher(s3Uri);
    		  if (matcher.find()) {
    			 String bucket = matcher.group(1);
    			 String key = matcher.group(2);
    			 found &= s3.doesBucketExist(bucket) & (s3.getObject(bucket, key) != null);
    			 if (!found) {
    				return false;
    			 }
    		  }
    		  else {
    			  return false;
    		  }
     	}
    	return true;
    } 
    
	/**
	 *  Check if there is a 1-1 correspondence between input and output
		e.g., if the input folder is 'input' and the output folder is 'output' (in the
		S3 bucket 'test-job-bucket') and
		there is an input file with 	  input-path  'input/2011/08/01/*.txt'
		there must be an output file with output-path 'output/2011/08/01/*.txt'
		assumption: the lists are ordered in the same manner (generated using the AWS list API)

		1-1 correspondence (executable just copies the file, file name may be different)
		input/2011/11/02/in/in2/in3/tfidf1	<->	output/2011/11/02/in/in2/in3/tfidf1
		input/2011/11/02/in/in2/tfidf1		<->	output/2011/11/02/in/in2/tfidf1
		input/2011/11/02/in/tfidf1			<->	output/2011/11/02/in/tfidf1
		input/2011/11/02/tfidf1				<->	output/2011/11/02/tfidf1
		input/2011/11/02/tfidf2				<->	output/2011/11/02/tfidf2
		input/2011/11/03/input				<->	output/2011/11/03/input
		input/2011/11/03/input1				<->	output/2011/11/03/input1
		input/2011/11/03/tfidf				<->	output/2011/11/03/tfidf

	 * @param iFiles: input files
	 * @param oFiles: output files
	 * @return true: if there is a 1-1 correspondence between the input files and the output files
	 */
    public static boolean validateInputOutput1_1Correspondence(String iPath, String oPath, 
    		List<String> iFiles, List<String> oFiles) {
		
		// number of input and output files must be same
   		if (iFiles.size() != oFiles.size()) {
   			return false;
   		}
   		// check 1-1 correspondence
   		for (int i = 0; i < iFiles.size(); ++i) {
   			String iFile = iFiles.get(i);
   			String oFile = oFiles.get(i);   			
   			if (!iFile.substring(0, iFile.indexOf('/')).equals(iPath) ||
   				!oFile.substring(0, oFile.indexOf('/')).equals(oPath) ||
   				!iFile.substring(iFile.indexOf('/'), iFile.lastIndexOf('/')).equals
   				(oFile.substring(oFile.indexOf('/'), oFile.lastIndexOf('/')))) {
   				return false;
   			}
   		}
   		return true;
	}
    
    /**
     * find if a date is in falls in a date range
     * @param date
     * @param start
     * @param end
     * @return true, if it falls in the range (inclusive)
     */
    public static boolean isDateWithinDates(Date date, Date start, Date end) {
    	Calendar curDate = Calendar.getInstance();
    	curDate.setTime(date);
    	Calendar startDate = Calendar.getInstance();
    	startDate.setTime(start);
    	Calendar endDate = Calendar.getInstance();
    	endDate.setTime(end);
    	return curDate.equals(startDate) || curDate.equals(endDate) ||
    		   curDate.after(startDate) && curDate.before(endDate); 
    }

    /**
     * Recursively enumerate all files inside the directory dirPath in FileSystem fs
     * @param fs
     * @param dirPath
     * @param files
     * @throws IOException
     */
	public static void getAllFiles(FileSystem fs, String dirPath, List<String> files) throws IOException {

		Path loc = new Path(dirPath);
		FileStatus[] statuses = fs.listStatus(loc);
		if (statuses != null) {
			int i = 0;
			for (FileStatus status : statuses) {
			    String file = statuses[i++].getPath().toString();
			    if (fs.isDirectory(new Path(file))) {
				getAllFiles(fs, file, files);	
			    }
			    else if (files.indexOf(file) == -1) { // if not already there
				System.out.println(file);
				files.add(file);
			    } 
			}
		}
	}

    /**
     * Recursively enumerate all files inside the directory dirPath
     * @param dirPath
     * @param job
     * @return list of files
     * @throws IOException
     */
	public static List<String> enumerateDistributedFiles(String dirPath, JobConf job) throws IOException {

		FileSystem fs;
		if (dirPath.contains(":")) {
		    try {
		    	fs = FileSystem.get(new URI(dirPath), job);
		    }
		    catch (URISyntaxException e) {
		    	throw new IOException(e);
		    }
		} else {
		    fs = FileSystem.get(job);
		}
		System.out.println("Path: " + dirPath);

		List<String> files = new ArrayList<String>();
		getAllFiles(fs, dirPath, files);
		return files;
	}
	
   /**
     * add days to a given date
     * @param dateStr (in inputDateFormt)
     * @param nDays
     * @return modified date (in outputDatFormat)
     * e.g., addDays("2008-02-28", 1, "yyyy-MM-dd", "MM/yy/dd") returns "02/08/29"
     * 		 addDays("2008-02-28", -29, "yyyy-MM-dd", "MM/yy/dd") returns "01/08/31"
     * 		 addDays("2008-02-28", 0, "yyyy-MM-dd", "MM/yy/dd") returns "02/08/28" (simply converts the date format)
     */
    public static String addDays(String dateStr, int nDays, String inputDateFormat, String outputDateFormat) {
	 
    	try {
		
    		DateFormat inputFormatter = new SimpleDateFormat(inputDateFormat);
    		DateFormat outputFormatter = inputDateFormat.equals(outputDateFormat) ? 
    				   					 inputFormatter : new SimpleDateFormat(outputDateFormat);
    		
    		Date date = (Date)inputFormatter.parse(dateStr);  
   
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			calendar.add(Calendar.DATE, nDays);
		
			return outputFormatter.format(calendar.getTime());
			 
	     } catch (ParseException e) {
      		
	    	 return null;
         }  
    }
    
    /**
     * add days to a given date
     * InputDateFormat = OutputDateFormat
     * @param dateStr
     * @param nDays
     * @param dateFormat
     * @return modifed date
     * e.g., addDays("2008-02-28", 1, "yyyy-MM-dd") returns 2008-02-29
     */
    public static String addDays(String dateStr, int nDays, String dateFormat) {
    	return addDays(dateStr, nDays, dateFormat, dateFormat);
    }
    
    /**
     * add days to a given date
     * InputDateFormat = OutputDateFormat = yyyy-MM-dd
     * @param dateStr
     * @param nDays
     * @return modified date
     * e.g., addDays("2008-02-28", 1, "yyyy-MM-dd") returns 2008-02-29
     */
    public static String addDays(String dateStr, int nDays) {
    	return addDays(dateStr, nDays, "yyyy-MM-dd");
    }

    /**
	 * convert the input date from source to destination format
	 * @param date
	 * @param sourceFormat
	 * @param destinationFormat
	 * @return date in the destination format
	 * @throws java.text.ParseException
	 */
    public static String convertDate(String date, String sourceFormat, String destinationFormat) throws java.text.ParseException {
	    String returnDateString = "";
	   
	    SimpleDateFormat formatter = new SimpleDateFormat(sourceFormat);
	    Date returnDate = formatter.parse(date);
	 
	    formatter = new SimpleDateFormat(destinationFormat);
	    returnDateString = formatter.format(returnDate);
	  
	    return returnDateString;
    }
}
