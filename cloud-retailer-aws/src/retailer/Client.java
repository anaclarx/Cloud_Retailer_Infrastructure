package retailer;

import java.io.File;
 
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
 
public class Client {
    public static void main(String[] args) {
    	
    	File dir = new File("/Users/caca/Desktop/Cloud_Retailer_Infrastructure/sales-data/");
    	
    	File[] directoryListing = dir.listFiles();
    	
    	if (directoryListing != null) {
    		for (File child : directoryListing) {
    			
    			String bucketName = "retailerbucket2212";
    	        
    	        String fileName = child.getName();
    	         
    	        String filePath = "/Users/caca/Desktop/Cloud_Retailer_Infrastructure/sales-data/" + fileName;
    	         
    	        S3Client client = S3Client.builder().build();
    	         
    	        PutObjectRequest request = PutObjectRequest.builder()
    	                            .bucket(bucketName).key(fileName).build();
    	         
    	        client.putObject(request, RequestBody.fromFile(new File(filePath)));
    		 }
    	} else {
    		return;
    		  }
    	

                 
    }
}



  
  