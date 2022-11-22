package retailer;

import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.util.ArrayList;
import java.util.Hashtable;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Consolidator {
    public static void main(String[] args) {
        Region region = Region.US_EAST_1;        
        
        String bucketName = "summaryretailerbucket2212";
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        
        S3Client s3 = S3Client.builder().region(region).build();
        
        if (args.length < 1) {
            System.out.println("Missing the file date argument");
            System.exit(1);
        }
        
    	ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(args[0]);

    	List<String> keys = new ArrayList<>();

    	ObjectListing objects = s3Client.listObjects(listObjectsRequest);

    	for (;;) {
    		List<S3ObjectSummary> summaries = objects.getObjectSummaries();
    		if (summaries.size() < 1) {
    			break;
    		}

    		summaries.forEach(s -> keys.add(s.getKey()));
    		objects = s3Client.listNextBatchOfObjects(objects);
    	}
    	
        // variable declaration
    	double profitRetailer = 0;
        String mostProfitableStore = new String();
        double biggerProfit = 0;
        String leastProfitableStore = new String();
        double smallestProfit = Double.POSITIVE_INFINITY;
        Hashtable<String, Double> profit = new Hashtable<String, Double>();
        Hashtable<String, Integer> quantity = new Hashtable<String, Integer>();
        Hashtable<String, Double> sold = new Hashtable<String, Double>();

    	for (String element : keys) {

                // Retrieve file
                GetObjectRequest objectRequest = GetObjectRequest.builder().key(element)
                    .bucket(bucketName).build();

                ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
                byte[] data = objectBytes.asByteArray();

                File file = new File(element);
                try (OutputStream os = new FileOutputStream(file)) {
                    os.write(data);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                // read what we wrote
                String dataString = new String(data, StandardCharsets.UTF_8);
               // System.out.println(dataString);

                // manipulating the data
                String[] tempArray = dataString.split("\\n");
                String[][] dataArray = new String[tempArray.length - 1][6];
                for (int i = 1; i < dataArray.length; i++){ //start in i = 1 not to get the header of the csv file
                    dataArray[i] = tempArray[i].split(",");
                    
                    //PRODUCTS PROFIT
                    
                    StringBuilder profitProduct = new StringBuilder(dataArray[i][3]);
					profitProduct.deleteCharAt(0);
					//System.out.println(dataArray[i][3]);
					profitProduct.deleteCharAt(dataArray[i][3].length() - 2);
					String profitFinal = profitProduct.toString();
					
					
					//QUANTITY PER PRODUCT
                    StringBuilder quantityProduct = new StringBuilder(dataArray[i][4]);
					quantityProduct.deleteCharAt(0);
					quantityProduct.deleteCharAt(dataArray[i][4].length() - 2);
					String quantityFinal = quantityProduct.toString();
					
					//SOLD PER PRODUCT
                    StringBuilder soldProduct = new StringBuilder(dataArray[i][5]);
					soldProduct.deleteCharAt(0);
					soldProduct.deleteCharAt(dataArray[i][5].length() - 2);
					String soldFinal = soldProduct.toString();
					
					
					
                    // profit per product
                    if (profit.get(dataArray[i][2]) != null){
						
                        profit.put(dataArray[i][2], Double.parseDouble( profitFinal) + profit.get(dataArray[i][2]));
                    } else {
                        profit.put(dataArray[i][2], Double.parseDouble(profitFinal));
                    }

                    // quantity per product
                    if (quantity.get(dataArray[i][2]) != null){
							
                        quantity.put(dataArray[i][2], Integer.parseInt(quantityFinal) + quantity.get(dataArray[i][2]));
                    } else {
                        quantity.put(dataArray[i][2], Integer.parseInt(quantityFinal));
                    }

                    // sold per product
                    if (sold.get(dataArray[i][2]) != null){
							
                        sold.put(dataArray[i][2], Double.parseDouble(soldFinal) + sold.get(dataArray[i][2]));
                    } else {
                        sold.put(dataArray[i][2], Double.parseDouble(soldFinal));
                    }
              }
                
                
                
				//PROFIT STORE
//                System.out.println(dataArray);
//                System.out.println(dataArray);
//                System.out.println("Loja em questao: "+ dataArray[1][0]);
                StringBuilder profitStoreProduct = new StringBuilder(dataArray[1][1]);
				profitStoreProduct.deleteCharAt(0);
				profitStoreProduct.deleteCharAt(dataArray[1][1].length() - 2);
				
				String profitStoreFinal = profitStoreProduct.toString();
				
				// Retailer's total profit
                profitRetailer = profitRetailer + Double.parseDouble(profitStoreFinal);
                
                // Most Profitable Store
                if (Double.parseDouble(profitStoreFinal) > biggerProfit){
                    biggerProfit = Double.parseDouble(profitStoreFinal);
                    mostProfitableStore = dataArray[1][0];
                }

                // Least Profitable Store
                if (Double.parseDouble(profitStoreFinal) < smallestProfit){
                    smallestProfit = Double.parseDouble(profitStoreFinal);
                    leastProfitableStore = dataArray[1][0];
                }
   
            }
        System.out.println("Total Retailer's Profit: " + profitRetailer);

        System.out.println("Least Profitable Store: " + leastProfitableStore);

        System.out.println("Most Profitable Store: " + mostProfitableStore);
        
    	System.out.println("Products Informations: ");
    	profit.forEach( (k, v) -> System.out.println("Product : " + k + ", Profit : " + v)); 
        quantity.forEach( (k, v) -> System.out.println("Product : " + k + ", Quantity : " + v)); 
        sold.forEach( (k, v) -> System.out.println("Product : " + k + ", Sold : " + v));
    	}

    }
