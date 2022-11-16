package retailer;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import java.util.Hashtable;
import java.util.Set;
import java.io.FileWriter;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVWriter;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class WorkerJavaApplication{

  public static void main(String[] args) throws IOException {
	  
			Region region = Region.US_EAST_1;
			
			

	    	String queueURL = "https://sqs.us-east-1.amazonaws.com/148253347322/retailer-files-queue";
	    	
			
			SqsClient sqsClient = SqsClient.builder().region(region).build();
			
			ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(queueURL).maxNumberOfMessages(1)
					.build();
			
			List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
			
			Hashtable<String, Double> profit = new Hashtable<String, Double>();

			Hashtable<String, Integer> quantity = new Hashtable<String, Integer>();

			Hashtable<String, Double> sold = new Hashtable<String, Double>();

			Hashtable<String, Double> productsProfit = new Hashtable<String, Double>();

			if (!messages.isEmpty()) {
				Message msg = messages.get(0);
				String[] arguments = msg.body().split(";");
				String bucketName = arguments[0];
				String fileName = arguments[1];
				

				AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
				
				if (!messages.isEmpty()) {
					try(final S3Object s3Object = s3.getObject(bucketName, fileName);
							final InputStreamReader streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
							final BufferedReader reader = new BufferedReader(streamReader)) {
																	

							 reader.lines().forEach(line -> {
							    	try {
										String[] tempArr;
										tempArr = line.split(";");

										// get total profit by store
										if (profit.get(tempArr[1]) != null){
											
											profit.put(tempArr[1], Double.parseDouble(tempArr[6])*Integer.parseInt(tempArr[3]) + profit.get(tempArr[1]));
										} else {
											profit.put(tempArr[1], Double.parseDouble(tempArr[6])*Integer.parseInt(tempArr[3]));
										}

										// get total quantity by product by store
										if (quantity.get(tempArr[2]) != null){
											
											quantity.put(tempArr[2], quantity.get(tempArr[2]) + Integer.parseInt(tempArr[3]));
										} else {
											quantity.put(tempArr[2], Integer.parseInt(tempArr[3]));
										}					

										// get total sold by product by store
										if (sold.get(tempArr[2]) != null){
											sold.put(tempArr[2], sold.get(tempArr[2]) + Double.parseDouble(tempArr[7]));
										} else {
											sold.put(tempArr[2], Double.parseDouble(tempArr[7]));
										}

										// get total profit by product by store
										if (productsProfit.get(tempArr[2]) != null){
											productsProfit.put(tempArr[2], productsProfit.get(tempArr[2]) + Double.parseDouble(tempArr[6]));
										} else {
											productsProfit.put(tempArr[2], Double.parseDouble(tempArr[6]));
										}

								    }
							    	catch(Exception ex) 
							    	  {
							    	    System.out.println(ex); 
							    	  }
							      });
							
					         // File csvOutPutData = new File("/Users/caca/Desktop/França/Cloud/Cloud_Retailer_Infrastructure/sales-data/"+fileKey);
						      File csvOutPutData = File.createTempFile("output-"+fileName, ".csv");
						      try {
						    	  
						    	  FileWriter outputfile = new FileWriter(csvOutPutData);
								  CSVWriter writer = new CSVWriter(outputfile);
								  String[] header = {"Store", "Store Profit", "Product", "Product Profit", "Product Quantity", "Product Sold"};
								  writer.writeNext(header);
								  
								  Set<String> storeKeys = profit.keySet();
								  Set<String> productKeys = quantity.keySet();
								  String keyStore, storeProfit, keyProduct, productProfit, productQuantity, productSold;
								  
								  
								  for(String key: productKeys) {
									  System.out.println(key);
									  keyStore = (String) profit.keySet().toArray()[0];
									  storeProfit = profit.get(keyStore).toString();
									  keyProduct = key;
									  productProfit = productsProfit.get(key).toString();
									  productQuantity = quantity.get(key).toString();
									  productSold = sold.get(key).toString();
									  String[] data = {keyStore, storeProfit, keyProduct, productProfit, productQuantity, productSold};
									  writer.writeNext(data);
								  }
								  

								  writer.close();  
						      }
						      catch(IOException e){
						    	  System.out.println("IOException: " + e.getMessage());    	  
						      }
						      
						      s3.putObject("retailerjavapplication2212", csvOutPutData.getName() + "-output", csvOutPutData);


					for (Message message : messages) {
						DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder().queueUrl(queueURL)
								.receiptHandle(message.receiptHandle()).build();

						sqsClient.deleteMessage(deleteRequest);
					}
					}
				} else {
					System.out.println("File is not available in the Bucket");
				}
			
				  System.out.println("Finished... processing file");
			
		}
	}
}