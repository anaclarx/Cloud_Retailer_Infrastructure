package retailer;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

public class SQSHandler {

	public static void main(String args[]) {
		
	    if (args.length < 3) {
	        System.out.println("Missing arguments");
	        System.exit(1);
	      }
		
		Region region = Region.US_EAST_1;
		
		String bucketName = args[0];
		
		String fileName = args[1];
		
		String queueURL = args[2];
		
		
		SqsClient sqsClient = SqsClient.builder().region(region).build();

		SendMessageRequest sendRequest = SendMessageRequest.builder().queueUrl(queueURL)
				.messageBody(bucketName + ";" + fileName).build();

		SendMessageResponse sqsResponse = sqsClient.sendMessage(sendRequest);

		System.out.println(
				sqsResponse.messageId() + " Message sent. Status is " + sqsResponse.sdkHttpResponse().statusCode());
		
		
	}
}
