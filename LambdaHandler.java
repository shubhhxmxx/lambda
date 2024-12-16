package com.easy2excel;

import java.util.Map;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


public class LambdaHandler implements RequestHandler<S3Event,Boolean> {

	private String ccilBucket="destinationbucketshubham";
	
    private final S3Client s3Client = S3Client.create();

	private String source_bucket="shubhhxmxxbucket";
	
	
	private String ccilBaseUrl="https://random-data-api.com";
	private String readCsvUrl="/api/v2/users";

	@Override
	public Boolean handleRequest(S3Event input, Context context) {
		final LambdaLogger logger=context.getLogger();
		//checking if we are getting any file
		logger.log("ccilBaseUrl;"+ccilBaseUrl);
		logger.log("ccilBucket:"+ccilBucket);
		logger.log("source_bucket:"+source_bucket);
		logger.log("S3Event:"+input.getRecords().toString());
		if(input.getRecords().isEmpty()) {
			logger.log("No Records Found");
			
		}
		//processing the records
		for(S3EventNotification.S3EventNotificationRecord record:input.getRecords()) {
			String bucketName= record.getS3().getBucket().getName();
			String key= record.getS3().getObject().getKey();
			if(bucketName.equalsIgnoreCase(source_bucket)) {
				logger.log("lambda triggered for file: " + key);
				 try {
					moveFile(source_bucket, ccilBucket, key);
				} catch (Exception e) {
						logger.log(e.getMessage());
				}
				//make a call to readCSV;
				try (CloseableHttpClient httpClient = HttpClients.createDefault()){
					String ccilReadCSVUrl = ccilBaseUrl.concat(readCsvUrl);
					logger.log("ccilReadCSVUrl:"+ccilReadCSVUrl);
					HttpPost httpPost=new HttpPost(ccilReadCSVUrl);
					httpPost.setHeader("content-type","application/json");
					try (CloseableHttpResponse response=httpClient.execute(httpPost)) {
						logger.log(response.toString());
						if(response.getStatusLine().getStatusCode()==200) {
							logger.log("readCsv call returned successfully to lambda for "+key);
							return true;
						}else {
							logger.log("readCsv call failed for :"+key);
							return false;
						}
					}
				} catch (Exception e) {
					logger.log("httpclient call failed for :"+key);

				}
			}else {
				logger.log("no csv files found in folder for lambda");
			}		
		}		
	return false;
	}
	private void moveFile(String sourceBucket, String destinationBucket, String sourceKey) throws Exception {
        try {
            // Copy the object to the destination bucket
            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .sourceBucket(sourceBucket)
                    .sourceKey(sourceKey)
                    .destinationBucket(destinationBucket)
                    .destinationKey(sourceKey)
                    .build();

            s3Client.copyObject(copyRequest);
            System.out.println("File copied to destination bucket: " + destinationBucket + "/" + sourceKey);

            // Delete the object from the source bucket
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();

            s3Client.deleteObject(deleteRequest);
            System.out.println("File deleted from source bucket: " + sourceBucket + "/" + sourceKey);

        } catch (S3Exception e) {
            System.err.println("S3 Exception: " + e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

}
