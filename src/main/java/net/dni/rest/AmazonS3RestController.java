package net.dni.rest;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import net.dni.Loggable;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/rest")
public class AmazonS3RestController {

    @Autowired
    AmazonS3 amazonS3;

    @Value("${aws.bucketName}")
    String BUCKET_NAME;

    @Loggable
    private Logger logger;

    /**
     * List objects in default bucket
     *
     * @return
     */
    @RequestMapping(value = "/list", method = RequestMethod.GET, produces = "application/json")
    public ObjectListing list() {
        return amazonS3.listObjects(BUCKET_NAME);
    }

    /**
     * View metadata of object in default bucket
     *
     * @param key filename
     * @return
     */
    @RequestMapping(value = "/metadata/{key}/", method = RequestMethod.GET, produces = "application/json")
    public S3Object getObjectMetadata(@PathVariable("key") String key) {
        logger.info("key: {}", key);
        return amazonS3.getObject(BUCKET_NAME, key);
    }

    /**
     * delete object in default bucket
     *
     * @param key filename
     */
    @RequestMapping(value = "/delete/{key}/", method = RequestMethod.DELETE, produces = "application/json")
    public void delete(@PathVariable("key") String key) {
        logger.info("key: {}", key);
        amazonS3.deleteObject(BUCKET_NAME, key);
    }

    /**
     * Read object content as String in default bucket
     *
     * @param key filename
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/download/{key}/", method = RequestMethod.GET)
    public String download(@PathVariable("key") String key) throws IOException {
        logger.info("key: {}", key);
        S3Object object = amazonS3.getObject(BUCKET_NAME, key);
        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line);
        }
        return content.toString();
    }

    /**
     * Return a pre-signed url to download the object from AWS
     *
     * @param key
     * @return
     */
    @RequestMapping(value = "/download-url/{key}", method = RequestMethod.GET)
    public URL getPreSignedDownloadUrl(@PathVariable("key") String key) {

        Date expiration = new Date();
        long msec = expiration.getTime();
        msec += 1000 * 60; // 1 min.
        expiration.setTime(msec);

        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(BUCKET_NAME, key);
        generatePresignedUrlRequest.setMethod(HttpMethod.GET); // Default.
        generatePresignedUrlRequest.setExpiration(expiration);

        return amazonS3.generatePresignedUrl(generatePresignedUrlRequest);
    }

    /**
     * Stream object content in default bucket
     *
     * @param key      filename
     * @param response HttpResponse
     * @throws IOException
     */
    @RequestMapping(value = "/stream/download/{key}/", method = RequestMethod.GET)
    public void streamDownload(@PathVariable("key") String key, HttpServletResponse response) throws IOException {
        logger.info("key: {}", key);
        S3Object object = amazonS3.getObject(BUCKET_NAME, key);

        response.addHeader("Content-disposition", "attachment;filename=" + object.getKey());
        response.setContentType(object.getObjectMetadata().getContentType());

        InputStream stream = object.getObjectContent();
        IOUtils.copy(stream, response.getOutputStream());
        response.flushBuffer();
    }

    /**
     * Stream upload multipart files to default bucket
     * add User getObjectMetadata
     *
     * @param files
     * @throws IOException
     */
    @RequestMapping(value = "/stream/upload", method = RequestMethod.POST)
    public void streamUploadMultipleFile(@RequestParam("file") MultipartFile[] files) throws IOException {
        for (MultipartFile file : files) {
            logger.info("fileupload:{}, size:{}", file.getOriginalFilename(), file.getSize());
            if (!file.isEmpty()) {
                try {
                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentLength(file.getSize());
                    metadata.setContentType(file.getContentType());
                    metadata.addUserMetadata("user", "dni");
                    PutObjectRequest request = new PutObjectRequest(BUCKET_NAME, file.getOriginalFilename(), file.getInputStream(), metadata).withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
                    amazonS3.putObject(request);
                } catch (AmazonServiceException e) {
                    logger.error("", e);
                }
            }
        }
    }

    @RequestMapping(value = "/stream/multipart-upload", method = RequestMethod.POST)
    public void streamUploadMultipleFileIntoOneFile(@RequestParam("file") MultipartFile[] files, @RequestParam("filename") String fileName) throws IOException {
        // Create a list of UploadPartResponse objects. You get one of these for each part upload.
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Step 1: Initialize.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(BUCKET_NAME, fileName);
        initRequest.setSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
        InitiateMultipartUploadResult initResponse = amazonS3.initiateMultipartUpload(initRequest);

        int part = 1;
        int filePosition = 0;

        for (MultipartFile file : files) {
            logger.info("fileupload:{}, size:{}", file.getOriginalFilename(), file.getSize());
            if (!file.isEmpty()) {
                File convFile = new File(file.getOriginalFilename());
                file.transferTo(convFile);

                // Create request to upload a part.
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(BUCKET_NAME).withKey(fileName)
                        .withUploadId(initResponse.getUploadId()).withPartNumber(part)
                        .withFile(convFile)
                        .withPartSize(convFile.length());

                // Upload part and add response to our list.
                partETags.add(amazonS3.uploadPart(uploadRequest).getPartETag());

                part++;
                filePosition += file.getSize();
            }
        }

        // Step 3: Complete.
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(BUCKET_NAME, fileName, initResponse.getUploadId(), partETags);
        amazonS3.completeMultipartUpload(compRequest);
    }

}
