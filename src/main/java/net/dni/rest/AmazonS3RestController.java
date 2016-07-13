package net.dni.rest;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import net.dni.Loggable;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@RestController
@RequestMapping("/rest")
public class AmazonS3RestController {

    @Autowired
    AmazonS3 amazonS3;

    @Value("${aws.bucketName}")
    String bucketName;

    @Loggable
    private Logger logger;

    /**
     * List objects in default bucket
     *
     * @return
     */
    @RequestMapping(value = "/list", method = RequestMethod.GET, produces = "application/json")
    public ObjectListing list() {
        return amazonS3.listObjects(bucketName);
    }

    /**
     * View metadata of object in default bucket
     *
     * @param key filename
     * @return
     */
    @RequestMapping(value = "/metadata/{key}/", method = RequestMethod.GET, produces = "application/json")
    public S3Object metadata(@PathVariable("key") String key) {
        logger.info("key: {}", key);
        return amazonS3.getObject(bucketName, key);
    }

    /**
     * delete object in default bucket
     *
     * @param key filename
     */
    @RequestMapping(value = "/delete/{key}/", method = RequestMethod.DELETE, produces = "application/json")
    public void delete(@PathVariable("key") String key) {
        logger.info("key: {}", key);
        amazonS3.deleteObject(bucketName, key);
    }

    /**
     * Read object content as String in default bucket
     *
     * @param key
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/download/{key}/", method = RequestMethod.GET)
    public String download(@PathVariable("key") String key) throws IOException {
        logger.info("key: {}", key);
        S3Object object = amazonS3.getObject(bucketName, key);
        BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line);
        }
        return content.toString();
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
        S3Object object = amazonS3.getObject(bucketName, key);

        response.addHeader("Content-disposition", "attachment;filename=" + object.getKey());
        response.setContentType(object.getObjectMetadata().getContentType());

        InputStream stream = object.getObjectContent();
        IOUtils.copy(stream, response.getOutputStream());
        response.flushBuffer();
    }

    /**
     * Stream upload multipart files to default bucket
     * add User metadata
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
                    PutObjectRequest request = new PutObjectRequest(bucketName, file.getOriginalFilename(), file.getInputStream(), metadata);
                    amazonS3.putObject(request);
                } catch (AmazonServiceException e){
                    logger.error("",e);
                }
            }
        }
    }

}
