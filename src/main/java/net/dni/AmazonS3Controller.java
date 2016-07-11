package net.dni;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;

@RestController
@RequestMapping("/s3")
public class AmazonS3Controller {

    @Autowired
    AmazonS3 amazonS3;

    @Value("${aws.bucketName}")
    String bucketName;

    @Loggable
    private Logger logger;

    @RequestMapping(value = "/list", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    public ObjectListing list() {
        return amazonS3.listObjects(bucketName);
    }

    @RequestMapping(value = "/view/{key}/", method = RequestMethod.GET, produces = "application/json")
    public S3Object view(@PathVariable("key") String key) {
        logger.info("key: {}", key);
        return amazonS3.getObject(bucketName, key);
    }

    @RequestMapping(value = "/download/{key}/", method = RequestMethod.GET)
    @ResponseBody
    public void download(@PathVariable("key") String key, HttpServletResponse response) throws IOException {
        logger.info("key: {}", key);
        S3Object object = amazonS3.getObject(bucketName, key);

        response.addHeader("Content-disposition", "attachment;filename=" + object.getKey());
        response.setContentType(object.getObjectMetadata().getContentType());

        InputStream stream = object.getObjectContent();
        IOUtils.copy(stream, response.getOutputStream());
        response.flushBuffer();
    }

}
