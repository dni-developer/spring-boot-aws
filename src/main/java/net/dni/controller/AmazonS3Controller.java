package net.dni.controller;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import net.dni.Loggable;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Controller
public class AmazonS3Controller {

    @Autowired
    AmazonS3 amazonS3;

    @Value("${aws.bucketName}")
    String bucketName;

    @Loggable
    private Logger logger;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String list(Model model) {
        ObjectListing objectListing = amazonS3.listObjects(bucketName);
        List<S3ObjectSummary> s3ObjectSummaryList = objectListing.getObjectSummaries();
        List<String> keyList = s3ObjectSummaryList.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        model.addAttribute("files", keyList);
        return "index";
    }

    @RequestMapping(method = RequestMethod.POST, value = "/")
    public String handleFileUpload(@RequestParam("file") MultipartFile file, RedirectAttributes redirectAttributes) {
        if (!file.isEmpty()) {
            try {
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(IOUtils.toByteArray(file.getInputStream()).length);

                PutObjectRequest request = new PutObjectRequest(bucketName, file.getName(), file.getInputStream(), metadata);
                amazonS3.putObject(request);
                redirectAttributes.addFlashAttribute("message", "You successfully uploaded " + file.getOriginalFilename() + "!");
            } catch (IOException | RuntimeException e) {
                redirectAttributes.addFlashAttribute("message", "Failued to upload " + file.getOriginalFilename() + " => " + e.getMessage());
            }
        } else {
            redirectAttributes.addFlashAttribute("message", "Failed to upload " + file.getOriginalFilename() + " because it was empty");
        }

        return "redirect:/";
    }


}
