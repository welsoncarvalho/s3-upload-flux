package com.example.s3.upload.controller;

import com.example.s3.upload.decorator.FilePartDecorator;
import com.example.s3.upload.model.Image;
import com.example.s3.upload.util.FileUtil;
import com.example.s3.upload.util.S3Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class UploadController {

    @Autowired
    FileUtil fileUtil;

    @Autowired
    S3Util s3Util;

    @PostMapping("/upload")
    public Mono<Image> upload(@RequestPart("file") final FilePart filePart) {
        return s3Util.upload(filePart)
                .flatMap(fileKey -> {
                    Image image = new Image();

                    image.setName(filePart.filename());
                    image.setPath(fileKey);

                    return Mono.just(image);
                });
//        return Mono.just(filePart)
//                .map(f -> new FilePartDecorator(f))
//                .flatMap(decorator -> decorator.toInputStream())
//                .flatMap(is -> fileUtil.save(is, filePart.filename()))
//                .flatMap(path -> {
//                    Image image = new Image();
//
//                    image.setName(filePart.filename());
//                    image.setPath(path);
//
//                    return Mono.just(image);
//                });
    }

}
