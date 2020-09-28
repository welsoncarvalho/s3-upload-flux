package com.example.s3.upload.decorator;

import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePart;
import reactor.core.publisher.Mono;

import java.io.InputStream;

public class FilePartDecorator {

    private FilePart filePart;

    public FilePartDecorator(FilePart filePart) {
        this.filePart = filePart;
    }

    public Mono<InputStream> toInputStream() {
        return DataBufferUtils
                .join(filePart.content())
                .flatMap(is -> Mono.just(is.asInputStream()));
    }
}
