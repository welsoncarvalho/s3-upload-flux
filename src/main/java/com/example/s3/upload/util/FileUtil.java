package com.example.s3.upload.util;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@Component
public class FileUtil {

    private static final String FILE_BASE = "/home/welson/Development/img_test/";

    public Mono<String> save(InputStream is, String filename) {
        try {
            byte[] buffer = new byte[is.available()];
            is.read(buffer);

            Path path = Paths.get(FILE_BASE + filename);
            Files.write(path, buffer, StandardOpenOption.CREATE_NEW);

            return Mono.just(FILE_BASE + filename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
