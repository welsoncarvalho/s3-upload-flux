package com.example.s3.upload.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties(S3ConfigProperties.class)
public class S3Config {

    @Bean
    public S3AsyncClient s3AsyncClient(S3ConfigProperties s3ConfigProperties, AwsCredentialsProvider awsCredentialsProvider) {

        return S3AsyncClient.builder()
                .httpClient(sdkAsyncHttpClient())
                .region(s3ConfigProperties.getRegion())
                .credentialsProvider(awsCredentialsProvider)
                .serviceConfiguration(s3Configuration()).build();
    }

    private SdkAsyncHttpClient sdkAsyncHttpClient() {
        return NettyNioAsyncHttpClient.builder()
                .writeTimeout(Duration.ZERO)
                .maxConcurrency(64)
                .build();
    }

    private S3Configuration s3Configuration() {
        return S3Configuration.builder()
                .checksumValidationEnabled(false)
                .chunkedEncodingEnabled(true)
                .build();
    }

    @Bean
    public AwsCredentialsProvider awsCredentialsProvider(S3ConfigProperties s3ConfigProperties) {
        return () -> AwsBasicCredentials.create(
                s3ConfigProperties.getAccessKeyId(),
                s3ConfigProperties.getSecretAccessKey());
    }
}
