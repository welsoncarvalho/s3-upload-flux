package com.example.s3.upload.util;

import com.example.s3.upload.config.S3ConfigProperties;
import com.example.s3.upload.exception.UploadException;
import com.example.s3.upload.model.UploadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class S3Util {

    private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

    private S3AsyncClient s3AsyncClient;
    private S3ConfigProperties s3ConfigProperties;

    public S3Util(S3AsyncClient s3AsyncClient, S3ConfigProperties s3ConfigProperties) {
        this.s3AsyncClient = s3AsyncClient;
        this.s3ConfigProperties = s3ConfigProperties;
    }

    public Mono<String> upload(FilePart filePart) {

        String filename = filePart.filename();
        String fileKey = "app-client/" + filePart.filename();

        Map<String, String> metadata = new HashMap<>();
        metadata.put("filename", filename);

        MediaType mediaType = filePart.headers().getContentType();
        if (mediaType == null) {
            mediaType = MediaType.APPLICATION_OCTET_STREAM;
        }

        CompletableFuture<CreateMultipartUploadResponse> uploadRequest = s3AsyncClient
                .createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .contentType(mediaType.toString())
                        .key(fileKey)
                        .metadata(metadata)
                        .bucket(s3ConfigProperties.getBucket())
                        .build());

        UploadStatus uploadStatus = new UploadStatus(s3ConfigProperties.getBucket(), fileKey);

        return Mono.fromFuture(uploadRequest)
                .flatMapMany(response -> {
                    checkResult(response);
                    uploadStatus.setUploadId(response.uploadId());
                    LOG.info("[upload] uploadId={}", response.uploadId());
                    return filePart.content();
                })
                .bufferUntil(dataBuffer -> {
                    uploadStatus.addBuffered(dataBuffer.readableByteCount());

                    if (uploadStatus.getBuffered() >= s3ConfigProperties.getMultipartMinPartSize()) {
                        LOG.info("[I173] bufferUntil: returning true, bufferedBytes={}, partCounter={}, uploadId={}",
                                uploadStatus.getBuffered(), uploadStatus.getPartCounter(), uploadStatus.getUploadId());

                        uploadStatus.setBuffered(0);
                        return true;
                    }

                    return false;
                })
                .map(this::concatBuffers)
                .flatMap(byteBuffer -> this.uploadPart(uploadStatus, byteBuffer))
                .onBackpressureBuffer()
                .reduce(uploadStatus, (status, completedPart) -> {
                    LOG.info("[I188] completed: partNumber={}, etag={}", completedPart.partNumber(), completedPart.eTag());
                    ((UploadStatus) status).getCompletedParts().put(completedPart.partNumber(), completedPart);
                    return status;
                })
                .flatMap(uploadStatus1 -> completeUpload(uploadStatus))
                .map(response -> {
                    checkResult(response);
                    return uploadStatus.getFileKey();
                });
    }

    private Mono<CompleteMultipartUploadResponse> completeUpload(UploadStatus uploadStatus) {
        LOG.info("[I202] completeUpload: bucket={}, filekey={}, completedParts.size={}",
                uploadStatus.getBucket(), uploadStatus.getFileKey(), uploadStatus.getCompletedParts().size());

        CompletedMultipartUpload multipartUpload = CompletedMultipartUpload.builder()
                .parts(uploadStatus.getCompletedParts().values())
                .build();

        return Mono.fromFuture(s3AsyncClient.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(uploadStatus.getBucket())
                .uploadId(uploadStatus.getUploadId())
                .multipartUpload(multipartUpload)
                .key(uploadStatus.getFileKey())
                .build()));
    }

    private Mono<CompletedPart> uploadPart(UploadStatus uploadStatus, ByteBuffer buffer) {
        final int partNumber = uploadStatus.getAddedPartCounter();
        LOG.info("[I218] uploadPart: partNumber={}, contentLength={}",partNumber, buffer.capacity());

        CompletableFuture<UploadPartResponse> request = s3AsyncClient.uploadPart(UploadPartRequest.builder()
                        .bucket(uploadStatus.getBucket())
                        .key(uploadStatus.getFileKey())
                        .partNumber(partNumber)
                        .uploadId(uploadStatus.getUploadId())
                        .contentLength((long) buffer.capacity())
                        .build(),
                        AsyncRequestBody.fromPublisher(Mono.just(buffer)));

        return Mono
                .fromFuture(request)
                .map((uploadPartResult) -> {
                    checkResult(uploadPartResult);
                    LOG.info("[I230] uploadPart complete: part={}, etag={}", partNumber, uploadPartResult.eTag());
                    return CompletedPart.builder()
                            .eTag(uploadPartResult.eTag())
                            .partNumber(partNumber)
                            .build();
                });
    }

    private ByteBuffer concatBuffers(List<DataBuffer> buffers) {
        LOG.info("[I198] creating BytBuffer from {} chunks", buffers.size());

        int partSize = 0;
        for( DataBuffer b : buffers) {
            partSize += b.readableByteCount();
        }

        ByteBuffer partData = ByteBuffer.allocate(partSize);
        buffers.forEach((buffer) -> {
            partData.put(buffer.asByteBuffer());
        });

        // Reset read pointer to first byte
        partData.rewind();

        LOG.info("[I208] partData: size={}", partData.capacity());
        return partData;

    }

    private void checkResult(SdkResponse sdkResponse) {
        if (sdkResponse.sdkHttpResponse() == null || !sdkResponse.sdkHttpResponse().isSuccessful()) {
            throw new UploadException(sdkResponse.sdkHttpResponse().statusCode() + " - " +
                    sdkResponse.sdkHttpResponse().statusText());
        }
    }
}
