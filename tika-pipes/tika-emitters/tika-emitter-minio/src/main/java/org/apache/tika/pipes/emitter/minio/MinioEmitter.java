/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.pipes.emitter.minio;

import static org.apache.tika.config.TikaConfig.mustNotBeEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.Field;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.emitter.AbstractEmitter;
import org.apache.tika.pipes.emitter.StreamEmitter;
import org.apache.tika.pipes.emitter.TikaEmitterException;

public class MinioEmitter extends AbstractEmitter implements Initializable, StreamEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinioEmitter.class);
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String region;
    private MinioClient minioClient;

    @Override
    public void initialize(Map<String, Param> params) throws TikaConfigException {
        try {
            minioClient = MinioClient.builder()
                    .endpoint(getEndpoint())
                    .credentials(getAccessKey(), getSecretKey())
                    .region(getRegion())
                    .build();
        }
        catch (Exception e) {
            throw new TikaConfigException("can't initialize minio emitter", e);
        }
    }

    @Override
    public void checkInitialization(InitializableProblemHandler problemHandler) throws TikaConfigException {
        mustNotBeEmpty("endpoint", this.endpoint);
        mustNotBeEmpty("accessKey", this.accessKey);
        mustNotBeEmpty("secretKey", this.secretKey);
    }


    @Override
    public void emit(String emitKey, List<Metadata> metadataList) throws IOException, TikaEmitterException {
        if (metadataList == null || metadataList.size() == 0) {
            throw new TikaEmitterException("metadata list must not be null or of size 0");
        }
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path tmpPath = tmp.createTempFile();
            try (Writer writer = Files.newBufferedWriter(tmpPath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE)) {
                JsonMetadataList.toJson(metadataList, writer);
            } catch (IOException e) {
                throw new TikaEmitterException("can't jsonify", e);
            }
            try (InputStream is = TikaInputStream.get(tmpPath)) {
                emit(emitKey, is, new Metadata());
            }
        }
    }

    @Override
    public void emit(String emitKey, InputStream inputStream, Metadata userMetadata)
            throws IOException, TikaEmitterException {
        String[] infos = emitKey.split("/");
        String bucket =  infos[0];
        String[] objectInfos = infos[1].split("-");
        String tenantId = objectInfos[0];
        String appCode = objectInfos[1];
        String fileId = objectInfos[2];
        String[] objectKey = fileId.split("\\.");
        String newObjectKey = objectKey[0] + "-tika.json";
        try {
            BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder().bucket(bucket).region(region).build();
            boolean found = minioClient.bucketExists(bucketExistsArgs);
            if (!found) {
                MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder().bucket(bucket).region(region).build();
                minioClient.makeBucket(makeBucketArgs);
            } else {
                LOGGER.info("Bucket '" + bucket + "' already exists.");
            }

            if (inputStream instanceof TikaInputStream) {
                if (((TikaInputStream) inputStream).hasFile()) {
                    try {
                        Map<String, String> um = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                        um.put("x-amz-meta-t",tenantId);
                        um.put("x-amz-meta-a",appCode);
                        um.put("x-amz-meta-h","tikacontentextract");
                        PutObjectArgs putObjectArgs = PutObjectArgs.builder().bucket(bucket)
                                .region(region).stream(inputStream,inputStream.available(),-1)
                                .userMetadata(um).object(newObjectKey).build();
                        minioClient.putObject(putObjectArgs);
                    } catch (IOException e) {
                        throw new TikaEmitterException("exception sending underlying file", e);
                    }
                }
            }
        }
        catch (Exception e) {
            LOGGER.error("Error",e);
        }
    }

    public String getEndpoint() {
        return endpoint;
    }
    @Field
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }
    @Field
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }
    @Field
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }
}
