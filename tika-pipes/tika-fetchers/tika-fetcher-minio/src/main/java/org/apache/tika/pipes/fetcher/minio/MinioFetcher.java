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
package org.apache.tika.pipes.fetcher.minio;

import static org.apache.tika.config.TikaConfig.mustNotBeEmpty;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.Field;
import org.apache.tika.config.Initializable;
import org.apache.tika.config.InitializableProblemHandler;
import org.apache.tika.config.Param;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.AbstractFetcher;
import org.apache.tika.pipes.fetcher.RangeFetcher;


public class MinioFetcher extends AbstractFetcher implements Initializable, RangeFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinioFetcher.class);
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
        mustNotBeEmpty("endpoint", this.getEndpoint());
        mustNotBeEmpty("accessKey", this.getAccessKey());
        mustNotBeEmpty("secretKey", this.getSecretKey());
    }

    @Override
    public InputStream fetch(String fetchKey, Metadata metadata) throws TikaException, IOException {
        return getStream(fetchKey,0,0,metadata);
    }

    @Override
    public InputStream fetch(String fetchKey, long startOffset, long endOffset, Metadata metadata)
            throws TikaException, IOException {
        return getStream(fetchKey,startOffset,endOffset,metadata);
    }

    private InputStream getStream(String fetchKey, long startOffset, long endOffset, Metadata metadata)
            throws TikaException, IOException {
        String[] infos = fetchKey.split(":");
        String bucket =  infos[0];
        String tenantId = infos[1];
        String appCode = infos[2];
        String objectInfo = infos[3];
        Long len = endOffset - startOffset;
        GetObjectArgs getObjectArgs;
        if (len > 0 ) {
            getObjectArgs = GetObjectArgs.builder().bucket(bucket)
                    .object(objectInfo).offset(startOffset).length(len).build();
        }
        else {
            getObjectArgs = GetObjectArgs.builder().bucket(bucket)
                    .object(objectInfo).build();
        }

        try {
            metadata.add("x-amz-meta-t", tenantId);
            metadata.add("x-amz-meta-a", appCode);
            InputStream stream = minioClient.getObject(getObjectArgs);
            long start = System.currentTimeMillis();
            TikaInputStream tis = TikaInputStream.get(stream);
            tis.getPath();
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.debug("took {} ms to copy to local tmp file", elapsed);
            return tis;
        } catch (Exception e) {
            LOGGER.error("fetch error", fetchKey,e);
            throw new IOException("minio Client exception", e);
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
    @Field
    public void setRegion(String region) {
        this.region = region;
    }
}
