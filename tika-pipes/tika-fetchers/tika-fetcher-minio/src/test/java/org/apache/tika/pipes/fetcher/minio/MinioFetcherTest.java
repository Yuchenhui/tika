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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;

@Disabled("write actual unit tests")
public class MinioFetcherTest {
    private final String endpoint = "http://127.0.0.1:9000";
    private final String emitKey = "marshall-test/marshall-test-5PG3WlIQgQY.pdf";
    private final String region = "us-east-1";
    private final String accessKey = "7YJXNRHTAX04PIEAV9NB";
    private final String secretKey = "+szUcO82UE4n0yzKAKMkuMxs207xKQADjpmDdEZS";
    private final Path outputFile = Paths.get("QY.pdf");
    @Test
    public void testBasic() throws Exception {
        MinioFetcher fetcher = new MinioFetcher();
        fetcher.setAccessKey(accessKey);
        fetcher.setSecretKey(secretKey);
        fetcher.setEndpoint(endpoint);
        fetcher.setRegion(region);
        fetcher.initialize(Collections.EMPTY_MAP);

        Metadata metadata = new Metadata();
        try (InputStream is = fetcher.fetch(emitKey, metadata)) {
            Files.copy(is, outputFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Test
    public void testConfig() throws Exception {
        FetcherManager fetcherManager = FetcherManager.load(
                Paths.get(this.getClass().getResource("/tika-config-minio.xml").toURI()));
        Fetcher fetcher = fetcherManager.getFetcher("minio");
        Metadata metadata = new Metadata();
        try (InputStream is = fetcher.fetch(emitKey, metadata)) {
            Files.copy(is, outputFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
