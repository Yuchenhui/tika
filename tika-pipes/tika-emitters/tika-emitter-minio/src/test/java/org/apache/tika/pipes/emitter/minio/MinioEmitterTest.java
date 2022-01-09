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

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.filter.FieldNameMappingFilter;

@Disabled
public class MinioEmitterTest {

    @Test
    public void oneOff() throws Exception {
        String endpoint = "http://127.0.0.1:6100";
        String emitKey = "marshall-test:marshall:test:chi_sim:QSCWKo7LpK4.docx";
        String region = "us-east-1";
        String accessKey = "admin";
        String secretKey = "admin123";
        MinioEmitter minioEmitter = new MinioEmitter();
        minioEmitter.setEndpoint(endpoint);
        minioEmitter.setAccessKey(accessKey);
        minioEmitter.setSecretKey(secretKey);
        minioEmitter.setRegion(region);
        minioEmitter.initialize(null);

        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.CREATED, new Date());
        metadata.set(TikaCoreProperties.TIKA_CONTENT, "（机场有群，购买后请入群，请谨慎购买，使用后有成瘾性）");

        HashMap mappings = new HashMap();
        FieldNameMappingFilter filter = new FieldNameMappingFilter();
        mappings.put(TikaCoreProperties.CREATED.getName(), "created");
        mappings.put(TikaCoreProperties.TIKA_CONTENT.getName(), "content");
        filter.setMappings(mappings);
        filter.filter(metadata);

        minioEmitter.emit(emitKey, Collections.singletonList(metadata));
    }
}
