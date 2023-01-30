/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hadoop.compression.lzo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

import java.io.IOException;
import java.io.OutputStream;

public class LzopCodec extends io.airlift.compress.lzo.LzopCodec {
    private static final Log LOG = LogFactory.getLog(LzopCodec.class);

    static final String gplLzopCodec = LzopCodec.class.getName();
    static final String airliftLzopCodec = io.airlift.compress.lzo.LzopCodec.class.getName();
    static boolean warned = false;

    static {
        LOG.info("Bridging " + gplLzopCodec + " to " + airliftLzopCodec + ".");
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out,
                                                      Compressor compressor) throws IOException {
        if (!warned) {
            LOG.warn(gplLzopCodec + " is deprecated. You should use " + airliftLzopCodec
                    + " instead to generate LZOP compressed data.");
            warned = true;
        }
        return super.createOutputStream(out, compressor);
    }
}
