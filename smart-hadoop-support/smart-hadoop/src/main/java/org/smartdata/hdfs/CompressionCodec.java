/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.hdfs;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.smartdata.action.SmartAction;
import org.smartdata.conf.SmartConf;
import org.smartdata.conf.SmartConfKeys;

import java.util.ArrayList;
import java.util.List;
/**
 * This class decide which compressor type for SmartCompressorStream 
 */
public class CompressionCodec  {
  static final Logger LOG = LoggerFactory.getLogger(SmartAction.class);
  
  private static String[] compressionImplArray = new String[0];
  SmartConf conf = new SmartConf();
  
  /**
   *  Create a compressor
   */
  public Compressor createCompressor(int bufferSize, String compressionImpl){
//    conf.set(SmartConfKeys.SMART_COMPRESSION_IMPL,"Lz4,Zlib" );
    conf.set(SmartConfKeys.SMART_COMPRESSION_IMPL,"Lz4,Bzip2,Zlib" );
    compressionImplArray = conf.getStrings(SmartConfKeys.SMART_COMPRESSION_IMPL);
    Boolean b = typeConfigured(compressionImplArray,compressionImpl);
    if(typeConfigured(compressionImplArray,compressionImpl)){
      switch (compressionImpl){
        case "Lz4" :
          return  new Lz4Compressor(bufferSize);
        case "Bzip2" :
          return  new Bzip2Compressor(9,
                                      30,
                                      bufferSize);
//          return  new Bzip2Compressor();
        case "Zlib" :
          return new ZlibCompressor(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
                                    ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
                                    ZlibCompressor.CompressionHeader.DEFAULT_HEADER,
                                    bufferSize);
        default:
          return new SnappyCompressor(bufferSize);
      }      
    }else{
      LOG.warn("This compressionImpl: " + compressionImpl + " is not defined in configuration,use the default Snappycompressor");
      return new SnappyCompressor(bufferSize); 
    }
  }
  
  /**
   *  Judge the compressionImpl is configured or not
   */
  public static boolean typeConfigured(String[] compressionImplArray, String compressionImpl) {
    return ArrayUtils.contains(compressionImplArray,compressionImpl);
  }
}
