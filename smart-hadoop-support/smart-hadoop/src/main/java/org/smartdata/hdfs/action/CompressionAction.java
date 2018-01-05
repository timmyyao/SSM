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
package org.smartdata.hdfs.action;

import com.google.gson.Gson;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.smartdata.action.ActionException;
import org.smartdata.action.Utils;
import org.smartdata.action.annotation.ActionSignature;
import org.smartdata.hdfs.SmartCompressorStream;
import org.smartdata.model.SmartFileCompressionInfo;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * This action convert a file to a compressed file.
 */
@ActionSignature(
    actionId = "compress",
    displayName = "compress",
    usage =
        HdfsAction.FILE_PATH
            + " $file "
            + CompressionAction.BUF_SIZE
            + " $size "
            + CompressionAction.COMPRESS_IMPL
            + " $impl "
)
public class CompressionAction extends HdfsAction {

  public static final String BUF_SIZE = "-bufSize";
  public static final String COMPRESS_IMPL = "-compressionImpl";
  private static List<String> compressionImplList = Arrays.asList(new String[]{
    "Lz4","Bzip2","Zlib","snappy"});

  private String filePath;
  private int bufferSize = 10 * 1024 * 1024;
  private int UserDefinedbuffersize;
  private int Calculatedbuffersize;
  private String compressionImpl = "snappy";

  private SmartFileCompressionInfo compressionInfo;

  @Override
  public void init(Map<String, String> args) {
    super.init(args);
    this.filePath = args.get(FILE_PATH);
    if (args.containsKey(BUF_SIZE)) {
      this.UserDefinedbuffersize = Integer.valueOf(args.get(BUF_SIZE));
    }
    if (args.containsKey(COMPRESS_IMPL)){
      this.compressionImpl = args.get(COMPRESS_IMPL);
    }
  }

  @Override
  protected void execute() throws Exception {
    if (filePath == null) {
      throw new IllegalArgumentException("File parameter is missing.");
    }
    appendLog(
        String.format("Action starts at %s : Read %s", Utils.getFormatedCurrentTime(), filePath));
    if (!dfsClient.exists(filePath)) {
      throw new ActionException("ReadFile Action fails, file doesn't exist!");
    }
    if(!compressionImplList.contains(compressionImpl)){
      throw new ActionException("Action fails, this compressionImpl isn't supported!");
    }

    // Generate compressed file
    String compressedFileName = "/tmp/ssm" + filePath + "." + System.currentTimeMillis() + ".ssm_compress";
    HdfsFileStatus srcFile = dfsClient.getFileInfo(filePath);
    short replication = srcFile.getReplication();
    long blockSize = srcFile.getBlockSize();
    long fileSize = srcFile.getLen();
    //The capacity of originalPos and compressedPos is 5000 in database 
    this.Calculatedbuffersize = (int)fileSize/5000;
    
    //Determine the actual buffersize
    if(UserDefinedbuffersize < bufferSize || UserDefinedbuffersize < Calculatedbuffersize){
      if(bufferSize <= Calculatedbuffersize){
        appendLog("User defined buffersize is too small,use the calculated buffersize:" + Calculatedbuffersize );
      }else{
        appendLog("User defined buffersize is too small,use the default buffersize:" + bufferSize );
      }
    }
    bufferSize = Math.max(Math.max(UserDefinedbuffersize,Calculatedbuffersize),bufferSize);
    
    DFSInputStream dfsInputStream = dfsClient.open(filePath);
    compressionInfo = new SmartFileCompressionInfo(filePath, bufferSize, compressionImpl);
    compressionInfo.setcompressionImpl(compressionImpl);
    compressionInfo.setOriginalLength(srcFile.getLen());
    OutputStream compressedOutputStream = dfsClient.create(compressedFileName,
      true, replication, blockSize);
    compress(dfsInputStream, compressedOutputStream);
    HdfsFileStatus destFile = dfsClient.getFileInfo(compressedFileName);
    compressionInfo.setCompressedLength(destFile.getLen());
    String compressionInfoJson = new Gson().toJson(compressionInfo);
    appendResult(compressionInfoJson);

    // Replace the original file with the compressed file
    dfsClient.delete(filePath);
    dfsClient.rename(compressedFileName, filePath);
  }

  private void compress(InputStream inputStream, OutputStream outputStream) throws Exception {
    SmartCompressorStream smartCompressorStream = new SmartCompressorStream(
        inputStream, outputStream, bufferSize, compressionInfo);
    smartCompressorStream.convert();
  }
}
