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
package org.apache.hadoop.smart.actions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.smart.mover.MoverPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;
import java.util.UUID;

/**
 * MoveFile Action
 */
public class MoveFile extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(MoveFile.class);

    public String storagePolicy;
    private String fileName;
    private Configuration conf;

    public MoveFile(DFSClient client, Configuration conf, String storagePolicy) {
        super(client);
        this.conf = conf;
        this.actionType = ActionType.MoveFile;
        this.storagePolicy = storagePolicy;
    }

    public ActionBase initial(String[] args) {
        this.fileName = args[0];
        return this;
    }

    /**
     * Execute an action.
     *
     * @return true if success, otherwise return false.
     */
    public UUID execute() {
        return runMove(fileName);
    }

    private UUID runMove(String fileName) {
        // TODO check if storagePolicy is the same
        LOG.info("Action starts at " + new Date(System.currentTimeMillis())
            + " : " + fileName + " -> " + storagePolicy);
        try {
            dfsClient.setStoragePolicy(fileName, storagePolicy);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return MoverPool.getInstance().createMoverAction(fileName);
    }


}