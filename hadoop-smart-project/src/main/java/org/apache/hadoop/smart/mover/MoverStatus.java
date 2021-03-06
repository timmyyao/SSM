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
package org.apache.hadoop.smart.mover;

import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Status of Mover tool.
 */
public class MoverStatus extends Status {
  static final Logger LOG = LoggerFactory.getLogger(MoverStatus.class);

  private UUID id;
  private Boolean isFinished;
  private long startTime;
  private Boolean succeeded;
  private long totalDuration;
  private long totalBlocks;
  private long totalSize;
  private long movedBlocks;

  private void init() {
    isFinished = false;
    startTime = Time.monotonicNow();
    succeeded = false;
    totalDuration = 0;
    totalBlocks = 0;
    totalSize = 0;
    movedBlocks = 0;
  }

  public MoverStatus(UUID id ) {
    this.id = id;
    init();
  }

  @Override
  synchronized public UUID getId() {
    return id;
  }

  /**
   * Denote whether the Mover process is finished.
   * @return true if the Mover process is finished
   */
  @Override
  synchronized public Boolean getIsFinished() {
    return isFinished;
  }

  /**
   * Set when the Mover process is finished.
   */
  @Override
  synchronized public void setIsFinished() {
    this.isFinished = true;
    LOG.info("Mover {} : Finished", id);
  }

  /**
   * Get the start time for the Mover process.
   * @return the start time
   */
  @Override
  synchronized public long getStartTime() { return startTime; }

  /**
   * Set the start time for the Mover process.
   * @param startTime
   */
  @Override
  synchronized public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get whether the Mover process is done successfully.
   * @return true if successful
   */
  @Override
  synchronized public Boolean getSucceeded() { return succeeded; }

  /**
   * Set when the Mover process succeeds.
   */
  @Override
  synchronized public void setSucceeded() {
    this.succeeded = true;
    LOG.info("Mover {} : Succeeded", id);
  }

  /**
   * Set the total execution time for the Mover process.
   * @param totalDuration
   */
  @Override
  synchronized public void setTotalDuration(long totalDuration) {
    this.totalDuration = totalDuration;
  }

  /**
   * Get the running time for the Mover process.
   * @return the current running time if the Mover process has not been finished
   * or the total execution time if it is finished
   */
  @Override
  synchronized public long getRunningTime() {
    if (totalDuration != 0) {
      return totalDuration;
    }
    return Time.monotonicNow() - startTime;
  }

  /**
   * Reset status to initial value.
   */
  @Override
  synchronized public void reset() {
    init();
  }

  synchronized public long getTotalBlocks() {
    return totalBlocks;
  }

  synchronized public void setTotalBlocks(long blocks) {
    totalBlocks = blocks;
  }

  synchronized public long increaseTotalBlocks(long blocks) {
    totalBlocks += blocks;
    return totalBlocks;
  }

  @Override
  synchronized public long getTotalSize() {
    return totalSize;
  }

  synchronized public void setTotalSize(long size) {
    totalSize = size;
  }

  synchronized public long increaseTotalSize(long size) {
    totalSize += size;
    return totalSize;
  }

  synchronized public long increaseMovedBlocks(long blocks) {
    movedBlocks += blocks;
    return movedBlocks;
  }

  synchronized public void setMovedBlocks(long blocks) {
    movedBlocks = blocks;
  }

  synchronized public long getMovedBlocks() {
    return movedBlocks;
  }

  @Override
  synchronized public float getPercentage() {
    if (totalBlocks == 0) {
      return 0;
    }
    if (isFinished) {
      return 1;
    }
    return movedBlocks >= totalBlocks ? 0.99f :
        0.99f * movedBlocks / totalBlocks;
  }
}
