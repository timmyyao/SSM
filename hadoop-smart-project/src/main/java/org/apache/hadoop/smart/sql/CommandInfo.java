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
package org.apache.hadoop.smart.sql;

import org.apache.hadoop.smart.CommandState;
import org.apache.hadoop.smart.actions.ActionType;


public class CommandInfo {
  private long cid;
  private long rid;
  // TODO Maybe need actionID
  //private long actionId;
  private ActionType actionType;
  private CommandState state;
  private String parameters;
  private long generateTime;
  private long stateChangedTime;

  public CommandInfo(long cid, long rid, ActionType actionType, CommandState state,
                     String parameters, long generateTime, long stateChangedTime) {
    this.cid = cid;
    this.rid = rid;
    this.actionType = actionType;
    this.state = state;
    this.parameters = parameters;
    this.generateTime = generateTime;
    this.stateChangedTime = stateChangedTime;
  }

  @Override
  public String toString() {
    return String.format("{cid = %d, rid = %d, genTime = %d, "
        + "stateChangedTime = %d, actionType = %s, state = %s, params = %s}",
        cid, rid, generateTime, stateChangedTime, actionType, state,
        parameters);
  }

  public long getCid() {
    return cid;
  }

  public void setCid(long cid) {
    this.cid = cid;
  }

  public long getRid() {
    return rid;
  }

  public void setRid(int rid) {
    this.rid = rid;
  }

  public ActionType getActionType() {
    return actionType;
  }

  public void setActionType(ActionType actionType) {
    this.actionType = actionType;
  }

  public CommandState getState() {
    return state;
  }

  public void setState(CommandState state) {
    this.state = state;
  }

  public String getParameters() {
    return parameters;
  }

  public void setParameters(String parameters) {
    this.parameters = parameters;
  }

  public long getGenerateTime() {
    return generateTime;
  }

  public void setGenerateTime(long generateTime) {
    this.generateTime = generateTime;
  }

  public long getStateChangedTime() {
    return stateChangedTime;
  }

  public void setStateChangedTime(long stateChangedTime) {
    this.stateChangedTime = stateChangedTime;
  }

}
