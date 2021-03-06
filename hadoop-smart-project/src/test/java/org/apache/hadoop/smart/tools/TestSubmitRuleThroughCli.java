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
package org.apache.hadoop.smart.tools;

import org.apache.hadoop.smart.TestEmptyMiniSmartCluster;
import org.apache.hadoop.smart.protocol.SmartClient;
import org.apache.hadoop.smart.rule.RuleInfo;
import org.apache.hadoop.smart.sql.TestDBUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

public class TestSubmitRuleThroughCli extends TestEmptyMiniSmartCluster {

  @Test
  public void test() throws Exception {
    waitTillSSMExitSafeMode();

    String ruleFile = TestDBUtil.getUniqueFilePath();
    try {

      String rule = "file: every 1s \n | length > 10 | cachefile";
      FileOutputStream os = new FileOutputStream(ruleFile);
      os.write(rule.getBytes());
      os.close();

      SmartClient client = new SmartClient(conf);

      String[] args = new String[] {
          "submitrule",
          ruleFile
      };

      SmartShell.main(args);

      Thread.sleep(2000);

      List<RuleInfo> infos = client.listRulesInfo();
      Assert.assertTrue(infos.size() == 1);

      Thread.sleep(1500);

      List<RuleInfo> infos2 = client.listRulesInfo();
      long diff = infos2.get(0).getNumChecked() - infos.get(0).getNumChecked();
      Assert.assertTrue(diff >= 1);

    } finally {
      File f = new File(ruleFile);
      if (f.exists()) {
        f.deleteOnExit();
      }
    }
  }
}
