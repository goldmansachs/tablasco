/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gs.tablasco;

import com.gs.tablasco.investigation.Investigation;
import com.gs.tablasco.investigation.InvestigationLevel;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class InvestigationExampleTest
{
    @Rule
    public final TableVerifier tableVerifier = new TableVerifier()
            .withFilePerMethod()
            .withMavenDirectoryStrategy();

    @Test(expected = AssertionError.class)
    public void example()
    {
        Investigation investigation = new Investigation()
        {
            @Override
            public InvestigationLevel getFirstLevel()
            {
                return new InvestigationTest.SimpleInvestigationLevel(
                        "Group By Entity",
                        TableTestUtils.createTable(3,
                                "Entity", "Value", "Key",
                                "GSIB", 5,"GSIB",
                                "GSJC", 4,"GSJC",
                                "GSCO", 20,"GSCO",
                                "GSIL", 15,"GSIL",
                                "JANY", 12,"JANY"
                        ),
                        TableTestUtils.createTable(3,
                                "Entity", "Value", "Key",
                                "GSIB", 5,"GSIB",
                                "GSJC", 4,"GSJC",
                                "GSCO", 22,"GSCO",
                                "GSIL", 15,"GSIL",
                                "JANY", 10,"JANY"
                        ));
            }

            @Override
            public InvestigationLevel getNextLevel(List<Object> drilldownKeys)
            {
                if (drilldownKeys.contains("GSCO"))
                {
                    return new InvestigationTest.SimpleInvestigationLevel(
                            "Drilldown by Entity, Account",
                            TableTestUtils.createTable(4,
                                    "Entity", "Account", "Value", "Key",
                                    "GSCO", "7002", 20, "GSCO#7002",
                                    "JANY", "7003", 10, "JANY#7003",
                                    "JANY", "7004", 2, "JANY#7004"),
                            TableTestUtils.createTable(4,
                                    "Entity", "Account", "Value", "Key",
                                    "GSCO", "7001", 2, "GSCO#7001",
                                    "GSCO", "7002", 20, "GSCO#7002",
                                    "JANY", "7003", 8, "JANY#7003",
                                    "JANY", "7004", 2, "JANY#7004"));
                }
                if (drilldownKeys.contains("GSCO#7001"))
                {
                    return new InvestigationTest.SimpleInvestigationLevel(
                            "Drilldown by Entity, Account, Tran Ref",
                            TableTestUtils.createTable(5,
                                    "Entity", "Account", "Tran Ref", "Value", "Key",
                                    "JANY", "7003", "T2", 6, "GSCO#7001#T2",
                                    "JANY", "7003", "T3", 4, "GSCO#7001#T3"
                            ),
                            TableTestUtils.createTable(5,
                                    "Entity", "Account", "Tran Ref", "Value", "Key",
                                    "GSCO", "7001", "T1", 2, "GSCO#7001#T1",
                                    "JANY", "7003", "T2", 4, "GSCO#7001#T2",
                                    "JANY", "7003", "T3", 4, "GSCO#7001#T3"
                            ));
                }
                return null;
            }

            @Override
            public int getRowKeyLimit()
            {
                return 100;
            }
        };
        this.tableVerifier.investigate(investigation);
    }
}