package com.antgroup.geaflow.dsl.runtime.query;

import org.testng.annotations.Test;

public class IntersectTest {
    @Test
    public void testIntersect_001() throws Exception {
        QueryTester.build()
                .withQueryPath("/query/gql_intersect_001.sql")
                .execute()
                .checkSinkResult();
    }
}
