package com.antgroup.geaflow.dsl;

import org.apache.calcite.plan.RelOptUtil;
import org.testng.annotations.Test;

public class GQLValidateIntersectTest {
    @Test
    public void testGqlIntersect() {
        //构建逻辑执行计划并验证GQL语法
        PlanTester planTester = PlanTester.build()
                .gql("SELECT\n" +
                        "   a.id,\n" +
                        "   b.id\n" +
                        "FROM (\n" +
                        "   MATCH (a:user where a.age > 18)-[e:knows]->(b:user)\n" +
                        "   RETURN a, b\n" +
                        "   INTERSECT\n" +
                        "   MATCH (a:user where a.age < 55)-[e:knows]->(b:user)\n" +
                        "   RETURN a, b\n" +
                        ")")
                .validate()
                .toRel();

        RelOptUtil.toString(planTester.getRelNode());
    }
}