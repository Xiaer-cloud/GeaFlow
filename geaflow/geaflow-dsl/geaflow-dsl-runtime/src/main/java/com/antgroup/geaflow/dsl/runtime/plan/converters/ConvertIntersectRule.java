package com.antgroup.geaflow.dsl.runtime.plan.converters;

import com.antgroup.geaflow.dsl.runtime.plan.PhysicConvention;
import com.antgroup.geaflow.dsl.runtime.plan.PhysicIntersectRelNode;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalIntersect;

import java.util.List;

public class ConvertIntersectRule extends ConverterRule {
    public static final ConverterRule INSTANCE = new ConvertIntersectRule();

    public ConvertIntersectRule() {
        super(LogicalIntersect.class,
                Convention.NONE,
                PhysicConvention.INSTANCE,
                ConvertIntersectRule.class.getSimpleName());
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalIntersect intersect = (LogicalIntersect) rel;
        RelTraitSet relTraitSet =
                intersect.getTraitSet().replace(PhysicConvention.INSTANCE);
        List<RelNode> convertedInputs = Lists.newArrayList();
        for (RelNode input : intersect.getInputs()) {
            RelNode convertedInput = convert(input,
                    input.getTraitSet().replace(PhysicConvention.INSTANCE));
            convertedInputs.add(convertedInput);
        }
        return new PhysicIntersectRelNode(intersect.getCluster(), relTraitSet,
                convertedInputs, intersect.all);
    }
}
