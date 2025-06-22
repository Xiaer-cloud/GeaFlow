package com.antgroup.geaflow.dsl.runtime.plan;

import com.antgroup.geaflow.dsl.common.exception.GeaFlowDSLException;
import com.antgroup.geaflow.dsl.runtime.QueryContext;
import com.antgroup.geaflow.dsl.runtime.RDataView;
import com.antgroup.geaflow.dsl.runtime.RuntimeTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.ArrayList;
import java.util.List;

public class PhysicIntersectRelNode extends Intersect implements
        PhysicRelNode<RuntimeTable> {
    public PhysicIntersectRelNode(RelOptCluster cluster, RelTraitSet traits,
                                  List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new PhysicIntersectRelNode(super.getCluster(), traitSet, inputs,
                all);
    }

    @Override
    public RuntimeTable translate(QueryContext context) {
        List<RDataView> dataViews = new ArrayList<>();
        for (RelNode input : getInputs()) {
            dataViews.add(((PhysicRelNode<?>) input).translate(context));
            if (dataViews.get(dataViews.size() - 1).getType() !=
                    RDataView.ViewType.TABLE) {
                throw new GeaFlowDSLException("DataView: "
                        + dataViews.get(dataViews.size() - 1).getType() + " cannot support SQL intersect");
            }
        }
        if (!dataViews.isEmpty()) {
            RuntimeTable output = (RuntimeTable) dataViews.get(0);
            for (int i = 1; i < dataViews.size(); i++) {
                output = output.intersect((RuntimeTable) dataViews.get(i));
            }
            return output;
        } else {
            throw new GeaFlowDSLException("Intersect inputs cannot be empty.");
        }
    }

    @Override
    public String showSQL() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getInputs().size(); i++) {
            if (i > 0) {
                sb.append(" ").append(kind.name()).append(" ");
            }
            sb.append(((PhysicRelNode) getInputs().get(i)).showSQL());
        }
        return sb.toString();
    }
}
