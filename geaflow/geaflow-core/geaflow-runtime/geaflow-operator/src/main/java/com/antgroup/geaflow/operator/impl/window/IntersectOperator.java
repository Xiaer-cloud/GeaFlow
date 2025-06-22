package com.antgroup.geaflow.operator.impl.window;

import com.antgroup.geaflow.api.function.Function;
import com.antgroup.geaflow.operator.base.window.AbstractTwoInputOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class IntersectOperator<T, U> extends AbstractTwoInputOperator<T, U,
        Function> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IntersectOperator.class);
    private final Set<T> firstInputSet = new HashSet<>();
    private final Set<U> intersectionSet = new HashSet<>();
    public IntersectOperator() {
        super();
    }
    @Override
    protected void processRecordOne(T value) throws Exception {
        LOGGER.info("processRecordOne: {}", value);
        if (!firstInputSet.contains(value)) {
            firstInputSet.add(value);
        }
    }
    @Override
    protected void processRecordTwo(U value) throws Exception {
        LOGGER.info("processRecordTwo: {}", value);

        if (firstInputSet.contains(value) && !intersectionSet.contains(value)) {
            intersectionSet.add(value);

            collectValue(value);
        }
    }
}
