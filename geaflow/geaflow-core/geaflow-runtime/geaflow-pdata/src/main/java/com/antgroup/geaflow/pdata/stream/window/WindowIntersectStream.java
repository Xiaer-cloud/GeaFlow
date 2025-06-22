package com.antgroup.geaflow.pdata.stream.window;

import com.antgroup.geaflow.api.pdata.stream.PIntersectStream;
import com.antgroup.geaflow.common.encoder.IEncoder;
import com.antgroup.geaflow.operator.impl.window.IntersectOperator;
import com.antgroup.geaflow.pdata.stream.TransformType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WindowIntersectStream<T> extends WindowDataStream<T> implements
        PIntersectStream<T> {
    private List<WindowDataStream<T>> intersectWindowDataStreamList;

    public WindowIntersectStream(WindowDataStream<T> stream, WindowDataStream<T>
            intersectStream, IntersectOperator intersectOperator) {
        super(stream, intersectOperator);
        this.intersectWindowDataStreamList = new ArrayList<>();
        this.addIntersectDataStream(intersectStream);
    }

    public void addIntersectDataStream(WindowDataStream<T> intersectStream) {
        this.intersectWindowDataStreamList.add(intersectStream);
    }

    public List<WindowDataStream<T>> getIntersectWindowDataStreamList() {
        return intersectWindowDataStreamList;
    }

    @Override
    public WindowIntersectStream<T> withConfig(Map map) {
        setConfig(map);
        return this;
    }

    @Override
    public WindowIntersectStream<T> withConfig(String key, String value) {
        setConfig(key, value);
        return this;
    }

    @Override
    public WindowIntersectStream<T> withName(String name) {
        this.opArgs.setOpName(name);
        return this;
    }

    @Override
    public WindowIntersectStream<T> withParallelism(int parallelism) {
        setParallelism(parallelism);
        return this;
    }

    @Override
    public TransformType getTransformType() {
        return TransformType.StreamIntersect;
    }

    @Override
    public WindowIntersectStream<T> withEncoder(IEncoder<T> encoder) {
        this.encoder = encoder;
        return this;
    }
}
