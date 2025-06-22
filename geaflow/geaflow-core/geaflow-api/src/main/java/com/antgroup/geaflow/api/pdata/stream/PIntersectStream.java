package com.antgroup.geaflow.api.pdata.stream;

import com.antgroup.geaflow.common.encoder.IEncoder;

import java.util.Map;

public interface PIntersectStream<T> extends PStream<T>  {
    @Override
    PIntersectStream<T> withConfig(Map map);

    @Override
    PIntersectStream<T> withConfig(String key, String value);

    @Override
    PIntersectStream<T> withName(String name);

    @Override
    PIntersectStream<T> withParallelism(int parallelism);

    PIntersectStream<T> withEncoder(IEncoder<T> encoder);
}
