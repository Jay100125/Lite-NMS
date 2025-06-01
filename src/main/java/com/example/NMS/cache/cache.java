package com.example.NMS.cache;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface cache
{
    void init();

    void insert(JsonObject jsonObject);

    void update(JsonObject jsonObject);

    void delete(Long id);
}
