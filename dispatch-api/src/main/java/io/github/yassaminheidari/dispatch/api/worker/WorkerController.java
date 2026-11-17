package io.github.yassaminheidari.dispatch.api.worker;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Lists live workers. Workers write a heartbeat hash to Redis with a short TTL; expired workers disappear. */
@RestController
public class WorkerController {

    public record WorkerView(String id, Map<String, String> state) {
    }

    private final StringRedisTemplate redis;

    public WorkerController(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @GetMapping("/api/workers")
    public List<WorkerView> workers() {
        Set<String> ids = redis.opsForSet().members("workers");
        List<WorkerView> result = new ArrayList<>();
        if (ids == null) {
            return result;
        }
        for (String id : new java.util.TreeSet<>(ids)) {
            Map<Object, Object> hash = redis.opsForHash().entries("worker:" + id);
            if (hash.isEmpty()) {
                redis.opsForSet().remove("workers", id);
                continue;
            }
            Map<String, String> state = new TreeMap<>();
            hash.forEach((k, v) -> state.put(String.valueOf(k), String.valueOf(v)));
            result.add(new WorkerView(id, state));
        }
        return result;
    }
}
