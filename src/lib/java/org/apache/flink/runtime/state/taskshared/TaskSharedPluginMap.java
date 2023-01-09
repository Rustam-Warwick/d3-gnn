package org.apache.flink.runtime.state.taskshared;

import elements.Plugin;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Map state with TaskShared semantics
 * <p>
 *     Does not checkpoint anything as it the prerogative of the {@link Plugin}
 * </p>
 */
public class TaskSharedPluginMap extends TaskSharedState implements Map<String, Plugin> {

    protected Map<String, Plugin> wrappedMap = new NonBlockingHashMap<>();

    @Override
    public int size() {
        return wrappedMap.size();
    }

    @Override
    public boolean isEmpty() {
        return wrappedMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return wrappedMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return wrappedMap.containsValue(value);
    }

    @Override
    public Plugin get(Object key) {
        return wrappedMap.get(key);
    }

    @Nullable
    @Override
    public Plugin put(String key, Plugin value) {
        return wrappedMap.put(key, value);
    }

    @Override
    public Plugin remove(Object key) {
        return wrappedMap.remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends String, ? extends Plugin> m) {
        wrappedMap.putAll(m);
    }

    @Override
    public void clear() {
        wrappedMap.clear();
    }

    @NotNull
    @Override
    public Set<String> keySet() {
        return wrappedMap.keySet();
    }

    @NotNull
    @Override
    public Collection<Plugin> values() {
        return wrappedMap.values();
    }

    @NotNull
    @Override
    public Set<Entry<String, Plugin>> entrySet() {
        return wrappedMap.entrySet();
    }

    @Override
    public boolean equals(Object o) {
        return wrappedMap.equals(o);
    }

    @Override
    public int hashCode() {
        return wrappedMap.hashCode();
    }

    @Override
    public Plugin getOrDefault(Object key, Plugin defaultValue) {
        return wrappedMap.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Plugin> action) {
        wrappedMap.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Plugin, ? extends Plugin> function) {
        wrappedMap.replaceAll(function);
    }

    @Nullable
    @Override
    public Plugin putIfAbsent(String key, Plugin value) {
        return wrappedMap.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return wrappedMap.remove(key, value);
    }

    @Override
    public boolean replace(String key, Plugin oldValue, Plugin newValue) {
        return wrappedMap.replace(key, oldValue, newValue);
    }

    @Nullable
    @Override
    public Plugin replace(String key, Plugin value) {
        return wrappedMap.replace(key, value);
    }

    @Override
    public Plugin computeIfAbsent(String key, @NotNull Function<? super String, ? extends Plugin> mappingFunction) {
        return wrappedMap.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Plugin computeIfPresent(String key, @NotNull BiFunction<? super String, ? super Plugin, ? extends Plugin> remappingFunction) {
        return wrappedMap.computeIfPresent(key, remappingFunction);
    }

    @Override
    public Plugin compute(String key, @NotNull BiFunction<? super String, ? super Plugin, ? extends Plugin> remappingFunction) {
        return wrappedMap.compute(key, remappingFunction);
    }

    @Override
    public Plugin merge(String key, @NotNull Plugin value, @NotNull BiFunction<? super Plugin, ? super Plugin, ? extends Plugin> remappingFunction) {
        return wrappedMap.merge(key, value, remappingFunction);
    }

    public static <K, V> Map<K, V> of() {
        return Map.of();
    }

    public static <K, V> Map<K, V> of(K k1, V v1) {
        return Map.of(k1, v1);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2) {
        return Map.of(k1, v1, k2, v2);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3) {
        return Map.of(k1, v1, k2, v2, k3, v3);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
    }

    public static <K, V> Map<K, V> of(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4, K k5, V v5, K k6, V v6, K k7, V v7, K k8, V v8, K k9, V v9, K k10, V v10) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
    }

    @SafeVarargs
    public static <K, V> Map<K, V> ofEntries(Entry<? extends K, ? extends V>... entries) {
        return Map.ofEntries(entries);
    }

    public static <K, V> Entry<K, V> entry(K k, V v) {
        return Map.entry(k, v);
    }

    public static <K, V> Map<K, V> copyOf(Map<? extends K, ? extends V> map) {
        return Map.copyOf(map);
    }
}
