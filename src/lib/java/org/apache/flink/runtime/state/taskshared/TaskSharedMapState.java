package org.apache.flink.runtime.state.taskshared;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Map state with TaskShared semantics
 *
 * @param <T> key type
 * @param <V> value type
 */
public class TaskSharedMapState<T, V> extends TaskSharedState implements Map<T, V> {

    protected Map<T, V> wrappedMap = new NonBlockingHashMap<>();

    public static <K, V1> Map<K, V1> of() {
        return Map.of();
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1) {
        return Map.of(k1, v1);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2) {
        return Map.of(k1, v1, k2, v2);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3) {
        return Map.of(k1, v1, k2, v2, k3, v3);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5, K k6, V1 v6) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5, K k6, V1 v6, K k7, V1 v7) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5, K k6, V1 v6, K k7, V1 v7, K k8, V1 v8) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5, K k6, V1 v6, K k7, V1 v7, K k8, V1 v8, K k9, V1 v9) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9);
    }

    public static <K, V1> Map<K, V1> of(K k1, V1 v1, K k2, V1 v2, K k3, V1 v3, K k4, V1 v4, K k5, V1 v5, K k6, V1 v6, K k7, V1 v7, K k8, V1 v8, K k9, V1 v9, K k10, V1 v10) {
        return Map.of(k1, v1, k2, v2, k3, v3, k4, v4, k5, v5, k6, v6, k7, v7, k8, v8, k9, v9, k10, v10);
    }

    @SafeVarargs
    public static <K, V1> Map<K, V1> ofEntries(Entry<? extends K, ? extends V1>... entries) {
        return Map.ofEntries(entries);
    }

    public static <K, V1> Entry<K, V1> entry(K k, V1 v1) {
        return Map.entry(k, v1);
    }

    public static <K, V1> Map<K, V1> copyOf(Map<? extends K, ? extends V1> map) {
        return Map.copyOf(map);
    }

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
    public V get(Object key) {
        return wrappedMap.get(key);
    }

    @Nullable
    @Override
    public V put(T key, V value) {
        return wrappedMap.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return wrappedMap.remove(key);
    }

    @Override
    public void putAll(@NotNull Map<? extends T, ? extends V> m) {
        wrappedMap.putAll(m);
    }

    @Override
    public void clear() {
        wrappedMap.clear();
    }

    @NotNull
    @Override
    public Set<T> keySet() {
        return wrappedMap.keySet();
    }

    @NotNull
    @Override
    public Collection<V> values() {
        return wrappedMap.values();
    }

    @NotNull
    @Override
    public Set<Entry<T, V>> entrySet() {
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
    public V getOrDefault(Object key, V defaultValue) {
        return wrappedMap.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super T, ? super V> action) {
        wrappedMap.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super T, ? super V, ? extends V> function) {
        wrappedMap.replaceAll(function);
    }

    @Nullable
    @Override
    public V putIfAbsent(T key, V value) {
        return wrappedMap.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return wrappedMap.remove(key, value);
    }

    @Override
    public boolean replace(T key, V oldValue, V newValue) {
        return wrappedMap.replace(key, oldValue, newValue);
    }

    @Nullable
    @Override
    public V replace(T key, V value) {
        return wrappedMap.replace(key, value);
    }

    @Override
    public V computeIfAbsent(T key, @NotNull Function<? super T, ? extends V> mappingFunction) {
        return wrappedMap.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(T key, @NotNull BiFunction<? super T, ? super V, ? extends V> remappingFunction) {
        return wrappedMap.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(T key, @NotNull BiFunction<? super T, ? super V, ? extends V> remappingFunction) {
        return wrappedMap.compute(key, remappingFunction);
    }

    @Override
    public V merge(T key, @NotNull V value, @NotNull BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return wrappedMap.merge(key, value, remappingFunction);
    }
}
