package features;

import types.GraphElement;

import java.util.concurrent.CompletableFuture;

public class StaticFeature<T>  extends Feature<T>{
    public volatile T value;
    public transient CompletableFuture<T> fuzzyValue;
    public StaticFeature() {
        super();
        this.value = null;
    }

    public StaticFeature(String fieldName) {
        super(fieldName);
        this.value = null;
    }

    public StaticFeature(String fieldName, GraphElement element) {
        super(fieldName, element);
        this.value = null;
    }
    public StaticFeature(String fieldName, GraphElement element,T value) {
        super(fieldName, element);

        this.value = value;
        this.fuzzyValue = new CompletableFuture<>();
        this.fuzzyValue.complete(value);
    }

    @Override
    public CompletableFuture<T> getValue() {
        return this.fuzzyValue;
    }

    @Override
    public void setValue(T newValue) {

    }

    @Override
    public void updateMessage(Update<T> msg) {

    }
}
