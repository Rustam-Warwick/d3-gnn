package helpers;

import ai.djl.MalformedModelException;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.Parameter;
import ai.djl.nn.ParameterList;
import ai.djl.util.Pair;
import ai.djl.util.PairList;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SerializableParameters extends ParameterList implements Serializable {
    public transient ParameterList list;
    public SerializableParameters(){
        list = new ParameterList();
    }
    public SerializableParameters(ParameterList e){
        this.list = e;
    }
    public static SerializableParameters of(ParameterList e){
        return new SerializableParameters(e);
    }
    private void writeObject(ObjectOutputStream oos) throws IOException {
        // default serialization
        for(Pair<String, Parameter> pair: list){
            oos.writeBoolean(false);
            pair.getValue().save(new DataOutputStream(oos));
        }
        oos.writeBoolean(true);

    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        // default deserialization
        NDManager manager = NDManager.newBaseManager();
        while(true){
            try {
                if(!ois.readBoolean())break;
                Parameter tmp = new Parameter.Builder().build();
                tmp.load(manager, new DataInputStream(ois));
                this.list.add(tmp.getId(), tmp);
            } catch (MalformedModelException e) {
                e.printStackTrace();
            }
        }

    }
//    @Override
//    public void writeExternal(ObjectOutput out) throws IOException {
//        DataOutputStream a = new DataOutputStream(new OutputStream() {
//            @Override
//            public void write(int b) throws IOException {
//                out.write(b);
//            }
//        });
//        for(Pair<String, Parameter> pair: list){
//            a.writeBoolean(false);
//            pair.getValue().save(a);
//        }
//        a.writeBoolean(true);
//
//    }
//
//    @Override
//    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        NDManager manager = NDManager.newBaseManager();
//        DataInputStream a = new DataInputStream(new InputStream() {
//            @Override
//            public int read() throws IOException {
//                return in.read();
//            }
//        });
//
//        while(true){
//            try {
//                if(!a.readBoolean())break;
//                Parameter tmp = new Parameter.Builder().build();
//                tmp.load(manager, a);
//                this.list.add(tmp.getId(), tmp);
//            } catch (MalformedModelException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    @Override
    public void add(int index, String key, Parameter value) {
        list.add(index, key, value);
    }

    @Override
    public void add(String key, Parameter value) {
        list.add(key, value);
    }

    @Override
    public void add(Pair<String, Parameter> pair) {
        list.add(pair);
    }

    @Override
    public void addAll(PairList<String, Parameter> other) {
        list.addAll(other);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public Pair<String, Parameter> get(int index) {
        return list.get(index);
    }

    @Override
    public Parameter get(String key) {
        return list.get(key);
    }

    @Override
    public int indexOf(String key) {
        return list.indexOf(key);
    }

    @Override
    public String keyAt(int index) {
        return list.keyAt(index);
    }

    @Override
    public Parameter valueAt(int index) {
        return list.valueAt(index);
    }

    @Override
    public List<String> keys() {
        return list.keys();
    }

    @Override
    public List<Parameter> values() {
        return list.values();
    }

    @Override
    public String[] keyArray(String[] target) {
        return list.keyArray(target);
    }

    @Override
    public Parameter[] valueArray(Parameter[] target) {
        return list.valueArray(target);
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public Parameter remove(String key) {
        return list.remove(key);
    }

    @Override
    public Parameter remove(int index) {
        return list.remove(index);
    }

    @Override
    public PairList<String, Parameter> subList(int fromIndex) {
        return list.subList(fromIndex);
    }

    @Override
    public PairList<String, Parameter> subList(int fromIndex, int toIndex) {
        return list.subList(fromIndex, toIndex);
    }

    @Override
    public Stream<Pair<String, Parameter>> stream() {
        return list.stream();
    }

    @Override
    public boolean contains(String key) {
        return list.contains(key);
    }

    @Override
    public PairList<String, Parameter> unique() {
        return list.unique();
    }

    @Override
    public Iterator<Pair<String, Parameter>> iterator() {
        return list.iterator();
    }

    @Override
    public Map<String, Parameter> toMap() {
        return list.toMap();
    }

    @Override
    public Map<String, Parameter> toMap(boolean checkDuplicate) {
        return list.toMap(checkDuplicate);
    }

    @Override
    public void forEach(Consumer<? super Pair<String, Parameter>> action) {
        list.forEach(action);
    }

    @Override
    public Spliterator<Pair<String, Parameter>> spliterator() {
        return list.spliterator();
    }
}
