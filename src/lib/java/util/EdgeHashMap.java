package util;

import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.AbstractLong2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

public class EdgeHashMap extends AbstractLong2ObjectMap<String> {
    private static final boolean ASSERTS = false;
    /** The array of keys. */
    protected transient long[] key;
    /** The array of values. */
    protected transient String[] value;
    /** The mask for wrapping a position counter. */
    protected transient int mask;
    /** Whether this map contains the key zero. */
    protected transient boolean containsNullKey;
    /** The current table size. */
    protected transient int n;
    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    protected transient int maxFill;
    /** We never resize below this threshold, which is the construction-time {#n}. */
    protected final transient int minN;
    /** Number of entries in the set (including the key zero, if present). */
    protected int size;
    /** The acceptable load factor. */
    protected final float f;
    /** Cached set of entries. */
    protected transient FastEntrySet<String> entries;
    /** Cached set of keys. */
    protected transient LongSet keys;
    /** Cached collection of values. */
    protected transient ObjectCollection<String> values;

    public EdgeHashMap(final int expected, final float f) {
        if (f <= 0 || f >= 1) throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than 1");
        if (expected < 0) throw new IllegalArgumentException("The expected number of elements must be nonnegative");
        this.f = f;
        minN = n = arraySize(expected, f);
        mask = n - 1;
        maxFill = maxFill(n, f);
        key = new long[n + 1];
        value = (String[])new Object[n + 1];
    }
    
    private int realSize() {
        return containsNullKey ? size - 1 : size;
    }

    /**
     * Ensures that this map can hold a certain number of keys without rehashing.
     *
     * @param capacity a number of keys; there will be no rehashing unless the map {@linkplain #size()
     *            size} exceeds this number.
     */
    public void ensureCapacity(final int capacity) {
        final int needed = arraySize(capacity, f);
        if (needed > n) rehash(needed);
    }

    private void tryCapacity(final long capacity) {
        final int needed = (int)Math.min(1 << 30, Math.max(2, HashCommon.nextPowerOfTwo((long)Math.ceil(capacity / f))));
        if (needed > n) rehash(needed);
    }
    
    @Override
    public int size() {
        return 0;
    }

    @Override
    public ObjectSet<Entry<String>> long2ObjectEntrySet() {
        return null;
    }

    @Override
    public String get(long key) {
        return null;
    }

    public List<String> put(int srcId, int destId, @Nullable String attribute) {
        final int pos = find(k);
        if (pos < 0) {
            insert(-pos - 1, k, v);
            return defRetValue;
        }
        final V oldValue = value[pos];
        value[pos] = v;
        return oldValue;
    }
    
    protected void rehash(final int newN) {
        final long[] key = this.key;
        final String[] value = this.value;
        final int mask = newN - 1; // Note that this is used by the hashing macro
        final long[] newKey = new long[newN + 1];
        final String[] newValue = (String[])new Object[newN + 1];
        int i = n, pos;
        for (int j = realSize(); j-- != 0;) {
            while (((key[--i]) == (0)));
            if (!((newKey[pos = (int)it.unimi.dsi.fastutil.HashCommon.mix((key[i])) & mask]) == (0))) while (!((newKey[pos = (pos + 1) & mask]) == (0)));
            newKey[pos] = key[i];
            newValue[pos] = value[i];
        }
        newValue[newN] = value[n];
        n = newN;
        this.mask = mask;
        maxFill = maxFill(n, f);
        this.key = newKey;
        this.value = newValue;
    }

}
