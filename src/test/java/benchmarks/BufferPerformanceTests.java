package benchmarks;

import org.jctools.queues.MpscLinkedQueue;
import org.jctools.queues.SpscUnboundedArrayQueue;
import org.jctools.queues.unpadded.SpscUnboundedUnpaddedArrayQueue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Phaser;


public class BufferPerformanceTests {

    public long testLinkedTransferQueue() {
        long ms = System.currentTimeMillis();
        SpscUnboundedUnpaddedArrayQueue<Integer> a = new SpscUnboundedUnpaddedArrayQueue<>(1000);
        ArrayList<Thread> producers = new ArrayList<>();
        Phaser phaser = new Phaser();
        phaser.register();
        for (int i = 0; i < 1; i++) {
            producers.add(new Thread(() -> {
                phaser.register();
                for (int j = 0; j < 1E7; j++) {
                    a.add(j);
                }
                phaser.arrive();
            }));
        }
        producers.forEach(Thread::start);
        while (phaser.getArrivedParties() < 1) {
            a.drain((integer) -> {
                if (integer % 100000 == 0) System.out.println(integer);
            });
        }
        return System.currentTimeMillis() - ms;
    }

    public long testLinkedQueue() {
        long ms = System.currentTimeMillis();
        MpscLinkedQueue<Integer> a = new MpscLinkedQueue<>();
        ArrayList<Thread> producers = new ArrayList<>();
        Phaser phaser = new Phaser();
        phaser.register();
        for (int i = 0; i < 1; i++) {
            producers.add(new Thread(() -> {
                phaser.register();
                for (int j = 0; j < 1E7; j++) {
                    a.add(j);
                }
                phaser.arrive();
            }));
        }
        producers.forEach(Thread::start);
        while (phaser.getArrivedParties() < 1) {
            for (Integer integer : a) {
                if (integer % 100000 == 0) System.out.println(integer);
            }
        }
        return System.currentTimeMillis() - ms;
    }

    public long testSPSCQueue() {
        long ms = System.currentTimeMillis();
        SpscUnboundedArrayQueue<Integer> a = new SpscUnboundedArrayQueue<>(1000);
        ArrayList<Thread> producers = new ArrayList<>();
        Phaser phaser = new Phaser();
        phaser.register();
        for (int i = 0; i < 1; i++) {
            producers.add(new Thread(() -> {
                phaser.register();
                for (int j = 0; j < 1E7; j++) {
                    a.add(j);
                }
                phaser.arrive();
            }));
        }
        producers.forEach(Thread::start);
        while (phaser.getArrivedParties() < 1) {
            a.drain((tmp) -> {
                if (tmp != null) if (tmp % 100000 == 0) System.out.println(tmp);
            });
        }
        return System.currentTimeMillis() - ms;
    }

    @Test
    public void compareQueues() {
        long unpadded = testLinkedTransferQueue();
        long linkedQueue = 0;
        long spscQueue = testSPSCQueue();
        System.out.format("Unpadded-array: %s | Linked: %s | Array : %s\n", unpadded, linkedQueue, spscQueue);
    }

}
