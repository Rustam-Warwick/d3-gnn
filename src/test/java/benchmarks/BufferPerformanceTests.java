package benchmarks;

import org.jctools.queues.SpscLinkedQueue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Phaser;

public class BufferPerformanceTests {

    public long testLinkedTransferQueue() {
        long ms = System.currentTimeMillis();
        LinkedTransferQueue<Integer> a = new LinkedTransferQueue<>();
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

    public long testLinkedQueue() {
        long ms = System.currentTimeMillis();
        ConcurrentLinkedQueue<Integer> a = new ConcurrentLinkedQueue<>();
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
        SpscLinkedQueue<Integer> a = new SpscLinkedQueue<>();
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
        long linkedTransferQueue = testLinkedTransferQueue();
        long linkedQueue = testLinkedQueue();
        long spscQueue = testSPSCQueue();
        System.out.format("Transfer: %s | Linked: %s | SPSC: %s\n", linkedTransferQueue, linkedQueue, spscQueue);
    }

}
