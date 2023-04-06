package benchmarks;


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * ThreadLocal has a very similar performance to non-thread local variant
 * Can be used in {@link elements.Plugin}
 */
@Disabled
public class ThreadLocalTest {

    @RepeatedTest(value = 20)
    public void testThreadLocal() throws Exception {
        long msStart = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 50; i++) {
            executorService.submit(new WithThreadLocal());
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        System.out.format("ThreadLocal took %s \n", System.currentTimeMillis() - msStart);
    }

    @RepeatedTest(value = 20)
    public void testNonThreadLocal() throws Exception {
        long msStart = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (int i = 0; i < 50; i++) {
            executorService.submit(new NoThreadLocal());
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        System.out.format("Non-Thread Local took %s \n", System.currentTimeMillis() - msStart);
    }

    static class WithThreadLocal implements Runnable {
        static ThreadLocal<Integer> counter;

        @Override
        public void run() {
            counter.set(0);
            for (int i = 0; i < 1000000000; i++) {
                counter.set(counter.get() + 1);
            }
        }
    }

    static class NoThreadLocal implements Runnable {
        int counter;

        @Override
        public void run() {
            for (int i = 0; i < 1000000000; i++) {
                counter++;
            }
        }
    }
}
