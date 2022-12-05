package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.jetbrains.annotations.Nullable;

public class IterationHeadTask<OUT> extends StreamTask<OUT, StreamOperator<OUT>>{

    public IterationHeadTask(Environment env) throws Exception {
        super(env);
    }

    public IterationHeadTask(Environment env, @Nullable TimerService timerService) throws Exception {
        super(env, timerService);
    }

    public IterationHeadTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler);
    }

    public IterationHeadTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler, StreamTaskActionExecutor actionExecutor) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor);
    }

    public IterationHeadTask(Environment environment, @Nullable TimerService timerService, Thread.UncaughtExceptionHandler uncaughtExceptionHandler, StreamTaskActionExecutor actionExecutor, TaskMailbox mailbox) throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor, mailbox);
    }

    @Override
    protected void init() throws Exception {

    }
}
