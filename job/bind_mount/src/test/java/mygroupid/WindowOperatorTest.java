package mygroupid;

import org.apache.flink.runtime.operators.testutils.MockEnvironment;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * There are some harness tests which can be used to test your function. It is also a common way to test function or operator in flink internal tests. Currently, the harness classes mainly include:
 * KeyedOneInputStreamOperatorTestHarness
 * KeyedTwoInputStreamOperatorTestHarness
 * OneInputStreamOperatorTestHarness
 * TwoInputStreamOperatorTestHarness
 * You can take a look at the source code of these classes.
 *
 * To be more specific, you can take a look at the testSlidingEventTimeWindowsApply[1], in which the RichSumReducer window function has been tested.
 *
 * [1] https://github.com/apache/flink/blob/master/flink-streaming-java/src/test/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperatorTest.java#L213
 */
@SuppressWarnings("serial")
public class WindowOperatorTest extends TestLogger {

    private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

    // For counting if close() is called the correct number of times on the SumReducer
    private static AtomicInteger closeCalled = new AtomicInteger(0);

    // late arriving event OutputTag<StreamRecord<IN>>
    private static final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-output") {};

    private static <OUT> OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(OneInputStreamOperator<Tuple2<String, Integer>, OUT> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
    }

    private void testSlidingEventTimeWindows(OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator) throws Exception {

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
                createTestHarness(operator);

        testHarness.setup();
        testHarness.open();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 999));
        expectedOutput.add(new Watermark(999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
        expectedOutput.add(new Watermark(1999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
        expectedOutput.add(new Watermark(2999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
        expectedOutput.add(new Watermark(3999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 4999));
        expectedOutput.add(new Watermark(4999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
        expectedOutput.add(new Watermark(5999));
        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

        testHarness.close();
    }
    @Test
    public void testSlidingEventTimeWindowsApply() throws Exception {
        closeCalled.set(0);
        final int windowSize = 3;
        final int windowSlide = 1;
        ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
                STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));
        WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
                SlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
                new TimeWindow.Serializer(),
                new TupleKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
                stateDesc,
                new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
                EventTimeTrigger.create(),
                0,
                null /* late data output tag */);
        testSlidingEventTimeWindows(operator);
        Assert.assertEquals("Close was not called.", 2, closeCalled.get());        // we close once in the rest...
    }
    //  UDFs
    private static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
        private static final long serialVersionUID = 1L;
        private boolean openCalled = false;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            openCalled = true;
        }
        @Override
        public void close() throws Exception {
            super.close();
            closeCalled.incrementAndGet();
        }
        @Override
        public void apply(String key,
                          W window,
                          Iterable<Tuple2<String, Integer>> input,
                          Collector<Tuple2<String, Integer>> out) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            int sum = 0;

            for (Tuple2<String, Integer> t: input) {
                sum += t.f1;
            }
            out.collect(new Tuple2<>(key, sum));
        }
    }
    @SuppressWarnings("unchecked")
    private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
                StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
                StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
                if (sr0.getTimestamp() != sr1.getTimestamp()) {
                    return (int) (sr0.getTimestamp() - sr1.getTimestamp());
                }
                int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
                if (comparison != 0) {
                    return comparison;
                } else {
                    return sr0.getValue().f1 - sr1.getValue().f1;
                }
            }
        }
    }
    private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
