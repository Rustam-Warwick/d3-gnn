package helpers;

//
//public class KeepLastElement implements Evictor<GraphOp, TimeWindow> {
//    @Override
//    public void evictBefore(Iterable<TimestampedValue<GraphOp>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//        int firstCount = 0;
//        int secondCount = 0;
//        for (TimestampedValue<GraphOp> el : elements) {
//            else if (Objects.nonNull(el.getValue().getTwo())) secondCount++;
//        }
//
//        for (Iterator<TimestampedValue<GraphOp>> iterator = elements.iterator();
//             iterator.hasNext(); ) {
//            TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>> el = iterator.next();
//            if (Objects.nonNull(el.getValue().getOne()) && firstCount > 1) {
//                firstCount--;
//                iterator.remove();
//            } else if (Objects.nonNull(el.getValue().getTwo()) && secondCount > 1) {
//                secondCount--;
//                iterator.remove();
//            }
//        }
//    }
//
//    @Override
//    public void evictAfter(Iterable<TimestampedValue<CoGroupedStreams.TaggedUnion<GraphOp, GraphOp>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//
//    }
//}
