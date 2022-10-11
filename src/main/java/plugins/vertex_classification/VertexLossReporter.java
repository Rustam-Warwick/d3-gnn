package plugins.vertex_classification;

//
///**
// * Reports the model accuracy with new data
// * Assumes to be in the last layer
// * Expects vertexOutputLayer
// * Feature -> last last vertex embedding
// * testLabel -> Label to test by
// */
//public class VertexLossReporter extends Plugin {
//    protected final String modelName;
//    public transient VertexOutputLayer output;
//    public int totalCorrect = 0;
//    public int totalTested = 0;
//
//    public VertexLossReporter(String modelName) {
//        super(String.format("%s-loss", modelName));
//        this.modelName = modelName;
//    }
//
//    @Override
//    public void open() throws Exception {
//        super.open();
//        output = (VertexOutputLayer) storage.getPlugin(String.format("%s-inferencer", modelName));
//        storage.layerFunction
//                .getRuntimeContext()
//                .getMetricGroup()
//                .gauge("loss", new MyGauge());
//    }
//
//    @Override
//    public void addElementCallback(GraphElement element) {
//        super.addElementCallback(element);
//        if (element.elementType() == ElementType.FEATURE) {
//            Feature<?, ?> feature = (Feature<?, ?>) element;
//            if (("testLabel".equals(feature.getName()) || "f".equals(feature.getName())) && feature.attachedTo.f0 == ElementType.VERTEX) {
//                Vertex parent = (Vertex) feature.getElement();
//                if (trainLossReady(parent)) {
//                    NDArray maxArg = output.output(new NDList((NDArray) parent.getFeature("f").getValue()), false).get(0).argMax();
//                    NDArray label = (NDArray) parent.getFeature("testLabel").getValue();
//                    if (maxArg.eq(label).getBoolean()) {
//                        totalCorrect++;
//                    }
//                    totalTested++;
//                }
//            }
//        }
//    }
//
//    @Override
//    public void updateElementCallback(GraphElement newElement, GraphElement oldElement) {
//        super.updateElementCallback(newElement, oldElement);
//        if (newElement.elementType() == ElementType.FEATURE) {
//            Feature<?, ?> newFeature = (Feature<?, ?>) newElement;
//            Feature<?, ?> oldFeature = (Feature<?, ?>) oldElement;
//            if ("f".equals(newFeature.getName()) && newFeature.attachedTo.f0 == ElementType.VERTEX) {
//                Vertex parent = (Vertex) newFeature.getElement();
//                if (trainLossReady(parent)) {
//                    NDArray maxArgNew = output.output(new NDList((NDArray) newFeature.getValue()), false).get(0).argMax();
//                    NDArray maxArgOld = output.output(new NDList((NDArray) oldFeature.getValue()), false).get(0).argMax();
//                    NDArray label = (NDArray) parent.getFeature("testLabel").getValue();
//                    if (maxArgOld.eq(label).getBoolean() && !maxArgNew.eq(label).getBoolean()) {
//                        // Old was correct now it is not correct
//                        totalCorrect--;
//                    }
//                    if (!maxArgOld.eq(label).getBoolean() && maxArgNew.eq(label).getBoolean()) {
//                        // Old was wrong now it is correct
//                        totalCorrect++;
//                    }
//                }
//            }
//        }
//    }
//
//    public boolean trainLossReady(Vertex v) {
//        return Objects.nonNull(v) && Objects.nonNull(v.getFeature("f")) && Objects.nonNull(v.getFeature("testLabel"));
//    }
//
//
//    class MyGauge implements Gauge<Integer> {
//        private final transient File outputFile;
//
//        public MyGauge() {
//            outputFile = new File("/Users/rustamwarwick/Desktop/output.txt");
//            try {
//                outputFile.createNewFile();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        @Override
//        public Integer getValue() {
//            float accuracy = (float) totalCorrect / totalTested;
//            try {
//                Files.write(outputFile.toPath(), String.valueOf(accuracy).concat("\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            return (int) (accuracy * 1000);
//        }
//    }
//
//
//}
