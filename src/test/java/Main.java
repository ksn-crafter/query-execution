import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class Main {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(com.dqs.eventdrivensearch.queryExecution.search.io.S3SearchResultWriter.class.getSimpleName())
                .include(com.dqs.eventdrivensearch.queryExecution.search.index.SingleIndexSearcher.class.getSimpleName())
                .forks(1)
                .warmupIterations(2)
                .measurementIterations(10)
                .build();

        new Runner(opt).run();
    }
}