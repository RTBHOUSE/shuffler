package com.rtbhouse.shuffler;

import static com.rtbhouse.shuffler.CrunchUtils.mapFn;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.plan.PlanningParameters;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Shuffler extends Configured implements Tool, Serializable {

    public static String CANONICAL_NAME = Shuffler.class.getCanonicalName();
    public static String SIMPLE_NAME = Shuffler.class.getSimpleName();

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Shuffler(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: hadoop jar JAR " + CANONICAL_NAME + " INPUT... OUTPUT");
            System.err.println();

            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        Pipeline pipeline = new MRPipeline(Shuffler.class, SIMPLE_NAME, getConf());
        pipeline.getConfiguration().set(PlanningParameters.PIPELINE_DOTFILE_OUTPUT_DIR, "/tmp");
        pipeline.getConfiguration().set(RuntimeParameters.LOG_JOB_PROGRESS, "true");
        // pipeline.enableDebug();

        List<Path> inputs = Arrays.stream(args)
                .limit(args.length - 1)
                .map(x -> new Path(x))
                .collect(Collectors.toList());

        Path output = new Path(args[args.length - 1]);

        PCollection<String> lines = pipeline.read(From.textFile(inputs));

        PCollection<Pair<Double, String>> pairs =
                lines.parallelDo(mapFn(x -> Pair.of(Math.random(), x)), 
                        Writables.pairs(Writables.doubles(), Writables.strings()));

        PCollection<Pair<Double, String>> sorted = Sort.sort(pairs);
                
        PCollection<String> stripped = sorted
                .parallelDo(mapFn(x -> x.second()), Writables.strings());

        pipeline.write(stripped, To.textFile(output));

        return pipeline.done().succeeded() ? 0 : 1;
    }
}
