#!/usr/bin/env sh

cd examples
python runner.py word_count/word_count.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -

cd examples
python runner.py data_enrichment/data_enrichment.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -

cd examples
python runner.py mandelbrot/mandelbrot_set.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -

cd examples
python runner.py template_example/application.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -

cd examples
python runner.py trending_hashtags/trending_hashtags.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -

cd examples
python runner.py mean_values/mean_values.py
cd -
cd target-flink/flink-1.5.2/log
grep -H "" flink-*-taskexecutor-*.out
cd -


