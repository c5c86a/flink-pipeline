#!/usr/bin/env bash -ex

declare -a path2py=(
    "word_count/word_count.py",
    "data_enrichment/data_enrichment.py",
    "mandelbrot/mandelbrot_set.py",
    "template_example/application.py",
    "trending_hashtags/trending_hashtags.py",
    "mean_values/mean_values.py")

for i in "${arr[@]}"
do
   cd examples
   python runner.py "$i"
   cd -
   cd target-flink/flink-1.5.2/log
   grep -H "" flink-*-taskexecutor-*.out
   cd -
done


