# Wukong-Hadoop

The Hadoop plugin for Wukong lets you run <a
href="http://github.com/infochimps-labs/wukong">Wukong processors</a>
through <a href="http://hadoop.apache.org/">Hadoop's</a> command-line
<a
href="http://hadoop.apache.org/docs/r0.15.2/streaming.html">streaming
interface</a>.

Before you use Wukong-Hadoop to develop, test, and write your Hadoop
jobs, you might want to read about <a href="http://github.com/infochimps-labs/wukong">Wukong</a>, write some
<a href="http://github.com/infochimps-labs/wukong#processors">simple processors</a>, and read about the structure of a <a href="http://en.wikipedia.org/wiki/MapReduce">map/reduce job</a>.

You might also want to check out some other projects which enrich the
Wukong and Hadoop experience:

* <a href="http://github.com/infochimps-labs/wonderdog">wonderdog</a>: Connect Wukong processors running within Hadoop to Elasticsearch as either a source or sink for data.
* <a href="http://github.com/infochimps-labs/wukong-deploy">wukong-deploy</a>: Orchestrate Wukong and other wu-tools together to support an application running on the Infochimps Platform.

<a name="installation"></a>
## Installation & Setup

Wukong-Hadoop can be installed as a RubyGem:

```
$ sudo gem install wukong-hadoop
```

If you actually want to run your map/reduce jobs on a Hadoop cluster,
you'll of course need one handy.  <a
href="http://github.com/infochimps-labs/ironfan">Ironfan</a> is a
great tool for building and managing Hadoop clusters and other
distributed infrastructure quickly and easily.

To run Hadoop jobs through Wukong-Hadoop, you'll need to move your
your Wukong code to each member of the Hadoop cluster, install
Wukong-Hadoop on each, and log in and launch your job fron one of
them.  Ironfan again helps with configuring this.

<a name="anatomy"></a>
## Anatomy of a map/reduce job

A map/reduce job consists of two separate phases, the **map** phase
and the **reduce** phase which are connected by an intermediary
**sort** phase.

The <tt>wu-hadoop</tt> command-line tool is used to run Wukong
processors in the shape of a map/reduce job, whether locally or on a
Hadoop cluster.

The examples used in this README are all taken from the
<tt>/examples</tt> directory within the Wukong-Hadoop source code.
They implement the usual "word count" example.

<a name="local"></a>
## Test and Develop Map/Reduce Jobs Locally

Hadoop is a powerful tool designed to process huge amounts of data
very quickly.  It's not designed to make developing Hadoop jobs
iterative and simple.  Wukong-Hadoop lets you define a map/reduce job
and execute it locally, on small amounts of sample data, then launch
that job into a Hadoop cluster when you're sure it works.

<a name="processors_to_mappers_and_reducers"></a>
### From Processors to Mappers & Reducers

Wukong processors can be used either for the map phase or the reduce
phase of a map/reduce job.  Different processors can be defined in
different <tt>.rb</tt> files or within the same one.

Map-phase processors would filter, transform, or otherwise modify
input records getting them ready for the reduce.  Reduce-phase
processors typically perform aggregative operations like counting,
grouping, averaging, &c.

Given that you've already created a map/reduce job (just like this
word count example that comes with Wukong-Hadoop), the first thing to
try is to run the job locally on sample input data in flat files.  The
<tt>--mode=local</tt> flag tells <tt>wu-hadoop</tt> to run in local
mode, suitable for development and testing of jobs:

```
$ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt
a	2
all	1
and	2
And	3
art	1
...
```

Wukong-Hadoop looks for processors named <tt>:mapper</tt> and a
<tt>:reducer</tt> in the <tt>word_count.rb</tt> file.  To understand
what's going on under the hood, pass the <tt>--dry_run</tt> option:

```
$ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt --dry_run
I, [2012-11-27T19:24:21.238429 #20104]  INFO -- : Dry run:
cat examples/sonnet_18.txt | wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=mapper | sort | wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=reducer
```

which shows that <tt>wu-hadoop</tt> is ultimately relying on
<tt>wu-local</tt> to do the heavy-lifting.  You can copy, paste, and
run this longer command (or a portion of it) when debugging.

You can also pass options to your processors:

```
$ wu-hadoop examples/word_count.rb  --mode=local --input=examples/sonnet_18.txt --fold_case --min_length=3
all	1
and	5
art	1
brag	1
...
```

Sometimes you may want to use a given processor in multiple jobs.  You
can therefore define each processor in separate files if you want.  If
Wukong-Hadoop doesn't find processors named <tt>:mapper</tt> and
<tt>:reducer</tt> it will try to use processors named after the files
you pass it:

```
$ wu-hadoop examples/tokenizer.rb examples/counter.rb --mode=local --input=examples/sonnet_18.txt
a	2
all	1
and	2
And	3
art	1
...
```

You can also just specify the processors you want to run using the
<tt>--mapper</tt> and <tt>--reducer</tt> options:

```
$ wu-hadoop examples/processors.rb --mode=local --input=examples/sonnet_18.txt --mapper=tokenizer --reducer=counter
a	2
all	1
and	2
And	3
art	1
...
```

<a name="map_only"></a>
### Map-Only Jobs

If Wukong-Hadoop can't find a processor named <tt>:reducer</tt> (and
you didn't give it two files explicitly) then it will run a map-only
job:

```
$ wu-hadoop examples/tokenizer.rb --mode=local --input=examples/sonnet_18.txt
Shall
I
compare
thee
...
```

You can force this behavior using using the <tt>--reduce_tasks</tt>
option:

```
$ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt --reduce_tasks=0
Shall
I
compare
thee
...
```

<a name="sort_options"></a>
### Sort Options

For some kinds of jobs, you may have special requirements about how
you sort.  You can specify an explicit <tt>--sort_command</tt> option:

```
$ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt --sort_command='sort -r'
winds	1
When	1
wander'st	1
untrimm'd	1
...
```

<a name="non_wukong"></a>
### Something Other than Wukong/Ruby?

Wukong-Hadoop even lets you use mappers and reducers which aren't
themselves Wukong processors or even Ruby code.  The <tt>:counter</tt>
processor is here replaced by good old <tt>uniq</tt>:

```
$ wu-hadoop examples/processors.rb --mode=local --input=examples/sonnet_18.txt --mapper=tokenizer --reduce_command='uniq -c'
      2 a
      1 all
      2 and
      3 And
      1 art
...
```

This is a good method for getting a little performance bump (if your
job is CPU-bound) or even lifting other, non-Hadoop or non-Wukong
aware code into the Hadoop world:


```
$ wu-hadoop --mode=local --input=examples/sonnet_18.txt --map_command='python tokenizer.py' --reduce_command='python counter.py'
a	2
all	1
and	2
And	3
art	1
...
```

The only requirement on <tt>tokenizer.py</tt> and <tt>counter.py</tt>
is that they work the same way as their Ruby
<tt>Wukong::Processor</tt> equivalents: one line at a time from STDIN
to STDOUT.

<a name="hadoop"></a>
## Running in Hadoop

Once you've got your code working locally, you can easily make it run
inside of Hadoop by just changing the <tt>--mode</tt> option.  You'll
also need to specify <tt>--input</tt> and <tt>--output</tt> paths that
Hadoop can access, either on the <a
href="http://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_Distributed_File_System">HDFS</a>
or on something like Amazon's <a
href="http://aws.amazon.com/s3/">S3</a> if you're using AWS and have
properly configured your Hadoop cluster.

Here's the very first example from the <a href="#local">Local</a>
section above, but executed within a Hadoop cluster, reading and writing data from the HDFS.

```
$ wu-hadoop examples/word_count.rb --mode=hadoop --input=/data/sonnet_18.txt --output=/data/word_count.tsv
I, [2012-11-27T19:27:18.872645 #20142]  INFO -- : Launching Hadoop!
I, [2012-11-27T19:27:18.873477 #20142]  INFO -- : Running

/usr/lib/hadoop/bin/hadoop 	\
  jar /usr/lib/hadoop/contrib/streaming/hadoop-*streaming*.jar 	\
  -D mapred.job.name='word_count---/data/sonnet_18.txt---/data/word_count.tsv' 	\
  -mapper       'wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=mapper' 	\
  -reducer      'wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=reducer' 	\
  -input        '/data/sonnet_18.txt' 	\
  -output       '/data/word_count.tsv' 	\
12/11/28 01:32:09 INFO mapred.FileInputFormat: Total input paths to process : 1
12/11/28 01:32:10 INFO streaming.StreamJob: getLocalDirs(): [/mnt/hadoop/mapred/local, /mnt2/hadoop/mapred/local]
12/11/28 01:32:10 INFO streaming.StreamJob: Running job: job_201210241848_0043
12/11/28 01:32:10 INFO streaming.StreamJob: To kill this job, run:
12/11/28 01:32:10 INFO streaming.StreamJob: /usr/lib/hadoop/bin/hadoop job  -Dmapred.job.tracker=10.124.54.254:8021 -kill job_201210241848_0043
12/11/28 01:32:10 INFO streaming.StreamJob: Tracking URL: http://ip-10-124-54-254.ec2.internal:50030/jobdetails.jsp?jobid=job_201210241848_0043
12/11/28 01:32:11 INFO streaming.StreamJob:  map 0%  reduce 0%
...
```

Hadoop throws an error if your output path already exists.  If you're
running the same job over and over, it can be annoying to constantly
have to remember to delete the output path from your last run.  Use
the <tt>--rm</tt> option in this case to automatically remove the
output path before launching a Hadoop job (this only works for Hadoop
mode).

### Advanced Hadoop Usage

For small or lightweight jobs, all you have to do to move from local
to Hadoop is change the <tt>--mode</tt> flag when executing your jobs
with <tt>wu-hadoop</tt>.

More complicated jobs that require either special code to be available
(new input/output formats, <tt>CLASSPATH</tt> or <tt>RUBYLIB</tt>
hacking, &c.) or require tuning at the level of Hadoop to run
efficiently.

#### Other Input/Output Formats

Hadoop streaming uses the <a
href="http://hadoop.apache.org/docs/r0.20.1/api/org/apache/hadoop/mapred/TextInputFormat.html">TextInputFormat</a>
and <a
href="http://hadoop.apache.org/docs/r0.20.2/api/org/apache/hadoop/mapreduce/lib/output/TextOutputFormat.html">TextOutputFormat</a>
by default.  These turn all input/output data into newline delimited
string records which creates a perfect match for the command-line and
the local mode of Wukong-Hadoop.

Other input and output formats can be specified with the
<tt>--input_format</tt> and <tt>--output_format</tt> options.

#### Tuning

Hadoop offers many, many options for configuring a particular Hadoop
job as well as the Hadoop cluster itself.  Wukong-Hadoop wraps many of
these familiar options (<tt>mapred.map.tasks</tt>,
<tt>mapred.reduce.tasks</tt>, <tt>mapred.task.timeout</tt>, &c.) with
friendlier names (<tt>map_tasks</tt>, <tt>reduce_tasks</tt>,
<tt>timeout</tt>, &c.).  See a complete list using <tt>wu-hadoop
--help</tt>.

Java options themselves can be set directly using the
<tt>--java_opts</tt> flag.  You can also use the <tt>--dry_run</tt>
option again to see the constructed Hadoop invocation without running
it:

```
$ wu-hadoop examples/word_count.rb --mode=hadoop --input=/data/sonnet_18.txt --output=/data/word_count.tsv --java_opts='-D foo.bar=3 -D something.else=hello' --dry_run
I, [2012-11-27T19:47:08.872784 #20512]  INFO -- : Launching Hadoop!
I, [2012-11-27T19:47:08.873630 #20512]  INFO -- : Dry run:
/usr/lib/hadoop/bin/hadoop 	\
  jar /usr/lib/hadoop/contrib/streaming/hadoop-*streaming*.jar 	\
  -D mapred.job.name='word_count---/data/sonnet_18.txt---/data/word_count.tsv' 	\
  -D foo.bar=3 	\
  -D something.else=hello 	\
  -mapper       'wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=mapper' 	\
  -reducer      'wu-local /home/user/wukong-hadoop/examples/word_count.rb --run=reducer' 	\
  -input        '/data/sonnet_18.txt' 	\
  -output       '/data/word_count.tsv' 	\
```

#### Accessing Hadoop Runtime Data

Hadoop streaming exposes several environment variables to scripts it
executes, including mapper and reducer scripts launched by
<tt>wu-hadoop</tt>.  Instead of manually inspecting the <tt>ENV</tt>
within your Wukong processors, you can use the following methods
defined for commonly accessed parameters:

* <tt>input_file</tt>: Path of the (data) file currently being processed.
* <tt>input_dir</tt>: Directory of the (data) file currently being processed.
* <tt>map_input_start_offset</tt>: Offset of the chunk currently being processed within the current input file.
* <tt>map_input_length</tt>: Length of the chunk currently being processed within the current input file.
* <tt>attempt_id</tt>: ID of the current map/reduce attempt.
* <tt>curr_task_id</tt>: ID of the current map/reduce task.

or use the <tt>hadoop_streaming_parameter</tt> method for the others.
