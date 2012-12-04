module Wukong
  module Hadoop

    # Configure the given settings object for use with Wukong::Hadoop.
    #
    # @param [Configliere::Param] settings the settings to configure
    # @return [Configliere::Param the configured settings
    def self.configure settings
      # Hadoop Options
      settings.define :hadoop_home,             wukong_hadoop: true,                description: 'Path to hadoop installation. HADOOP_HOME/bin/hadoop is used to run hadoop.', env_var: 'HADOOP_HOME', default: '/usr/lib/hadoop'
      settings.define :hadoop_runner,           wukong_hadoop: true,                description: 'Path to hadoop executable. Use this for non-standard hadoop installations.'

      # Translate simplified args to their hairy hadoop equivalents
      settings.define :io_sort_mb,              wukong_hadoop: true, jobconf: true, description: 'io.sort.mb'
      settings.define :io_sort_record_percent,  wukong_hadoop: true, jobconf: true, description: 'io.sort.record.percent'
      settings.define :job_name,                wukong_hadoop: true, jobconf: true, description: 'mapred.job.name'
      settings.define :key_field_separator,     wukong_hadoop: true, jobconf: true, description: 'map.output.key.field.separator'
      settings.define :map_speculative,         wukong_hadoop: true, jobconf: true, description: 'mapred.map.tasks.speculative.execution'
      settings.define :map_tasks,               wukong_hadoop: true, jobconf: true, description: 'mapred.map.tasks'
      settings.define :max_maps_per_cluster,    wukong_hadoop: true, jobconf: true, description: 'mapred.max.maps.per.cluster'
      settings.define :max_maps_per_node,       wukong_hadoop: true, jobconf: true, description: 'mapred.max.maps.per.node'
      settings.define :max_node_map_tasks,      wukong_hadoop: true, jobconf: true, description: 'mapred.tasktracker.map.tasks.maximum'
      settings.define :max_node_reduce_tasks,   wukong_hadoop: true, jobconf: true, description: 'mapred.tasktracker.reduce.tasks.maximum'
      settings.define :max_record_length,       wukong_hadoop: true, jobconf: true, description: 'mapred.linerecordreader.maxlength' 
      settings.define :max_reduces_per_cluster, wukong_hadoop: true, jobconf: true, description: 'mapred.max.reduces.per.cluster'
      settings.define :max_reduces_per_node,    wukong_hadoop: true, jobconf: true, description: 'mapred.max.reduces.per.node'
      settings.define :max_tracker_failures,    wukong_hadoop: true, jobconf: true, description: 'mapred.max.tracker.failures'
      settings.define :max_map_attempts,        wukong_hadoop: true, jobconf: true, description: 'mapred.map.max.attempts'
      settings.define :max_reduce_attempts,     wukong_hadoop: true, jobconf: true, description: 'mapred.reduce.max.attempts'
      settings.define :min_split_size,          wukong_hadoop: true, jobconf: true, description: 'mapred.min.split.size'
      settings.define :output_field_separator,  wukong_hadoop: true, jobconf: true, description: 'stream.map.output.field.separator'
      settings.define :partition_fields,        wukong_hadoop: true, jobconf: true, description: 'num.key.fields.for.partition'
      settings.define :reduce_tasks,            wukong_hadoop: true, jobconf: true, description: 'mapred.reduce.tasks'
      settings.define :respect_exit_status,     wukong_hadoop: true, jobconf: true, description: 'stream.non.zero.exit.is.failure'
      settings.define :reuse_jvms,              wukong_hadoop: true, jobconf: true, description: 'mapred.job.reuse.jvm.num.tasks'
      settings.define :sort_fields,             wukong_hadoop: true, jobconf: true, description: 'stream.num.map.output.key.fields'
      settings.define :timeout,                 wukong_hadoop: true, jobconf: true, description: 'mapred.task.timeout'
      settings.define :noempty,                 wukong_hadoop: true,                description: "Don't create zero-byte reduce files"
      settings.define :split_on_xml_tag,        wukong_hadoop: true,                description: "Parse XML document by specifying the tag name: 'anything found between <tag> and </tag> will be treated as one record for map tasks'"
      settings.define :input_format,            wukong_hadoop: true,                description: 'Fully qualified Java class name defining an alternative InputFormat.'
      settings.define :output_format,           wukong_hadoop: true,                description: 'Fully qualified Java class name defining an alternative OutputFormat.'
      settings.define :java_opts,               wukong_hadoop: true,                description: 'Additional Java options to be passed to hadoop streaming.', :type => Array, :default => []
      settings.define :files,                   wukong_hadoop: true,                description: "Comma-separated list of files (or globs) to be copied to the MapReduce cluster (-files).", :type => Array, :default => []
      settings.define :jars,                    wukong_hadoop: true,                description: "Comma-separated list of jars (or globs) to include on the Hadoop CLASSPATH (-libjars).", :type => Array, :default => []
      settings.define :archives,                wukong_hadoop: true,                description: "Comma-separated list of archives to be unarchived on each worker (-archives).", :type => Array, :default => []

      # Options given on the command-line
      settings.define :mode,           description: "Run in either 'hadoop' or 'local' mode",                                        wukong_hadoop: true, :default => 'hadoop'
      settings.define :map_command,    description: "Shell command to run as mapper, in place of a constructed wu-local command",    wukong_hadoop: true
      settings.define :reduce_command, description: "Shell command to run as reducer, in place of a constructed wu-local command",   wukong_hadoop: true
      settings.define :sort_command,   description: "Shell command to run as sorter (only in `local' mode)",             wukong_hadoop: true, :default => 'sort'
      settings.define :command_prefix, description: "Prefex to insert before all Wukong commands",                       wukong_hadoop: true
      settings.define :mapper,         description: "Name of processor to use as a mapper",                              wukong_hadoop: true
      settings.define :reducer,        description: "Name of processor to use as a reducer",                             wukong_hadoop: true
      settings.define :gemfile,        description: "Specify an alternative Gemfile to execute this wukong script with", wukong_hadoop: true 
      settings.define :dry_run,        description: "Echo the command that will be run, but don't run it",               wukong_hadoop: true, :type => :boolean, :default => false
      settings.define :rm,             description: "Recursively remove the destination directory.",                     wukong_hadoop: true, :type => :boolean, :default => false
      settings.define :input,          description: "Comma-separated list of input paths",                               wukong_hadoop: true
      settings.define :output,         description: "Output path.",                                                      wukong_hadoop: true

      settings.use(:commandline)

      def settings.usage()
        "usage: #{File.basename($0)} PROCESSOR|FLOW [PROCESSOR|FLOW] [ --param=value | -p value | --param | -p]"
      end

      settings.description = <<EOF
wu-hadoop is a tool to model and launch Wukong processors as
map/reduce workflows within the Hadoop framework.

Use wu-hadoop with existing processors in `local' mode to test the
logic of your job, reading from the specified --input and printing to
STDOUT:

  $ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt
  a	2
  all	1
  and	2
  ...

where it is assumed that your mapper is called 'mapper' and your
reducer 'reducer'.  You can also cat in data:

  $ cat examples/sonnet_18.txt | wu-hadoop examples/word_count.rb --mode=local

Or pass options directly:

  $ wu-hadoop examples/word_count.rb  --mode=local --input=examples/sonnet_18.txt --fold_case --min_length=3
  all	1
  and	5
  art	1
  brag	1
  ...

Or define both processors in separate files:

  $ wu-hadoop examples/tokenizer.rb examples/counter.rb --mode=local --input=examples/sonnet_18.txt

Or by name:

  $ wu-hadoop examples/processors.rb --mode=local --input=examples/sonnet_18.txt --mapper=tokenizer --reducer=counter

Or just by command:

$ wu-hadoop processors.rb --mapper=tokenizer --reduce_command='uniq -c' ...
$ wu-hadoop processors.rb --map_command='cut -f3' --reducer=counter ...
$ wu-hadoop --map_command='cut -f3' --reduce_command='uniq -c' ...

If you don't specify a --reducer explicitly, and you didn't give two
separate arguments, and no processor named :reducer exists in the
environment, then we assume you are launching a map-only job and
'mapred.tasktracker.reduce.tasks.maximum' will correspondingly be set
to 0:

  $ wu-hadoop examples/tokenizer.rb --mode=local --input=examples/sonnet_18.txt
  Shall
  I
  compare
  thee
  ...

You can achieve this directly with the --reduce_tasks=0 option.

Many other Hadoop options have been wrapped with similarly friendly
names below.  These are ignored when running in `local' mode.

Some options (like `--sort_command') only make sense in `local' mode.
These are ignored in `hadoop' mode.
EOF
      settings
    end

    # All Hadoop configuration for Wukong lives within this object.
    Configuration = configure(Configliere::Param.new) unless defined? Configuration
  end
end
