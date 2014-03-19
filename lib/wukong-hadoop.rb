require 'wukong'

module Wukong
  # Wukong-Hadoop is a plugin for Wukong that lets you develop, test,
  # and run map/reduce type workflows both locally and in the context
  # of a Hadoop cluster.
  #
  # It comes with a binary program called <tt>wu-hadoop</tt> which
  # lets you execute Ruby files containing Wukong processors as well
  # as built-in Wukong widgets.
  module Hadoop
    include Plugin

    # Configure the given settings object for use with Wukong::Hadoop.
    #
    # Will only add settings if the `program_name` is `wu-hadoop`.
    #
    # @param [Configliere::Param] settings the settings to configure
    # @param [String] program_name the name of the currently executing program
    def self.configure settings, program_name
      return unless program_name == 'wu-hadoop'
      
      # Hadoop Options
      settings.define :hadoop_runner,           wukong_hadoop: true,                description: 'Path to hadoop executable. Use this for non-standard hadoop installations.'
      settings.define :hadoop_streaming_jar,    wukong_hadoop: true,                description: 'Path to hadoop streaming jar.  Use this for non-standard hadoop installations.'

      # Translate simplified args to their hairy hadoop equivalents
      settings.define :io_sort_mb,              wukong_hadoop: true, jobconf: true, description: 'io.sort.mb'
      settings.define :io_sort_record_percent,  wukong_hadoop: true, jobconf: true, description: 'io.sort.record.percent'
      settings.define :job_name,                wukong_hadoop: true, jobconf: true, description: 'mapred.job.name'
      settings.define :key_field_separator,     wukong_hadoop: true, jobconf: true, description: 'map.output.key.field.separator'
      settings.define :map_speculative,         wukong_hadoop: true, jobconf: true, description: 'mapred.map.tasks.speculative.execution'
      settings.define :reduce_speculative,      wukong_hadoop: true, jobconf: true, description: 'mapred.reduce.tasks.speculative.execution'
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
      settings.define :command_prefix, description: "Prefix to insert before all Wukong commands",                       wukong_hadoop: true
      settings.define :mapper,         description: "Name of processor to use as a mapper",                              wukong_hadoop: true
      settings.define :reducer,        description: "Name of processor to use as a reducer",                             wukong_hadoop: true
      settings.define :gemfile,        description: "Specify an alternative Gemfile to execute this wukong script with", wukong_hadoop: true 
      settings.define :dry_run,        description: "Echo the command that will be run, but don't run it",               wukong_hadoop: true, :type => :boolean, :default => false
      settings.define :rm,             description: "Recursively remove the destination directory.",                     wukong_hadoop: true, :type => :boolean, :default => false
      settings.define :input,          description: "Comma-separated list of input paths",                               wukong_hadoop: true
      settings.define :output,         description: "Output path.",                                                      wukong_hadoop: true
    end

    # Boots the Wukong::Hadoop plugin.
    #
    # @param [Configliere::Param] settings the settings to boot from
    # @param [String] root the root directory to boot in
    def self.boot settings, root
    end
    
  end
end

require 'wukong-hadoop/runner'
require 'wukong-hadoop/extensions'
