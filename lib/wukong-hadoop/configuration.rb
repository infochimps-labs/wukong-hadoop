module Wukong
  module Hadoop
    Configuration = Configliere::Param.new unless defined? Configuration

    # Hadoop Options
    Configuration.define :hadoop_home,             wukong: true,                description: 'Path to hadoop installation. HADOOP_HOME/bin/hadoop is used to run hadoop.', env_var: 'HADOOP_HOME', default: '/usr/lib/hadoop'
    Configuration.define :hadoop_runner,           wukong: true,                description: 'Path to hadoop executable. Use this for non-standard hadoop installations.'

    # Translate simplified args to their hairy hadoop equivalents
    Configuration.define :io_sort_mb,              wukong: true, jobconf: true, description: 'io.sort.mb'
    Configuration.define :io_sort_record_percent,  wukong: true, jobconf: true, description: 'io.sort.record.percent'
    Configuration.define :job_name,                wukong: true, jobconf: true, description: 'mapred.job.name'
    Configuration.define :key_field_separator,     wukong: true, jobconf: true, description: 'map.output.key.field.separator'
    Configuration.define :map_speculative,         wukong: true, jobconf: true, description: 'mapred.map.tasks.speculative.execution'
    Configuration.define :map_tasks,               wukong: true, jobconf: true, description: 'mapred.map.tasks'
    Configuration.define :max_maps_per_cluster,    wukong: true, jobconf: true, description: 'mapred.max.maps.per.cluster'
    Configuration.define :max_maps_per_node,       wukong: true, jobconf: true, description: 'mapred.max.maps.per.node'
    Configuration.define :max_node_map_tasks,      wukong: true, jobconf: true, description: 'mapred.tasktracker.map.tasks.maximum'
    Configuration.define :max_node_reduce_tasks,   wukong: true, jobconf: true, description: 'mapred.tasktracker.reduce.tasks.maximum'
    Configuration.define :max_record_length,       wukong: true, jobconf: true, description: 'mapred.linerecordreader.maxlength' 
    Configuration.define :max_reduces_per_cluster, wukong: true, jobconf: true, description: 'mapred.max.reduces.per.cluster'
    Configuration.define :max_reduces_per_node,    wukong: true, jobconf: true, description: 'mapred.max.reduces.per.node'
    Configuration.define :max_tracker_failures,    wukong: true, jobconf: true, description: 'mapred.max.tracker.failures'
    Configuration.define :max_map_attempts,        wukong: true, jobconf: true, description: 'mapred.map.max.attempts'
    Configuration.define :max_reduce_attempts,     wukong: true, jobconf: true, description: 'mapred.reduce.max.attempts'
    Configuration.define :min_split_size,          wukong: true, jobconf: true, description: 'mapred.min.split.size'
    Configuration.define :output_field_separator,  wukong: true, jobconf: true, description: 'stream.map.output.field.separator'
    Configuration.define :partition_fields,        wukong: true, jobconf: true, description: 'num.key.fields.for.partition'
    Configuration.define :reduce_tasks,            wukong: true, jobconf: true, description: 'mapred.reduce.tasks'
    Configuration.define :respect_exit_status,     wukong: true, jobconf: true, description: 'stream.non.zero.exit.is.failure'
    Configuration.define :reuse_jvms,              wukong: true, jobconf: true, description: 'mapred.job.reuse.jvm.num.tasks'
    Configuration.define :sort_fields,             wukong: true, jobconf: true, description: 'stream.num.map.output.key.fields'
    Configuration.define :timeout,                 wukong: true, jobconf: true, description: 'mapred.task.timeout'
    Configuration.define :noempty,                 wukong: true,                description: "Don't create zero-byte reduce files"
    Configuration.define :split_on_xml_tag,        wukong: true,                description: "Parse XML document by specifying the tag name: 'anything found between <tag> and </tag> will be treated as one record for map tasks'"
    Configuration.define :input_format,            wukong: true,                description: 'Fully qualified Java class name defining an alternative InputFormat.'
    Configuration.define :output_format,           wukong: true,                description: 'Fully qualified Java class name defining an alternative OutputFormat.'
    Configuration.define :java_opts,               wukong: true,                description: 'Additional java options to be passed to hadoop streaming.', :type => Array, :default => []
  end
end
