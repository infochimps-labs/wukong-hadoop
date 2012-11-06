module Wukong
  module Hadoop
    Configuration = Configliere::Param.new unless defined? Configuration

    # Hadoop Options
    Configuration.define :hadoop_home, :default => '/usr/lib/hadoop', :description => "Path to hadoop installation. HADOOP_HOME/bin/hadoop is used to run hadoop.", :env_var => 'HADOOP_HOME', :wukong => true
    Configuration.define :hadoop_runner,                              :description => "Path to hadoop script. Usually set --hadoop_home instead of this.", :wukong => true

    # Translate simplified args to their hairy hadoop equivalents
    Configuration.define :io_sort_mb,             :jobconf => true,   :description => 'io.sort.mb',                                             :wukong => true
    Configuration.define :io_sort_record_percent, :jobconf => true,   :description => 'io.sort.record.percent',                                 :wukong => true
    Configuration.define :job_name,               :jobconf => true,   :description => 'mapred.job.name',                                        :wukong => true
    Configuration.define :key_field_separator,    :jobconf => true,   :description => 'map.output.key.field.separator',                         :wukong => true
    Configuration.define :map_speculative,        :jobconf => true,   :description => 'mapred.map.tasks.speculative.execution',                 :wukong => true
    Configuration.define :map_tasks,              :jobconf => true,   :description => 'mapred.map.tasks',                                       :wukong => true
    Configuration.define :max_maps_per_cluster,   :jobconf => true,   :description => 'mapred.max.maps.per.cluster',                            :wukong => true
    Configuration.define :max_maps_per_node,      :jobconf => true,   :description => 'mapred.max.maps.per.node',                               :wukong => true
    Configuration.define :max_node_map_tasks,     :jobconf => true,   :description => 'mapred.tasktracker.map.tasks.maximum',                   :wukong => true
    Configuration.define :max_node_reduce_tasks,  :jobconf => true,   :description => 'mapred.tasktracker.reduce.tasks.maximum',                :wukong => true
    Configuration.define :max_record_length,      :jobconf => true,   :description => 'mapred.linerecordreader.maxlength',                      :wukong => true 
    Configuration.define :max_reduces_per_cluster,:jobconf => true,   :description => 'mapred.max.reduces.per.cluster',                         :wukong => true
    Configuration.define :max_reduces_per_node,   :jobconf => true,   :description => 'mapred.max.reduces.per.node',                            :wukong => true
    Configuration.define :max_tracker_failures,   :jobconf => true,   :description => 'mapred.max.tracker.failures',                            :wukong => true
    Configuration.define :max_map_attempts,       :jobconf => true,   :description => 'mapred.map.max.attempts',                                :wukong => true
    Configuration.define :max_reduce_attempts,    :jobconf => true,   :description => 'mapred.reduce.max.attempts',                             :wukong => true
    Configuration.define :min_split_size,         :jobconf => true,   :description => 'mapred.min.split.size',                                  :wukong => true
    Configuration.define :output_field_separator, :jobconf => true,   :description => 'stream.map.output.field.separator',                      :wukong => true
    Configuration.define :partition_fields,       :jobconf => true,   :description => 'num.key.fields.for.partition',                           :wukong => true
    Configuration.define :reduce_tasks,           :jobconf => true,   :description => 'mapred.reduce.tasks',                                    :wukong => true
    Configuration.define :respect_exit_status,    :jobconf => true,   :description => 'stream.non.zero.exit.is.failure',                        :wukong => true
    Configuration.define :reuse_jvms,             :jobconf => true,   :description => 'mapred.job.reuse.jvm.num.tasks',                         :wukong => true
    Configuration.define :sort_fields,            :jobconf => true,   :description => 'stream.num.map.output.key.fields',                       :wukong => true
    Configuration.define :timeout,                :jobconf => true,   :description => 'mapred.task.timeout',                                    :wukong => true
    Configuration.define :noempty,                                    :description => "Don't create zero-byte reduce files", :wukong => true
    Configuration.define :split_on_xml_tag,                           :description => "Parse XML document by specifying the tag name: 'anything found between <tag> and </tag> will be treated as one record for map tasks'", :wukong => true
  end
end
