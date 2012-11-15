module Wukong
  module Hadoop
    module HadoopInvocation

      def ensure_input_and_output!
        raise Error.new("Explicit --input and --output paths are required to run a job in Hadoop mode.") if input_paths.nil? || input_paths.empty? || output_path.nil? || output_path.empty?
      end

      def overwrite!
        cmd = %Q{#{settings[:hadoop_runner]} fs -rmr '#{output_path}'}
        Log.info "Removing output file #{output_path}: #{cmd}"
        puts `#{cmd}`
      end

      def hadoop_commandline
        [
         hadoop_runner,
         "jar #{settings[:hadoop_home]}/contrib/streaming/hadoop-*streaming*.jar",
         hadoop_jobconf_options,
         "-D mapred.job.name='#{job_name}'",
         hadoop_other_args,
         "-mapper       '#{mapper_commandline}'",
         "-reducer      '#{reducer_commandline}'",
         "-input        '#{input_paths}'",
         "-output       '#{output_path}'",
         "-file         '#{mapper_file}'", # FIXME what about reducer file?
         io_formats,
         hadoop_recycle_env,
        ].flatten.compact.join(" \t\\\n  ")
      end

      def job_name
        return settings[:job_name] if settings[:job_name]
        relevant_filename = [mapper_file, reducer_file].uniq.map { |path| File.basename(path, '.rb') }.join('-')
        "#{relevant_filename}---#{input_paths}---#{output_path}".gsub(%r{[^\w/\.\-\+]+}, '')
      end
      
      def hadoop_runner
        settings[:hadoop_runner] || File.join(settings[:hadoop_home], 'bin/hadoop')
      end

      def hadoop_jobconf_options
        jobconf_options = []
        settings[:reuse_jvms]          = '-1'    if     (settings[:reuse_jvms] == true)
        settings[:respect_exit_status] = 'false' if     (settings[:ignore_exit_status] == true)
        # If no reducer and no reduce_command, then skip the reduce phase
        settings[:reduce_tasks]        = 0       unless (reduce? || settings[:reduce_tasks].nil?)
        # Fields hadoop should use to distribute records to reducers
        unless settings[:partition_fields].blank?
          jobconf_options += [jobconf(:partition_fields), jobconf(:output_field_separator)]
        end
        jobconf_options += [
                            :io_sort_mb,               :io_sort_record_percent,
                            :map_speculative,          :map_tasks,
                            :max_maps_per_cluster,     :max_maps_per_node,
                            :max_node_map_tasks,       :max_node_reduce_tasks,
                            :max_reduces_per_cluster,  :max_reduces_per_node,
                            :max_record_length,        :min_split_size,
                            :output_field_separator,   :key_field_separator,
                            :partition_fields,         :sort_fields,
                            :reduce_tasks,             :respect_exit_status,
                            :reuse_jvms,               :timeout,
                            :max_tracker_failures,     :max_map_attempts,
                            :max_reduce_attempts
                           ].map{ |opt| jobconf(opt)}
        jobconf_options.flatten.compact
      end

      def hadoop_other_args
        extra_str_args  = parsed_java_opts
        if settings[:split_on_xml_tag]
          extra_str_args << %Q{-inputreader 'StreamXmlRecordReader,begin=<#{options.split_on_xml_tag}>,end=</#{options.split_on_xml_tag}>'}
        end
        extra_str_args   << ' -lazyOutput' if settings[:noempty]  # don't create reduce file if no records
        extra_str_args   << ' -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner' unless settings[:partition_fields].blank?
        extra_str_args
      end

      def ruby_interpreter_path
        Pathname.new(File.join(Config::CONFIG['bindir'], Config::CONFIG['RUBY_INSTALL_NAME'] + Config::CONFIG['EXEEXT'])).realpath
      end
      
      def use_alternative_gemfile
        ENV['BUNDLE_GEMFILE'] = settings[:gemfile]
      end

      def hadoop_recycle_env
        use_alternative_gemfile if settings[:gemfile]
        %w[BUNDLE_GEMFILE].map{ |var| %Q{-cmdenv       '#{var}=#{ENV[var]}'} if ENV[var] }.compact
      end

      def parsed_java_opts
        settings[:java_opts].map do |java_opt| 
          java_opt.split('-D').reject{ |opt| opt.blank? }.map{ |opt| '-D ' + opt.strip }
        end.flatten
      end

      # emit a -jobconf hadoop option if the simplified command line arg is present
      # if not, the resulting nil will be elided later
      def jobconf option
        "-D %s=%s" % [settings.definition_of(option, :description), settings[option]] if settings[option]
      end

      def io_formats
        input  = "-inputformat  '#{settings[:input_format]}'"  if settings[:input_format]
        output = "-outputformat '#{settings[:output_format]}'" if settings[:output_format]
        [input, output]
      end
    end
  end
end
