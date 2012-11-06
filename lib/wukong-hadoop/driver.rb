module Wukong
  module Hadoop
    class Driver < Wukong::Driver

      attr_accessor :settings
      
      def self.run(settings)
        new(settings).run!
      end
      
      def initialize(settings)
        @settings = settings
      end
      
      def run!
        if (input_paths.blank? || output_path.blank?) && (not settings[:dry_run])
          raise "You need to specify a parsed input directory and a directory for output. Got #{input_paths} and #{output_path}"
        end        
        execute_hadoop_workflow
      end

      def this_script_filename
        Pathname.new(settings.rest.first).realpath
      end

      def ruby_interpreter_path
        Pathname.new(File.join(Config::CONFIG['bindir'], Config::CONFIG['RUBY_INSTALL_NAME'] + Config::CONFIG['EXEEXT'])).realpath
      end

      def input_paths
        @input_paths ||= settings[:input]
      end

      def output_path
        @output_path ||= settings[:output]
      end
      
      def wu_local_executable
        Wukong.local_runner_location
      end
      
      def mapper_commandline
        settings[:map_command]    || "#{ruby_interpreter_path} #{wu_local_executable} #{this_script_filename} --processor=mapper " + non_wukong_params        
      end
      
      def reducer_commandline
        settings[:reduce_command] || "#{ruby_interpreter_path} #{wu_local_executable} #{this_script_filename} --processor=reducer " + non_wukong_params                 
      end
      
      def job_name
        settings[:job_name]       || "#{File.basename(this_script_filename)}---#{input_paths}---#{output_path}".gsub(%r{[^\w/\.\-\+]+}, '')          
      end
      
      def non_wukong_params
        settings.reject{ |param, val| settings.definition_of(param, :wukong) }.map{ |param,val| "--#{param}=#{val}" }.join(" ")
      end

      def execute_command!(*args)
        command = args.flatten.reject(&:blank?).join(" \\\n    ")
        Log.info "Running\n\n#{command}\n"
        if settings[:dry_run]
          Log.info '== [Not running preceding command: dry run] =='
        else
          overwrite_output_paths!(output_path) if settings[:rm] || settings[:overwrite]
          puts `#{command}`
          raise "Streaming command failed!" unless $?.success?
        end
      end
      
      def overwrite_output_paths! output_path
        cmd = %Q{#{settings[:hadoop_runner]} fs -rmr '#{output_path}'}
        Log.info "Removing output file #{output_path}: #{cmd}"
        puts `#{cmd}`
      end

      def hadoop_runner
        settings[:hadoop_runner] || File.join(settings[:hadoop_home], 'bin/hadoop')
      end

      def hadoop_recycle_env
        %w[RUBYLIB].map{ |var| %Q{-cmdenv '#{var}=#{ENV[var]}'} if ENV[var] }.compact
      end

      def hadoop_other_args
        extra_str_args  = [ settings[:extra_args] ]
        if settings[:split_on_xml_tag]
          extra_str_args << %Q{-inputreader 'StreamXmlRecordReader,begin=<#{options.split_on_xml_tag}>,end=</#{options.split_on_xml_tag}>'}
        end
        extra_str_args   << ' -lazyOutput' if settings[:noempty]  # don't create reduce file if no records
        extra_str_args   << ' -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner' unless settings[:partition_fields].blank?
        extra_str_args
      end

      def hadoop_jobconf_options
        jobconf_options = []
        settings[:reuse_jvms]          = '-1'    if     (settings[:reuse_jvms] == true)
        settings[:respect_exit_status] = 'false' if     (settings[:ignore_exit_status] == true)
        # If no reducer and no reduce_command, then skip the reduce phase
        settings[:reduce_tasks]        = 0       unless (settings[:reduce_command] || settings[:reduce_tasks])
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

      # emit a -jobconf hadoop option if the simplified command line arg is present
      # if not, the resulting nil will be elided later
      def jobconf option
        "-D %s=%s" % [settings.definition_of(option, :description), settings[option]] if settings[option]
      end
      
      # Assemble the hadoop command to execute
      # and launch the hadoop runner to execute the script across all tasktrackers
      def execute_hadoop_workflow
        hadoop_commandline = [
                              hadoop_runner,
                              "jar #{settings[:hadoop_home]}/contrib/streaming/hadoop-*streaming*.jar",
                              hadoop_jobconf_options,
                              "-D mapred.job.name='#{job_name}'",
                              hadoop_other_args,
                              "-mapper  '#{mapper_commandline}'",
                              "-reducer '#{reducer_commandline}'",
                              "-input   '#{input_paths}'",
                              "-output  '#{output_path}'",
                              "-file    '#{this_script_filename}'",
                              hadoop_recycle_env,
                             ].flatten.compact.join(" \t\\\n  ")
        Log.info "  Launching hadoop!"
        execute_command!(hadoop_commandline)
      end
      
    end
  end
end
