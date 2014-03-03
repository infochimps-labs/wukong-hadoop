module Wukong
  module Hadoop

    # Provides methods for executing a map/reduce job on a Hadoop
    # cluster via {Hadoop
    # streaming}[http://hadoop.apache.org/docs/r0.15.2/streaming.html].
    module HadoopInvocation

      # Remove the output path.
      #
      # Will not actually do anything if the <tt>--dry_run</tt> option
      # is also given.
      def remove_output_path
        execute_command("#{hadoop_runner} fs -rmr '#{output_path}'")
      end

      # Return the Hadoop command used to launch this job in a Hadoop
      # cluster.
      #
      # You should be able to copy, paste, and run this command
      # unmodified when debugging.
      #
      # @return [String]
      def hadoop_commandline
        [
         hadoop_runner,
         "jar #{hadoop_streaming_jar}",
         hadoop_jobconf_options,
         "-D mapred.job.name='#{job_name}'",
         hadoop_files,
         hadoop_other_args,
         "-mapper       '#{mapper_commandline}'",
         "-reducer      '#{reducer_commandline}'",
         "-input        '#{input_paths}'",
         "-output       '#{output_path}'",
         io_formats,
         hadoop_recycle_env,
        ].flatten.compact.join(" \t\\\n  ")
      end

      # The job name that will be passed to Hadoop.
      #
      # Respects the <tt>--job_name</tt> option if given, otherwise
      # constructs one from the given processors, input, and output
      # paths.
      #
      # @return [String]
      def job_name
        return settings[:job_name] if settings[:job_name]
        relevant_filename = args.compact.uniq.map { |path| File.basename(path, '.rb') }.join('-')
        "#{relevant_filename}---#{input_paths}---#{output_path}".gsub(%r{[^\w/\.\-\+]+}, '')
      end

      # The input format to use.
      #
      # Respects the value of <tt>--input_format</tt>.
      #
      # @return [String]
      def input_format
        settings[:input_format]
      end

      # The output format to use.
      #
      # Respects the value of <tt>--output_format</tt>.
      #
      # @return [String]
      def output_format
        settings[:output_format]
      end

      # :nodoc:
      def io_formats
        input  = "-inputformat  '#{input_format}'"  if input_format
        output = "-outputformat '#{output_format}'" if output_format
        [input, output]
      end
      
      # The name of the Hadoop binary to use.
      #
      # Respects the value of <tt>--hadoop_runner</tt> if given.
      #
      # @return [String]
      def hadoop_runner
        settings[:hadoop_runner] || 'hadoop'
      end

      # The path (glob) to the Hadoop streaming jar.
      #
      # Respects the value of <tt>--hadoop_streaming_jar</tt> if
      # given.  Otherwise uses the default CDH4 location.
      #
      # @return [String]
      def hadoop_streaming_jar
        settings[:hadoop_streaming_jar] || '/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh*.jar'
      end

      # Return an array of jobconf (-D) options that will be passed to Hadoop.
      #
      # Translates the "friendly" <tt>wu-hadoop</tt> names into the
      # less-friendly Hadoop names.
      #
      # @return [Array<String>]
      def hadoop_jobconf_options
        jobconf_options = []
        settings[:reuse_jvms]          = '-1'    if     (settings[:reuse_jvms] == true)
        settings[:respect_exit_status] = 'false' if     (settings[:ignore_exit_status] == true)
        # If no reducer and no reduce_command, then skip the reduce phase
        settings[:reduce_tasks]      ||= 0       unless reduce?
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
                            :max_reduce_attempts,      :reduce_speculative
                           ].map do |opt|
          defn = settings.definition_of(opt, :description)
          val  = settings[opt]
          java_opt(defn, val)
        end
        jobconf_options.flatten.compact
      end

      # Returns other arguments used by Hadoop streaming.
      #
      # @return [String]
      def hadoop_other_args
        extra_str_args  = parsed_java_opts
        if settings[:split_on_xml_tag]
          extra_str_args << %Q{-inputreader 'StreamXmlRecordReader,begin=<#{options.split_on_xml_tag}>,end=</#{options.split_on_xml_tag}>'}
        end
        extra_str_args   << ' -lazyOutput' if settings[:noempty]  # don't create reduce file if no records
        extra_str_args   << ' -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner' unless settings[:partition_fields].blank?
        extra_str_args
      end

      # :nodoc:
      #
      # http://hadoop.apache.org/docs/r0.20.2/streaming.html#Package+Files+With+Job+Submissions
      def hadoop_files
        args.find_all { |arg| arg.to_s =~ /\.rb$/ }.each do |arg|
          settings[:files] << arg
        end

        [].tap do |files_options|
          {
            :files    => '-files        ',
            :jars     => '-libjars      ',
            :archives => '-archives     '
          }.each_pair do |file_type_name, file_option_name|
            unless settings[file_type_name].nil? || settings[file_type_name].empty?
              files = settings[file_type_name].map do |file_name_or_glob|
                # Don't glob on the HDFS
                file_type_name == :archives ? file_name_or_glob : [Dir[file_name_or_glob], file_name_or_glob]
              end.flatten.compact.uniq.join(',')
              files_options << "#{file_option_name}'#{files}'"
            end
          end
        end
      end

      # :nodoc:
      def ruby_interpreter_path
        Pathname.new(File.join(Config::CONFIG['bindir'], Config::CONFIG['RUBY_INSTALL_NAME'] + Config::CONFIG['EXEEXT'])).realpath
      end

      # :nodoc:
      def use_alternative_gemfile
        ENV['BUNDLE_GEMFILE'] = settings[:gemfile]
      end

      # :nodoc:
      def hadoop_recycle_env
        use_alternative_gemfile if settings[:gemfile]
        %w[BUNDLE_GEMFILE LANG].map{ |var| %Q{-cmdenv       '#{var}=#{ENV[var]}'} if ENV[var] }.compact
      end

      # :nodoc:
      def parsed_java_opts
        settings[:java_opts].map do |java_opt|
          java_opt.split('-D').reject{ |opt| opt.blank? }.map{ |opt| '-D ' + opt.strip }
        end.flatten
      end

      # :nodoc:
      def java_opt option, value
        "-D %s=%s" % [option, Shellwords.escape(value.to_s)] if value
      end

    end
  end
end
