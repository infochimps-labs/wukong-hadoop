module Wukong
  module Hadoop

    # Hadoop streaming exposes several environment variables to
    # scripts it executes.  This module contains methods that make
    # these variables easily accessed from within a processor.
    #
    # Since these environment variables are ultimately set by Hadoop's
    # streaming jar when executing inside Hadoop, you'll have to set
    # them manually when testing locally.
    #
    # Via @pskomoroch via @tlipcon:
    #
    #  "there is a little known Hadoop Streaming trick buried in this Python
    #   script. You will notice that the date is not actually in the raw log
    #   data itself, but is part of the filename. It turns out that Hadoop makes
    #   job parameters you would fetch in Java with something like
    #   job.get("mapred.input.file") available as environment variables for
    #   streaming jobs, with periods replaced with underscores:
    #
    #     filepath = os.environ["map_input_file"]
    #     filename = os.path.split(filepath)[-1]
    module EnvMethods

      # Fetch a parameter set by Hadoop streaming in the environment
      # of the currently executing process.
      #
      # @param [String] name the '.' separated parameter name to fetch
      # @return [String] the value from the process' environment
      def hadoop_streaming_parameter name
        ENV[name.gsub('.', '_')]
      end

      # Path of the (data) file currently being processed.
      #
      # @return [String]
      def input_file
        ENV['map_input_file']
      end

      # Directory of the (data) file currently being processed.
      #
      # @return [String]
      def input_dir
        ENV['mapred_input_dir']
      end

      # Offset of the chunk currently being processed within the current input file.
      #
      # @return [String]
      def map_input_start_offset
        ENV['map_input_start']
      end

      # Length of the chunk currently being processed within the current input file.
      #
      # @return [String]
      def map_input_length
        ENV['map_input_length']
      end

      # ID of the current map/reduce attempt.
      #
      # @return [String]
      def attempt_id
        ENV['mapred_task_id']
      end

      # ID of the current map/reduce task.
      #
      # @return [String]
      def curr_task_id
        ENV['mapred_tip_id']
      end
      
    end
  end

  Processor.class_eval{ include Hadoop::EnvMethods }
end
