module Wukong
  module Hadoop
    module EnvMethods
      #
      # Via @pskomoroch via @tlipcon,
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
      #   Thanks to Todd Lipcon for directing me to that hack.
      #

      # HDFS pathname to the input file currently being processed.
      def input_file
        ENV['map_input_file']
      end

      # Directory of the input file
      def input_dir
        ENV['mapred_input_dir']
      end

      # Offset of this chunk within the input file
      def map_input_start_offset
        ENV['map_input_start']
      end

      # length of the mapper's input chunk
      def map_input_length
        ENV['map_input_length']
      end

      def attempt_id
        ENV['mapred_task_id']
      end
      def curr_task_id
        ENV['mapred_tip_id']
      end

      def script_cmdline_urlenc
        ENV['stream_map_streamprocessor']
      end
    end
  end
end

Wukong::Processor.class_eval{ include Wukong::Hadoop::EnvMethods }
