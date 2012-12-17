require 'shellwords'
module Wukong
  module Hadoop

    # Provides methods for executing a map/reduce job locally on the
    # command-line.
    module LocalInvocation

      # Returns the full local command used by Wukong-Hadoop when
      # simulating a map/reduce job on the command-line.
      #
      # You should be able to run this commmand directly to simulate
      # the job yourself.
      #
      # @return [String]
      def local_commandline
        [
         [cat_input, mapper_commandline].tap do |pipeline|
           pipeline.concat([sort_commandline, reducer_commandline]) if reduce?
         end.flatten.compact.join(' | '),
         cat_output
        ].flatten.compact.join(' ')
      end

      # Returns the sort command used by Wukong-Hadoop when simulating
      # a map/reduce job on the command-line.
      #
      # @return [String]
      def sort_commandline
        settings[:sort_command]
      end

      # :nodoc:
      def cat_input
        return unless input_paths && (!input_paths.empty?)
        paths = Shellwords.join(input_paths.split(','))
        "cat #{paths}"
      end

      # :nodoc:
      def cat_output
        return unless output_path
        "> #{output_path}"
      end

    end
  end
end
