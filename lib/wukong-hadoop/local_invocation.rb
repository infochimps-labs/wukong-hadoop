module Wukong
  module Hadoop
    module LocalInvocation

      def local_commandline
        [
         [cat_input, mapper_commandline].tap do |pipeline|
           pipeline.concat([sort_commandline, reducer_commandline]) if reduce?
         end.flatten.compact.join(' | '),
         cat_output
        ].flatten.compact.join(' ')
      end

      def cat_input
        return unless input_paths && (!input_paths.empty?)
        "cat #{input_paths}"
      end

      def sort_commandline
        settings[:sort_command]
      end

      def cat_output
        return unless output_path
        "> #{output_path}"
      end

    end
  end
end
