module Wukong
  module Hadoop

    # A separate module to allow easy overriding from other plugins.
    module Overwritables

      # The input paths to read from.
      #
      # @return [String]
      def input_paths
        (settings[:input] || [])
      end

      # The output path to write to.
      #
      # @return [String]
      def output_path
        settings[:output]
      end

      # Returns the parameters to pass to invocations of `wu-local`.
      #
      # This is separated out as a separate method so that it can
      # easily be overriden by other plugins.
      #
      # @return [Configliere::Param]
      def params_to_pass
        settings
      end
      
    end
  end
end

  
