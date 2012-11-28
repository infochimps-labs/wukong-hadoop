module Wukong
  module Hadoop

    # Provides methods for determining input and output paths.
    # Written as a separate module to allow easy overriding from other
    # plugins.
    module InputsAndOutputs

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

    end
  end
end

  
