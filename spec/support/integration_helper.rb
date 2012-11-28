module Wukong
  module Hadoop
    module IntegrationHelper

      def root
        @root ||= Pathname.new(File.expand_path('../../..', __FILE__))
      end

      def lib_dir
        root.join('lib')
      end

      def bin_dir
        root.join('bin')
      end
      
      def examples_dir
        root.join('examples')
      end

      def integration_env
        {
          "PATH"    => [bin_dir.to_s, ENV["PATH"]].compact.join(':'),
          "RUBYLIB" => [lib_dir.to_s, ENV["RUBYLIB"]].compact.join(':')
        }
      end

      def integration_cwd
        root.to_s
      end

      def example_script *args
        examples_dir.join(*args)
      end

    end
  end
end

