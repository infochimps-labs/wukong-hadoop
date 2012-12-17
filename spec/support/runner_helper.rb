module Wukong
  module Hadoop
    module RunnerHelper

      def runner *args, &block
        settings = Configliere::Param.new
        Wukong::Hadoop.configure(settings)
        Wukong::Hadoop.configure_for_program(settings, 'wu-hadoop')
        if args.last.is_a?(Hash)
          settings.merge!(args.pop)
        end
        ARGV.replace(args.map(&:to_s))
        r = Wukong::Hadoop::Runner.new(settings)
        yield r if block_given?
        r.boot
        r
      end
      
    end
  end
end

