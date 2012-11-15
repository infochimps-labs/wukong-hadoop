require_relative("local_invocation")
require_relative("hadoop_invocation")

module Wukong
  module Hadoop
    class Driver < Wukong::Driver

      include HadoopInvocation
      include LocalInvocation

      attr_accessor :settings

      def self.run(settings, *extra_args)
        begin
          new(settings, *extra_args).run!
        rescue Wukong::Error => e
          $stderr.puts e.message
          exit(127)
        end
      end
      
      def initialize(settings, *args)
        @settings         = settings
        self.mapper_file  = args.shift
        self.reducer_file = args.shift
      end

      def mode
        settings[:mode].to_s == 'local' ? :local : :hadoop
      end

      def run!
        if mode == :local
          Log.info "Launching local!"
          execute_command!(local_commandline)
        else
          ensure_input_and_output!
          overwrite! if settings[:rm] || settings[:overwrite]
          Log.info "Launching Hadoop!"
          execute_command!(hadoop_commandline)
        end
      end

      def reduce?
        settings[:reduce_command] || processor_defined?(reducer)
      end

      def processor_defined? proc
        Wukong.registry.registered?(proc)
      end

      def mapper_commandline
        settings[:map_command]    || "#{command_prefix} wu-local #{mapper_file} --run=#{mapper} " + non_wukong_params
      end
      
      def reducer_commandline
        return settings[:reduce_command] if settings[:reduce_command]
        if processor_defined?(reducer)
          "#{command_prefix} wu-local #{reducer_file} --run=#{reducer} " + non_wukong_params
        else
          ''
        end
      end

      def command_prefix
        settings[:command_prefix]
      end
      
      def mapper_file= path
        # raise Error.new("No such path: #{path}") unless File.exist?(path)
        @mapper_file = Pathname.new(path).realpath
      end

      def mapper_file
        @mapper_file
      end

      def reducer_file= path
        return unless path
        raise Error.new("No such path: #{path}") unless File.exist?(path)
        @reducer_file = Pathname.new(path).realpath
      end

      def reducer_file
        @reducer_file || @mapper_file
      end

      def mapper
        case
        when settings[:mapper]
          settings[:mapper]
        when given_explicit_mapper_and_reducer?
          File.basename(mapper_file, '.rb')
        else
          'mapper'
        end
      end

      def reducer
        case
        when settings[:reducer]
          settings[:reducer]
        when given_explicit_mapper_and_reducer?
          File.basename(reducer_file, '.rb')
        else
          'reducer'
        end
      end
      
      def non_wukong_params
        settings.reject{ |param, val| settings.definition_of(param, :wukong) }.map{ |param,val| "--#{param}=#{val}" }.join(" ")
      end
      
      def given_explicit_mapper_and_reducer?
        mapper_file && reducer_file
      end

      def input_paths
        @input_paths ||= (settings[:input] || [])
      end

      def output_path
        settings[:output]
      end

      def execute_command!(*args)
        command = args.flatten.reject(&:blank?).join(" \\\n    ")
        if settings[:dry_run]
          Log.info "Dry run:\n#{command}\n"
        else
          puts `#{command}`
          raise "Streaming command failed!" unless $?.success?
        end
      end
      
    end
  end
end
