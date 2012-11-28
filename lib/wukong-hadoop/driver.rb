require 'shellwords'
require_relative("driver/map_logic")
require_relative("driver/reduce_logic")
require_relative("driver/local_invocation")
require_relative("driver/hadoop_invocation")


module Wukong
  module Hadoop

    # The <tt>Hadoop::Driver</tt> class contains the logic to examine
    # arguments and construct command lines which it will execute to
    # create the desired behavior.
    #
    # The Hadoop::Driver will introspect on its arguments to guess (if
    # not given) the processors to use as mapper and reducer in a
    # map/reduce job.  It will also decide whether to run that job in
    # local or Hadoop mode.  These decisions result in a command which
    # it will ultimately execute.
    class Driver < Wukong::Driver

      include MapLogic
      include ReduceLogic
      include HadoopInvocation
      include LocalInvocation

      # The settings used by this driver.
      #
      # @param [Configliere::Param]
      attr_accessor :settings

      # The (processed) arguments for this driver.
      #
      # @param [Array<String, Pathname>]
      attr_reader   :args

      # Initialize and run a new Wukong::Hadoop::Driver for the given
      # +settings+.
      #
      # Will rescue all Wukong::Error exceptions by printing a nice
      # message to STDERR and exiting.
      # 
      # @param [Configliere::Param] settings
      # @param [Array<String>] extra_args
      def self.run(settings, *extra_args)
        begin
          new(settings, *extra_args).run!
        rescue Wukong::Error => e
          $stderr.puts e.message
          exit(127)
        end
      end

      # Run this driver.
      def run!
        if mode == :local
          # Log.info "Launching local!"
          execute_command!(local_commandline)
        else
          ensure_input_and_output!
          remove_output_path! if settings[:rm] || settings[:overwrite]
          Log.info "Launching Hadoop!"
          execute_command!(hadoop_commandline)
        end
      end

      # Initialize a new driver with the given +settings+ and +args+.
      #
      # @param [Configliere::Param] settings
      # @param [Array<String>] args
      def initialize(settings, *args)
        @settings = settings
        self.args = args
      end

      # Set the +args+ for this driver.
      #
      # Arguments can be either (registered) processor names or files.
      #
      # An error will be raised on missing files or those which
      # couldn't be loaded.
      #
      # An error will be raised if more than two arguments (mapper and
      # reducer) are passed.
      #
      # @param [Array<String>] args
      def args= args
        raise Error.new("Cannot provide more than two arguments") if args.length > 2
        @args = args.map do |arg|
          if processor_registered?(arg)
            arg
          else
            begin
              rp = Pathname.new(arg).realpath
              load rp
              rp
            rescue => e
              raise Error.new("No such processor or file: #{arg}")
            end
          end
        end
      end

      # What mode is this driver in?
      #
      # @return [:hadoop, :local]
      def mode
        settings[:mode].to_s == 'local' ? :local : :hadoop
      end

      # Were mapper and/or reducer named by a single argument?
      #
      # @return [true, false]
      def single_job_arg?
        args.size == 1
      end

      # Were mapper and/or reducer named by separate arguments?
      #
      # @return [true, false]
      def separate_map_and_reduce_args?
        args.size == 2
      end

      # Is there a processor registered with the given +name+?
      #
      # @param [#to_s] name
      # @return [true, false]
      def processor_registered? name
        Wukong.registry.registered?(name.to_s.to_sym)
      end

      # Return the guessed name of a processor at the given +path+.
      #
      # @param [String] path
      # @return [String]
      def processor_name_from_file(path)
        File.basename(path, '.rb')
      end

      # Does the given +path+ contain a processor named after itself?
      #
      # @param [String] path
      # @return [true, false]
      def file_is_processor?(path)
        processor_registered?(processor_name_from_file(path))
      end

      # The prefix to insert befor all invocations of the
      # <tt>wu-local</tt> runner.
      #
      # @return [String]
      def command_prefix
        settings[:command_prefix]
      end

      # Returns parameters to pass to an invocation of
      # <tt>wu-local</tt>.
      #
      # Parameters like <tt>--reduce_tasks</tt> which are relevant to
      # Wukong-Hadoop will be interpreted and *not* passed.  Others
      # will be passed unmodified.
      #
      # @return [String]
      def params_to_pass
        s = (Wukong.loaded_deploy_pack? ? Deploy.pre_deploy_settings : settings)
        s.reject{ |param, val| s.definition_of(param, :wukong_hadoop) }.map{ |param,val| "--#{param}=#{Shellwords.escape(val.to_s)}" }.join(" ")
      end

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

      # Execute a command composed of the given parts.
      #
      # Will print the command instead of the <tt>--dry_run</tt>
      # option was given.
      #
      # @param [Array<String>] args
      def execute_command!(*args)
        command = args.flatten.reject(&:blank?).join(" \\\n    ")
        if settings[:dry_run]
          Log.info("Dry run:")
          puts command
        else
          puts `#{command}`
          raise "Streaming command failed!" unless $?.success?
        end
      end
      
    end
  end
end
