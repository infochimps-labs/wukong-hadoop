require 'shellwords'
require_relative("runner/inputs_and_outputs")
require_relative("runner/map_logic")
require_relative("runner/reduce_logic")
require_relative("runner/local_invocation")
require_relative("runner/hadoop_invocation")
module Wukong
  module Hadoop

    # The <tt>Hadoop::Runner</tt> class contains the logic to examine
    # arguments and construct command lines which it will execute to
    # create the desired behavior.
    #
    # The Hadoop::Runner will introspect on its arguments to guess (if
    # not given) the processors to use as mapper and reducer in a
    # map/reduce job.  It will also decide whether to run that job in
    # local or Hadoop mode.  These decisions result in a command which
    # it will ultimately execute.
    class HadoopRunner < Wukong::Runner

      program 'wu-hadoop'

      usage "PROCESSOR|FLOW [PROCESSOR|FLOW]"
      
      description <<EOF
wu-hadoop is a tool to model and launch Wukong processors as
map/reduce workflows within the Hadoop framework.

Use wu-hadoop with existing processors in `local' mode to test the
logic of your job, reading from the specified --input and printing to
STDOUT:

  $ wu-hadoop examples/word_count.rb --mode=local --input=examples/sonnet_18.txt
  a	2
  all	1
  and	2
  ...

where it is assumed that your mapper is called 'mapper' and your
reducer 'reducer'.  You can also cat in data:

  $ cat examples/sonnet_18.txt | wu-hadoop examples/word_count.rb --mode=local

Or pass options directly:

  $ wu-hadoop examples/word_count.rb  --mode=local --input=examples/sonnet_18.txt --fold_case --min_length=3
  all	1
  and	5
  art	1
  brag	1
  ...

Or define both processors in separate files:

  $ wu-hadoop examples/tokenizer.rb examples/counter.rb --mode=local --input=examples/sonnet_18.txt

Or by name:

  $ wu-hadoop examples/processors.rb --mode=local --input=examples/sonnet_18.txt --mapper=tokenizer --reducer=counter

Or just by command:

$ wu-hadoop processors.rb --mapper=tokenizer --reduce_command='uniq -c' ...
$ wu-hadoop processors.rb --map_command='cut -f3' --reducer=counter ...
$ wu-hadoop --map_command='cut -f3' --reduce_command='uniq -c' ...

If you don't specify a --reducer explicitly, and you didn't give two
separate arguments, and no processor named :reducer exists in the
environment, then we assume you are launching a map-only job and
'mapred.tasktracker.reduce.tasks.maximum' will correspondingly be set
to 0:

  $ wu-hadoop examples/tokenizer.rb --mode=local --input=examples/sonnet_18.txt
  Shall
  I
  compare
  thee
  ...

You can achieve this directly with the --reduce_tasks=0 option.

Many other Hadoop options have been wrapped with similarly friendly
names below.  These are ignored when running in `local' mode.

Some options (like `--sort_command') only make sense in `local' mode.
These are ignored in `hadoop' mode.
EOF

      include Logging
      include InputsAndOutputs
      include MapLogic
      include ReduceLogic
      include HadoopInvocation
      include LocalInvocation

      # Parses the +args+ for this runner.
      #
      # Will exit if more than two arguments (mapper and reducer) are
      # passed.
      #
      # Will exit if no input or output arguments are provided and we
      # are in Hadoop mode.
      def validate
        raise Error.new("Cannot provide more than two arguments") if args.length > 2
        if mode == :hadoop && (input_paths.nil? || input_paths.empty? || output_path.nil? || output_path.empty?)
          raise Error.new("Explicit --input and --output paths are required to run a job in Hadoop mode.")
        end
        true
      end

      # Run this command.
      def run
        if mode == :local
          log.info "Launching local!"
          execute_command!(local_commandline)
        else
          remove_output_path! if settings[:rm] || settings[:overwrite]
          hadoop_commandline
          log.info "Launching Hadoop!"
          execute_command!(hadoop_commandline)
        end
      end

      # What mode is this runner in?
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
        return false unless path
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
        s = (loaded_deploy_pack? ? Deploy.pre_deploy_settings : settings)
        s.reject{ |param, val| s.definition_of(param, :wukong_hadoop) }.map{ |param,val| "--#{param}=#{Shellwords.escape(val.to_s)}" }.join(" ")
      end

      # Execute a command composed of the given parts.
      #
      # Will print the command instead of the <tt>--dry_run</tt>
      # option was given.
      #
      # @param [Array<String>] argv
      def execute_command!(*argv)
        command = argv.flatten.reject(&:blank?).join(" \\\n    ")
        if settings[:dry_run]
          log.info("Dry run:")
          puts command
        else
          puts `#{command}`
          raise Error.new("Command failed!") unless $?.success?
        end
      end
      
    end
  end
end
