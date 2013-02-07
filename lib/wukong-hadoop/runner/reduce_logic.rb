module Wukong
  module Hadoop

    # Implements logic for figuring out the correct reducer
    # commandline given wu-hadoop's arguments and whether or not to
    # run a map-only (no-reduce) job.
    module ReduceLogic

      # Return the actual commandline used by the reducer, whether
      # running in local or Hadoop mode.
      #
      # You should be able to copy, paste, and run this command
      # unmodified to debug the reducer.
      #
      # @return [String]
      def reducer_commandline
        return ''                        unless reduce?
        return settings[:reduce_command] if     explicit_reduce_command?
        arg = (mode == :hadoop ? File.basename(reducer_arg) : reducer_arg)
        [command_prefix, 'wu-local', arg].tap do |cmd|
          cmd << "--run=#{reducer_name}" if reducer_needs_run_arg?
          cmd << non_wukong_hadoop_params_string
        end.compact.map(&:to_s).reject(&:empty?).join(' ')
      end

      # Were we given an explicit reduce command (like 'uniq -c') or
      # are we to introspect and construct the command?
      #
      # @return [true, false]
      def explicit_reduce_command?
        settings[:reduce_command]
      end

      # Were we given a processor to use as our reducer explicitly by
      # name or are we to introspect to discover the correct
      # processor?
      #
      # @return [true, false]
      def explicit_reduce_processor?
        settings[:reducer]
      end

      # Were we given an explicit reducer (either as a command or as a
      # processor) or should we introspect to find one?
      #
      # @return [true, false]
      def explicit_reducer?
        explicit_reduce_processor? || explicit_reduce_command?
      end

      # The argument that we should introspect on to turn into our
      # reducer.
      #
      # @return [String]
      def reducer_arg
        args.last
      end

      # Should we perform a reduce or is this a map-only job?
      #
      # We will definitely reduce if
      #
      #   - given an explicit <tt>--reduce_command</tt>
      #   - we discovered a reducer
      #
      # We will not reduce if:
      #
      #   - <tt>--reduce_tasks</tt> was explicitly set to 0
      #
      # @return [true, false]
      def reduce?
        return false if settings[:reduce_tasks] && settings[:reduce_tasks].to_i == 0
        return true  if settings[:reduce_command]
        return true  if reducer_name
        false
      end

      # Is this a map-only job?
      #
      # @see #reduce?
      #
      # @return [true, false]
      def map_only?
        (! reduce?)
      end

      # Does the reducer commandline need an explicit --run argument?
      #
      # Will not be used if the processor name is the same as the name
      # of the script.
      #
      # @return [true, false]
      def reducer_needs_run_arg?
        return false if reducer_arg.to_s == reducer_name.to_s
        return false if File.basename(reducer_arg.to_s, '.rb') == reducer_name
        true
      end

      # Return the name of the processor to use as the reducer.
      #
      # Will raise a <tt>Wukong::Error</tt> if a given reducer is
      # invalid.  Will return nil if no reducer can be guessed.
      #
      # Most of the logic that examines explicit command line
      # arguments and checks for the existence of named processors or
      # files is here.
      #
      # @return [String]
      def reducer_name
        case
        when explicit_reducer?
          if processor_registered?(settings[:reducer])
            settings[:reducer]
          else
            raise Error.new("No such processor: '#{settings[:reducer]}'")
          end
        when single_job_arg? && explicit_mapper? && processor_registered?(reducer_arg)
          reducer_arg
        when separate_map_and_reduce_args? && processor_registered?(reducer_arg)
          reducer_arg
        when separate_map_and_reduce_args? && file_is_processor?(reducer_arg)
          processor_name_from_file(reducer_arg)
        when processor_registered?('reducer')
          'reducer'
        end
      end
      
    end
  end
end
