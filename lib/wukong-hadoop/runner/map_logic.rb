module Wukong
  module Hadoop

    # Implements logic for figuring out the correct mapper commandline
    # given wu-hadoop's arguments.
    module MapLogic

      # Return the actual commandline used by the mapper, whether
      # running in local or Hadoop mode.
      #
      # You should be able to copy, paste, and run this command
      # unmodified to debug the mapper.
      #
      # @return [String]
      def mapper_commandline
        return settings[:map_command] if explicit_map_command?
        [command_prefix, 'wu-local',  mapper_arg].tap do |cmd|
          cmd << "--run=#{mapper_name}" if mapper_needs_run_arg?
          cmd << params_to_pass
        end.compact.map(&:to_s).reject(&:empty?).join(' ')
      end

      # Were we given an explicit map command (like 'cut -f 1') or are
      # we to introspect and construct the command?
      #
      # @return [true, false]
      def explicit_map_command?
        settings[:map_command]
      end

      # Were we given a processor to use as our mapper explicitly by
      # name or are we to introspect to discover the correct
      # processor?
      #
      # @return [true, false]
      def explicit_map_processor?
        settings[:mapper]
      end

      # Were we given an explicit mapper (either as a command or as a
      # processor) or should we introspect to find one?
      #
      # @return [true, false]
      def explicit_mapper?
        explicit_map_processor? || explicit_map_command?
      end

      # The argument that we should introspect on to turn into our
      # mapper.
      #
      # @return [String]
      def mapper_arg
        args.first
      end

      # Does the mapper commandline need an explicit --run argument?
      #
      # Will not be used if the processor name is the same as the name
      # of the script.
      #
      # @return [true, false]
      def mapper_needs_run_arg?
        return false if settings[:map_command]
        return false if mapper_arg.to_s == mapper_name.to_s
        return false if File.basename(mapper_arg.to_s, '.rb') == mapper_name.to_s
        true
      end

      # Return the name of the processor to use as the mapper.
      #
      # Will raise a <tt>Wukong::Error</tt> if a given mapper is
      # invalid or if none can be guessed.
      #
      # Most of the logic that examines explicit command line
      # arguments and checks for the existence of named processors or
      # files is here.
      #
      # @return [String]
      def mapper_name
        case
        when explicit_mapper?
          if processor_registered?(settings[:mapper])
            settings[:mapper]
          else
            raise Error.new("No such processor: '#{settings[:mapper]}'")
          end
        when map_only? && processor_registered?(mapper_arg)
          mapper_arg
        when map_only? && file_is_processor?(mapper_arg)
          processor_name_from_file(mapper_arg)
        when single_job_arg? && explicit_reducer? && processor_registered?(mapper_arg)
          mapper_arg
        when separate_map_and_reduce_args? && processor_registered?(mapper_arg)
          mapper_arg
        when separate_map_and_reduce_args? && file_is_processor?(mapper_arg)
          processor_name_from_file(mapper_arg)
        when processor_registered?('mapper')
          'mapper'
        else
          raise Error.new("Could not find a processor to use as a mapper")
        end
      end
    end
  end
end
