Feature: Run wu-hadoop from the command line
  In order to execute hadoop streaming commands
  As a user of wu-hadoop
  I should be able run wu-hadoop with wukong processors

  Scenario: Simple hadoop command
    Given a file named "simple_processor.rb" with:
    """
    Wukong.processor(:simple) do
    
      def process(record)
        yield record.reverse
      end

    end
    """
    When I run `bundle exec wu-hadoop simple_processor.rb --dry_run`
    Then the output should contain:
    """
    /usr/lib/hadoop/bin/hadoop 
    jar /usr/lib/hadoop/contrib/streaming/hadoop-*streaming*.jar    
    """
          
