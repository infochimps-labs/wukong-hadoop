`Feature: Run wu-hadoop from the command line
  In order to execute hadoop streaming commands
  As a user of wu-hadoop
  I should be able run wu-hadoop with wukong processors

  Scenario: Simple wu-hadoop command
    Given a wukong script "examples/word_count.rb"
    When  I run `bundle exec wu-hadoop examples/word_count.rb --dry_run --input=/foo --output=/bar `
    Then  the output should contain:
    """
    /usr/lib/hadoop/bin/hadoop 	\
      jar /usr/lib/hadoop/contrib/streaming/hadoop-*streaming*.jar 	\
      -D mapred.job.name='word_count.rb---/foo---/bar' 	\
    """
    And the output should match:
    """
      -mapper  '.*ruby bundle exec wu-local .*word_count.rb --run=mapper ' 	\\
      -reducer '.*ruby bundle exec wu-local .*word_count.rb --run=reducer ' 	\\
    """
    And the output should contain:
    """
      -input   '/foo' 	\
      -output  '/bar' 	\
    """
    And the output should match:
    """
      -file    '.*word_count.rb' 	\\
      -cmdenv  'BUNDLE_GEMFILE=.*wukong-hadoop/Gemfile'
    """

  Scenario: A wu-hadoop command without an input or output
    Given a wukong script "examples/word_count.rb"
    When  I run `bundle exec wu-hadoop examples/word_count.rb --dry_run`
    Then  the output should contain:
    """
    Missing values for: input (Comma-separated list of input paths.), output (Output directory for the hdfs.)
    """

  Scenario: Specifying an alternative gemfile
    Given a wukong script "examples/word_count.rb"
    When  I run `bundle exec wu-hadoop examples/word_count.rb --dry_run --input=/foo --output=/bar --gemfile=alt/Gemfile`
    Then  the output should contain:
    """
    -cmdenv  'BUNDLE_GEMFILE=alt/Gemfile'
    """
    
  Scenario: Skipping the reduce step
    Given a file named "wukong_script.rb" with:
    """
    Wukong.processor(:mapper) do

    end
    """
    When  I run `bundle exec wu-hadoop wukong_script.rb --dry_run --input=/foo --output=/bar`
    Then  the output should contain:
    """
      -D mapred.reduce.tasks=0 	\
    """

  Scenario: A processor without a mapper
    Given a file named "wukong_script.rb" with:
    """
    Wukong.processor(:reducer) do

    end
    """
    When  I run `bundle exec wu-hadoop wukong_script.rb --dry_run --input=/foo --output=/bar`
    Then  the output should match:
    """
    No :mapper definition found in .*wukong_script.rb
    """

  Scenario: Translating hadoop jobconf options
    Given a wukong script "examples/word_count.rb"
    When  I run `bundle exec wu-hadoop examples/word_count.rb --dry_run --input=/foo --output=/bar --max_tracker_failures=12`
    Then  the output should match:
    """
      -D mapred.max.tracker.failures=12 	\\
    """

  Scenario: Passing along extra configuration options
    Given a wukong script "examples/word_count.rb"
    When I run `bundle exec wu-hadoop examples/word_count.rb --dry_run --input=/foo --output=/bar --foo=bar`
    Then the output should match:
    """
      -mapper  '.* --foo=bar' 	\\
      -reducer '.* --foo=bar' 	\\
    """
