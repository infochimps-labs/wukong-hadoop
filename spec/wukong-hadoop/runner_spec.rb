require 'spec_helper'

describe Wukong::Hadoop::Runner do

  context "processing its arguments" do
    it "raises an error when it can't find a file" do
      expect { runner(example_script('processors.rb'), example_script('doesnt_exist.rb')) }.to raise_error(Wukong::Error, /no such file/)
    end
    
    it "raises an error when it can't find a processor given by name" do
      pending
      expect { runner('regexp', 'doesnt_exist') }.to raise_error(Wukong::Error, /no such processor/)
    end

    it "raises an error in Hadoop mode when called without input and output paths" do
      expect { runner('regexp', 'count') }.to raise_error(Wukong::Error, /input.*output/)
    end
    
    it "raises an error when given more than two arguments" do
      expect { runner('regexp', example_script('counter.rb'), 'extra') }.to raise_error(Wukong::Error, /two/)
    end
  end

  context "will execute a map-only job" do
    context "with an explicit map command" do
      subject                  { runner(:map_command => 'cut -f 1', :input => 'foo', :output => 'bar') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^cut -f 1$/ }
    end
    context "with a single widget" do
      subject                  { runner('regexp', :input => 'foo', :output => 'bar') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^wu-local regexp$/ }
    end
    context "with a single file" do
      context "defining a processor named 'mapper'" do
        subject                  { runner(example_script('map_only.rb'), :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*map_only.rb --run=mapper$/ }
      end
      context "defining a processor named after the file" do
        subject                  { runner(example_script('tokenizer.rb'), :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*tokenizer.rb$/ }
      end
      context "using the given --mapper option " do
        subject                  { runner(example_script('processors.rb'), :mapper => 'tokenizer', :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*processors.rb --run=tokenizer$/ }
      end
      context "defining a processor named 'reducer' but with --reduce_tasks=0" do
        subject                  { runner(example_script('word_count.rb'), :reduce_tasks => 0, :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*word_count.rb --run=mapper$/ }
      end
    end
    context "with two files but with --reduce_tasks=0" do
      subject                   { runner(example_script('tokenizer.rb'), example_script('counter.rb'), :reduce_tasks => 0, :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_false                          }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
    end
  end

  # # context "will execute a map-reduce job" do
  # #   context "with explicit map and reduce commands" do
  # #     subject                   { runner(:map_command => 'cut -f 1', :reduce_command => 'uniq -c') }
  # #     its(:reduce?)             { should be_true       }
  # #     its(:mapper_commandline)  { should == 'cut -f 1' }
  # #     its(:reducer_commandline) { should == 'uniq -c'  }
  # #   end
  # #   context "with two widgets" do
  # #     subject                   { runner('regexp', 'count')        }
  # #     its(:reduce?)             { should be_true                   }
  # #     its(:mapper_commandline)  { should match /^wu-local regexp$/ }
  # #     its(:reducer_commandline) { should match /^wu-local count$/  }
  # #   end
  # #   context "with a single file" do
  # #     context "defining processors named 'mapper' and 'reducer'" do
  # #       subject                   { runner(example_script('word_count.rb')) }
  # #       its(:reduce?)             { should be_true                   }
  # #       its(:mapper_commandline)  { should match /^wu-local .*word_count.rb --run=mapper$/  }
  # #       its(:reducer_commandline) { should match /^wu-local .*word_count.rb --run=reducer$/ }
  # #     end
  # #   end
  # #   context "with two files" do
  # #     subject                   { runner(example_script('tokenizer.rb'), example_script('counter.rb')) }
  # #     its(:reduce?)             { should be_true                           }
  # #     its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
  # #     its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
  # #   end
  # #   context "with a widget and a file" do
  # #     subject                   { runner('regexp', example_script('counter.rb')) }
  # #     its(:reduce?)             { should be_true                           }
  # #     its(:mapper_commandline)  { should match /^wu-local regexp$/         }
  # #     its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
  # #   end
  # #   context "with a file and a widget" do
  # #     subject                   { runner(example_script('tokenizer.rb'), 'count') }
  # #     its(:reduce?)             { should be_true                           }
  # #     its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
  # #     its(:reducer_commandline) { should match /^wu-local count$/          }
  # #   end
  # # end

  # # context "handling arguments" do
  # #   subject       { runner('regexp', :clean => 'hi', :messy => 'hi "there"', :reduce_tasks => 0, :dry_run => true, :rm => true) }
  # #   it "passes arguments it doesn't know about to wu-local" do
  # #     subject.mapper_commandline.should include('--clean=hi')
  # #   end
  # #   it "correctly passes messy arguments" do
  # #     subject.mapper_commandline.should include('--messy=hi\\ \\"there\\"')
  # #   end
  # #   it "does not pass arguments that are internal to wukong-hadoop" do
  # #     subject.mapper_commandline.should_not include('--reduce_tasks', '--dry_run', '--rm')
  # #   end
  # # end

  # # context "given the --command_prefix option" do
  # #   subject       { runner('regexp', 'count', :command_prefix => 'bundle exec') }
  # #   its(:mapper_commandline)  { should match(/^bundle exec wu-local/) }
  # #   its(:reducer_commandline) { should match(/^bundle exec wu-local/) }
  # # end
  
end
