require 'spec_helper'

describe Wukong::Hadoop::HadoopRunner do

  context "handling errors" do
    it "raises an error when it can't find a file" do
      expect { hadoop_runner(examples_dir('processors.rb'), examples_dir('doesnt_exist.rb'), :input => 'foo', :output => 'bar') }.to raise_error(Wukong::Error, /no such file/)
    end
    
    it "raises an error in Hadoop mode when called without input and output paths" do
      expect { hadoop_runner('regexp', 'count') }.to raise_error(Wukong::Error, /input.*output/)
    end
    
    it "raises an error when given more than two arguments" do
      expect { hadoop_runner('regexp', examples_dir('counter.rb'), 'extra', :input => 'foo', :output => 'bar') }.to raise_error(Wukong::Error, /two/)
    end
  end

  context "passing params to wu-local" do
    subject       { hadoop_runner('regexp', :clean => 'hi', :messy => 'hi "there"', :reduce_tasks => 0, :dry_run => true, :rm => true, :input => 'foo', :output => 'bar') }
    it "passes arguments it doesn't know about to wu-local" do
      subject.mapper_commandline.should include('--clean=hi')
    end
    it "correctly passes messy arguments" do
      subject.mapper_commandline.should include('--messy=hi\\ \\"there\\"')
    end
    it "does not pass arguments that are internal to wukong-hadoop" do
      subject.mapper_commandline.should_not include('--reduce_tasks', '--dry_run', '--rm')
    end
  end
  
  context "will execute a map-only job" do
    context "with an explicit map command" do
      subject                  { hadoop_runner(:map_command => 'cut -f 1', :input => 'foo', :output => 'bar') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^cut -f 1$/ }
    end
    context "with a single widget" do
      subject                  { hadoop_runner('regexp', :input => 'foo', :output => 'bar') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^wu-local regexp$/ }
    end
    context "with a single file" do
      context "defining a processor named 'mapper'" do
        subject                  { hadoop_runner(examples_dir('map_only.rb'), :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*map_only.rb --run=mapper$/ }
      end
      context "defining a processor named after the file" do
        subject                  { hadoop_runner(examples_dir('tokenizer.rb'), :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*tokenizer.rb$/ }
      end
      context "using the given --mapper option " do
        subject                  { hadoop_runner(examples_dir('processors.rb'), :mapper => 'tokenizer', :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*processors.rb --run=tokenizer$/ }
      end
      context "defining a processor named 'reducer' but with --reduce_tasks=0" do
        subject                  { hadoop_runner(examples_dir('word_count.rb'), :reduce_tasks => 0, :input => 'foo', :output => 'bar') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*word_count.rb --run=mapper$/ }
      end
    end
    context "with two files but with --reduce_tasks=0" do
      subject                   { hadoop_runner(examples_dir('tokenizer.rb'), examples_dir('counter.rb'), :reduce_tasks => 0, :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_false                          }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
    end
  end

  context "will execute a map-reduce job" do
    context "with explicit map and reduce commands" do
      subject                   { hadoop_runner(:map_command => 'cut -f 1', :reduce_command => 'uniq -c', :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_true       }
      its(:mapper_commandline)  { should == 'cut -f 1' }
      its(:reducer_commandline) { should == 'uniq -c'  }
    end
    context "with two widgets" do
      subject                   { hadoop_runner('regexp', 'count', :input => 'foo', :output => 'bar')        }
      its(:reduce?)             { should be_true                   }
      its(:mapper_commandline)  { should match /^wu-local regexp$/ }
      its(:reducer_commandline) { should match /^wu-local count$/  }
    end
    context "with a single file" do
      context "defining processors named 'mapper' and 'reducer'" do
        subject                   { hadoop_runner(examples_dir('word_count.rb'), :input => 'foo', :output => 'bar') }
        its(:reduce?)             { should be_true                   }
        its(:mapper_commandline)  { should match /^wu-local .*word_count.rb --run=mapper$/  }
        its(:reducer_commandline) { should match /^wu-local .*word_count.rb --run=reducer$/ }
      end
    end
    context "with two files" do
      subject                   { hadoop_runner(examples_dir('tokenizer.rb'), examples_dir('counter.rb'), :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
      its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
    end
    context "with a widget and a file" do
      subject                   { hadoop_runner('regexp', examples_dir('counter.rb'), :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local regexp$/         }
      its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
    end
    context "with a file and a widget" do
      subject                   { hadoop_runner(examples_dir('tokenizer.rb'), 'count', :input => 'foo', :output => 'bar') }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
      its(:reducer_commandline) { should match /^wu-local count$/          }
    end
  end

  context "given the --command_prefix option" do
    subject       { hadoop_runner('regexp', 'count', :command_prefix => 'bundle exec', :input => 'foo', :output => 'bar') }
    its(:mapper_commandline)  { should match(/^bundle exec wu-local/) }
    its(:reducer_commandline) { should match(/^bundle exec wu-local/) }
  end
  
end
