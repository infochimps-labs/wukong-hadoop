require 'spec_helper'

describe Wukong::Hadoop::Driver do

  context "processing its arguments" do
    it "raises an error when it can't find a file" do
      lambda { driver(example_script('processors.rb'), example_script('doesnt_exist.rb')) }.should raise_error(Wukong::Error, /No such processor or file/)
    end
    it "raises an error when it can't find a widget" do
      lambda { driver('regexp', 'doesnt_exist') }.should raise_error(Wukong::Error, /No such processor or file/)
    end
    it "raises an error when given more than two arguments" do
      lambda { driver('regexp', example_script('counter.rb'), 'extra') }.should raise_error(Wukong::Error, /more than two/)
    end
  end

  context "will execute a map-only job" do
    context "with an explicit map command" do
      let(:subject)            { driver(:map_command => 'cut -f 1') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^cut -f 1$/ }
    end
    context "with a single widget" do
      let(:subject)            { driver('regexp') }
      its(:reduce?)            { should be_false }
      its(:mapper_commandline) { should match /^wu-local regexp$/ }
    end
    context "with a single file" do
      context "defining a processor named 'mapper'" do
        let(:subject)            { driver(example_script('map_only.rb')) }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*map_only.rb --run=mapper$/ }
      end
      context "defining a processor named after the file" do
        let(:subject)            { driver(example_script('tokenizer.rb')) }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*tokenizer.rb$/ }
      end
      context "using the given --mapper option " do
        let(:subject)            { driver(example_script('processors.rb'), :mapper => 'tokenizer') }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*processors.rb --run=tokenizer$/ }
      end
      context "defining a processor named 'reducer' but with --reduce_tasks=0" do
        let(:subject)            { driver(example_script('word_count.rb'), :reduce_tasks => 0) }
        its(:reduce?)            { should be_false }
        its(:mapper_commandline) { should match /^wu-local .*word_count.rb --run=mapper$/ }
      end
    end
    context "with two files but with --reduce_tasks=0" do
      let(:subject)             { driver(example_script('tokenizer.rb'), example_script('counter.rb'), :reduce_tasks => 0) }
      its(:reduce?)             { should be_false                          }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
    end
  end

  context "will execute a map-reduce job" do
    context "with explicit map and reduce commands" do
      let(:subject)             { driver(:map_command => 'cut -f 1', :reduce_command => 'uniq -c') }
      its(:reduce?)             { should be_true       }
      its(:mapper_commandline)  { should == 'cut -f 1' }
      its(:reducer_commandline) { should == 'uniq -c'  }
    end
    context "with two widgets" do
      let(:subject)             { driver('regexp', 'count')        }
      its(:reduce?)             { should be_true                   }
      its(:mapper_commandline)  { should match /^wu-local regexp$/ }
      its(:reducer_commandline) { should match /^wu-local count$/  }
    end
    context "with a single file" do
      context "defining processors named 'mapper' and 'reducer'" do
        let(:subject)             { driver(example_script('word_count.rb')) }
        its(:reduce?)             { should be_true                   }
        its(:mapper_commandline)  { should match /^wu-local .*word_count.rb --run=mapper$/  }
        its(:reducer_commandline) { should match /^wu-local .*word_count.rb --run=reducer$/ }
      end
    end
    context "with two files" do
      let(:subject)             { driver(example_script('tokenizer.rb'), example_script('counter.rb')) }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
      its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
    end
    context "with a widget and a file" do
      let(:subject)             { driver('regexp', example_script('counter.rb')) }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local regexp$/         }
      its(:reducer_commandline) { should match /^wu-local .*counter.rb$/   }
    end
    context "with a file and a widget" do
      let(:subject)             { driver(example_script('tokenizer.rb'), 'count') }
      its(:reduce?)             { should be_true                           }
      its(:mapper_commandline)  { should match /^wu-local .*tokenizer.rb$/ }
      its(:reducer_commandline) { should match /^wu-local count$/          }
    end
  end

  context "handling arguments" do
    let(:subject) { driver('regexp', :clean => 'hi', :messy => 'hi "there"', :reduce_tasks => 0, :dry_run => true, :rm => true) }
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

  context "given the --command_prefix option" do
    let(:subject) { driver('regexp', 'count', :command_prefix => 'bundle exec') }
    its(:mapper_commandline)  { should match(/^bundle exec wu-local/) }
    its(:reducer_commandline) { should match(/^bundle exec wu-local/) }
  end
  
end
