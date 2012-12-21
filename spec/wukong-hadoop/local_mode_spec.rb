require 'spec_helper'

describe Wukong::Hadoop::LocalInvocation do
  it "reads from STDIN and writes to STDOUT by default" do
    hadoop_runner('regexp', :mode => :local).local_commandline.should == 'wu-local regexp'
  end
  it "reads from from multiple input paths given the --input option" do
    hadoop_runner('regexp', :input => '/some/file.tsv,something_else.dat', :mode => :local).local_commandline.should == 'cat /some/file.tsv something_else.dat | wu-local regexp'
  end
  it "writes to a file given the --output option" do
    hadoop_runner('regexp', :mode => :local, :output => '/tmp/output.json').local_commandline.should == 'wu-local regexp > /tmp/output.json'
  end
  it "will not perform a sort on a map-only job" do
    hadoop_runner('regexp', :mode => :local).local_commandline.should_not include('sort')
  end
  it "will perform a sort on a map-reduce job" do
    hadoop_runner('regexp', 'count', :mode => :local).local_commandline.should == 'wu-local regexp | sort | wu-local count'
  end
  it "will accept a custom sort command" do
    hadoop_runner('regexp', 'count', :mode => :local, :sort_command => 'sort -n').local_commandline.should == 'wu-local regexp | sort -n | wu-local count'
  end
end
