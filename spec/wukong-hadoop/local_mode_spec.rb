require 'spec_helper'

describe Wukong::Hadoop::LocalInvocation do
  it "reads from STDIN and writes to STDOUT by default" do
    runner('regexp').local_commandline.should == 'wu-local regexp'
  end
  it "reads from from multiple input paths given the --input option" do
    runner('regexp', :input => '/some/file.tsv,something_else.dat').local_commandline.should == 'cat /some/file.tsv something_else.dat | wu-local regexp'
  end
  it "writes to a file given the --output option" do
    runner('regexp', :output => '/tmp/output.json').local_commandline.should == 'wu-local regexp > /tmp/output.json'
  end
  it "will not perform a sort on a map-only job" do
    runner('regexp').local_commandline.should_not include('sort')
  end
  it "will perform a sort on a map-reduce job" do
    runner('regexp', 'count').local_commandline.should == 'wu-local regexp | sort | wu-local count'
  end
  it "will accept a custom sort command" do
    runner('regexp', 'count', :sort_command => 'sort -n').local_commandline.should == 'wu-local regexp | sort -n | wu-local count'
  end
end
