require 'spec_helper'

describe Wukong::Hadoop::HadoopInvocation do
  
  let(:map_only)   { runner('regexp',          input: '/tmp/input1,/tmp/input2', output: '/tmp/output') }
  let(:map_reduce) { runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output') }
  let(:complex)    { runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output', map_tasks: '100', job_name: 'testy', java_opts: ['-D foo.bar=3 -D baz.booz=hello', '-D hi.there=bye'], :reduce_tasks => 20) }
  let(:custom_io)  { runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output', input_format: 'com.example.InputFormat', output_format: 'com.example.OutputFormat') }
  let(:many_files) { runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output', files: %w[/file/1 /file/2], archives: %w[/archive/1 /archive/2], jars: %w[/jar/1 /jar/2])}

  context "defining input paths" do
    it "raises an error unless given an --input option" do
      lambda { runner('regexp', output: '/tmp/output').run! }.should raise_error(Wukong::Error, /--input.*required/)
    end
    it "sets its input paths correctly" do
      map_reduce.hadoop_commandline.should match(%r{-input\s+'/tmp/input1,/tmp/input2'})
    end
    it "sets its input format given the --input_format option" do
      custom_io.hadoop_commandline.should match(%r{-inputformat\s+'com.example.InputFormat'})
    end
  end
  
  context "defining its output path" do
    it "raises an error unless given an --output option" do
      lambda { runner('regexp', input: '/tmp/output').run! }.should raise_error(Wukong::Error, /--output.*required/)
    end
    it "sets its output path correctly" do
      map_reduce.hadoop_commandline.should match(%r{-output\s+'/tmp/output'})
    end
    it "sets its output format given the --output_format option" do
      custom_io.hadoop_commandline.should match(%r{-outputformat\s+'com.example.OutputFormat'})
    end
  end

  context "defining its mapper and reducer" do
    it "sets its mapper correctly" do
      map_reduce.hadoop_commandline.should match(%r{-mapper\s+'wu-local regexp'})
    end
    it "sets its reducer correctly" do
      map_reduce.hadoop_commandline.should match(%r{-reducer\s+'wu-local count'})
    end
    it "uses a blank reducer for a map-only job" do
      map_only.hadoop_commandline.should match(%r{-reducer\s+''})
    end
  end

  context "setting the number of reduce tasks" do
    it "does nothing on a map/reduce job" do
      map_reduce.hadoop_commandline.should_not match(%r{-D mapred.reduce.tasks})
    end
    it "respects the option when given" do
      complex.hadoop_commandline.should  match(%r{-D mapred.reduce.tasks=20})
    end
    it "sets reduce tasks to 0 for a map-only job" do
      map_only.hadoop_commandline.should match(%r{-D mapred.reduce.tasks=0})
    end
  end

  context "defining Hadoop JobConf options" do
    it "translates friendly names into native ones" do
      complex.hadoop_commandline.should include("-D mapred.job.name='testy'")
      complex.hadoop_commandline.should include("-D mapred.map.tasks=100")
    end
    it "passes options in the given --java_opts option" do
      complex.hadoop_commandline.should include('-D foo.bar=3','-D baz.booz=hello','-D hi.there=bye')
    end
  end

  context "removing existing output paths" do
    
    it "will not remove the output path by default" do
      map_reduce.should_not_receive(:remove_output_path!)
      map_reduce.should_receive(:execute_command!)
      map_reduce.run!
    end
    it "will remove the output path when given the --rm option" do
      d = runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output', rm: true)
      d.should_receive(:remove_output_path!)
      d.should_receive(:execute_command!)
      d.run!
    end
    it "will not remove the output path when given the --rm option AND the --dry_run option" do
      d = runner('regexp', 'count', input: '/tmp/input1,/tmp/input2', output: '/tmp/output', rm: true, dry_run: true)
      d.should_receive(:remove_output_path!)
      d.should_receive(:execute_command!)
      d.run!
    end
  end

  context "handle files, jars, and archives" do
    it "does not include any files, jars, or archives when no files were passed" do
      map_reduce.hadoop_commandline.should_not match(%r{-(files|archives|libjars)})
    end
    it "should include files when asked" do
      many_files.hadoop_commandline.should match(%r{-files\s+'/file/1,/file/2'})
    end
    it "should include jars when asked" do
      many_files.hadoop_commandline.should match(%r{-libjars\s+'/jar/1,/jar/2'})
    end
    it "should include archives when asked" do
      many_files.hadoop_commandline.should match(%r{-archives\s+'/archive/1,/archive/2'})
    end
    it "should include files when passed files as arguments" do
      runner(examples_dir('tokenizer.rb'), examples_dir('counter.rb'), input: '/tmp/input1,/tmp/input2', output: '/tmp/output').hadoop_commandline.should match(%r{-files.+tokenizer\.rb,.*counter\.rb})
    end
  end
  
end
