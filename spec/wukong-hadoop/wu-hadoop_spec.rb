require 'spec_helper'

describe 'wu-hadoop' do
  context "without any arguments" do
    let(:subject) { command('wu-hadoop') }
    it {should exit_with(:non_zero) }
    it "displays help on STDERR" do
      should have_stderr(/--input.*--output/)
    end
  end

  context "in local mode" do
    context "on a map-only job" do
      let(:subject) { command('wu-hadoop', examples_dir('tokenizer.rb'), "--mode=local", "--input=#{examples_dir('sonnet_18.txt')}") }
      it { should exit_with(0) }
      it { should have_stdout('Shall', 'I', 'compare', 'thee', 'to', 'a', "summer's", 'day') }
    end
    
    context "on a map-reduce job" do
      let(:subject) { command('wu-hadoop', examples_dir('word_count.rb'), "--mode=local", "--input=#{examples_dir('sonnet_18.txt')}") }
      it { should exit_with(0) }
      it { should have_stdout(/complexion\s+1/, /Death\s+1/, /temperate\s+1/) }
    end
  end

  context "in Hadoop mode" do
    context "on a map-only job" do
      let(:subject) { command('wu-hadoop', examples_dir('tokenizer.rb'), "--mode=hadoop", "--input=/data/in", "--output=/data/out", "--dry_run") }
      it { should exit_with(0) }
      it { should have_stdout(%r{jar.*hadoop.*streaming.*\.jar}, %r{-mapper.+tokenizer\.rb}, %r{-input.*/data/in}, %r{-output.*/data/out}) }
    end
  end
  
end
