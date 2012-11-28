require 'bundler'
Bundler::GemHelper.install_tasks

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:specs)

require 'yard'
YARD::Rake::YardocTask.new

require 'cucumber/rake/task'
Cucumber::Rake::Task.new(:features)

task :default => [:specs]
