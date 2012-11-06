require 'bundler' ; Bundler.setup(:default, :development)

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:specs)

require 'cucumber/rake/task'
Cucumber::Rake::Task.new(:features)

task :default => [:specs, :features]
