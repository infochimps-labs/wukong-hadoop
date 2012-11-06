Given /^a wukong script "(.*?)"$/ do |wu_file|
  Pathname(wu_file).should exist
  write_file(wu_file, File.read(wu_file))
end
