# -*- encoding: utf-8 -*-
require File.expand_path('../lib/wukong-hadoop/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name        = 'wukong-hadoop'
  gem.homepage    = 'https://github.com/infochimps-labs/wukong-hadoop'
  gem.licenses    = ["Apache 2.0"]
  gem.email       = 'coders@infochimps.org'
  gem.authors     = ['Infochimps', 'Philip (flip) Kromer', 'Travis Dempsey']
  gem.version     = Wukong::Hadoop::VERSION

  gem.summary     = 'Hadoop Streaming for Ruby. Wukong makes Hadoop so easy a chimpanzee can use it, yet handles terabyte-scale computation with ease.'
  gem.description = <<-EOF
  Treat your dataset like a:

      * stream of lines when it's efficient to process by lines
      * stream of field arrays when it's efficient to deal directly with fields
      * stream of lightweight objects when it's efficient to deal with objects

  Wukong is friends with Hadoop the elephant, Pig the query language, and the cat on your command line.
EOF

  gem.files         = `git ls-files`.split("\n")
  gem.executables   = gem.files.grep(/^bin/).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(/^spec/)
  gem.require_paths = ['lib']

  gem.add_dependency('configliere', '~> 0.4')
  # gem.add_dependency('wukong',      '~> 3')

  gem.add_development_dependency 'rake',     '~> 0.9'
  gem.add_development_dependency 'rspec',    '~> 2'
  gem.add_development_dependency 'cucumber', '~> 1.2'
  gem.add_development_dependency 'aruba',    '~> 0.4'
  
end
