Gem::Specification.new do |s|
  s.name        = 'rubyrep'
  s.version     = '2.0.0'
  s.licenses    = ['MIT']
  s.summary     = 'Asynchronous master-master replication of relational databases.'
  s.description = 'Asynchronous master-master replication of relational databases.'
  s.authors     = ['Arndt Lehmann', 'Kevin Klein']
  s.email       = 'mail@arndtlehman.com'
  s.files       = Dir['Rakefile', '{bin,lib,config,sims,spec,tasks}/**/*', 'README*', 'LICENSE*', 'HISTORY*'] & `git ls-files -z`.split("\0")
  s.homepage    = 'http://rubyrep.org'
end
