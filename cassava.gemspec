# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'cassava/version'

Gem::Specification.new do |spec|
  spec.name          = "cassava"
  spec.version       = Cassava::VERSION
  spec.authors       = ["Arron Norwell"]
  spec.email         = ["anorwell@datto.com"]
  spec.summary       = %q{An unopinionated wrapper for the datastax cassandra gem.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "minitest-reporters"

  spec.add_dependency 'cassandra-driver'
  spec.add_dependency 'cequel'
end
