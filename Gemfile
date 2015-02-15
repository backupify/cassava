source 'https://rubygems.org'

# Specify your gem's dependencies in cassava.gemspec
gemspec

group :development, :test do
  gem "pry"
  gem "awesome_print"
  gem 'm', :git => 'git@github.com:ANorwell/m.git', :branch => 'minitest_5'
end

group :test do
  gem 'minitest_should', :git => 'git@github.com:citrus/minitest_should.git'
  gem "mocha"
end
