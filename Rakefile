require "bundler/gem_tasks"
require 'rake'
require 'rake/testtask'

Rake::TestTask.new do |t|
  t.pattern = 'test/**/*_test.rb'
  t.libs.push 'test'
end

task :setup do
  require 'bundler/setup'
  Bundler.require(:default, :development)
end


task :console => [:setup] do
  Pry.start
end
