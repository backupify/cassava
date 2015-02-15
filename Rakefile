require "bundler/gem_tasks"

task :setup do
  require 'bundler/setup'
  Bundler.require(:default, :development)
end


task :console => [:setup] do
  Pry.start
end
