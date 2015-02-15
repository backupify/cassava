require 'rubygems'
require 'pry'
require 'minitest/autorun'
require 'minitest/should'


require 'cassava'

class Minitest::Should::TestCase
  def self.xshould(*args)
    puts "Disabled test: #{args}"
  end
end

def session_for_keyspace(keyspace = 'test_cassava')
  c = Cassandra.cluster(port: 9242)
  c.connect(keyspace)
end

def initialize_test_table
  sess = session_for_keyspace(nil)
  sess.execute('DROP KEYSPACE test_cassava') rescue nil
  sess.execute("CREATE KEYSPACE test_cassava with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  session_for_keyspace.execute('CREATE TABLE test(id text, a int, b text, c text, d int, primary key ((id), a, b))')
end
