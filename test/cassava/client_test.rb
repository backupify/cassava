require_relative '../test_helper'

module Cassava
  class ClientTest < Minitest::Should::TestCase
    setup do
      initialize_test_table
      @session = session_for_keyspace
      @client = Cassava::Client.new(@session)
    end

    def string_keys(hash)
      Hash[hash.map { |k, v| [k.to_s, v] }]
    end

    context 'insert' do
      should 'be able to insert an item' do
        item = { :id => 'i', :a => 1, :b => 'b', :c => 'c', :d => 1}
        @client.insert(:test, item)
        assert_equal string_keys(item), @client.select(:test).execute.first
      end

      should 'insert an item with some fields missing' do
        item = { :id => 'i', :a => 1, :b => 'b', :c => 'c'}
        expected = item.merge(:d => nil)
        @client.insert(:test, item)
        assert_equal string_keys(expected), @client.select(:test).execute.first
      end

      should 'raise an error if the row key is missing' do
        item = { :a => 1, :b => 'b', :c => 'c'}
        assert_raises(Cassandra::Errors::InvalidError) { @client.insert(:test, item) }
      end

      should 'raise an error if a primary key part is missing' do
        item = { :id => 'i', :a => 1, :c => 'c'}
        assert_raises(Cassandra::Errors::InvalidError) { @client.insert(:test, item) }
      end

      should 'allow the insertion of columns with mismatched quotes' do
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1}
        @client.insert(:test, item)
        assert_equal string_keys(item), @client.select(:test).execute.first
      end

      should 'allow the insertion with a ttl' do
        ttl = 12345
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :ttl => ttl }
        @client.insert(:test, item)

        assert @client.send(:insert_statement, :test, item, ttl).cql =~ /\sUSING\sTTL\s#{ttl}/
        assert_equal string_keys(item), @client.select(:test).execute.first
      end

      should 'allow the insertion with a timestamp' do
        timestamp = Time.now.to_i * 1000000 + Time.now.usec
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :optional_timestamp => timestamp }
        @client.insert(:test, item)

        assert @client.send(:insert_statement, :test, item, nil, timestamp).cql =~ /\sUSING\sTIMESTAMP\s#{timestamp}/
        saved_timestamp = @client.select_writetime(:test, :d, { :id => 'i' })
        assert_equal timestamp, saved_timestamp
      end

      should 'allow the insertion of a ttl and a timestamp' do
        ttl = 12345
        timestamp = Time.now.to_i * 1000000 + Time.now.usec
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :ttl => ttl, :optional_timestamp => timestamp }
        @client.insert(:test, item)

        assert @client.send(:insert_statement, :test, item, ttl, timestamp).cql =~ /\sUSING\sTTL\s#{ttl}\sAND\sTIMESTAMP\s#{timestamp}/
        assert_equal string_keys(item), @client.select(:test).execute.first
        saved_timestamp = @client.select_writetime(:test, :d, { :id => 'i' })
        assert_equal timestamp, saved_timestamp
      end

      context 'batched inserts' do
        setup do
          collected_inserts = []
          ttl = 12345
          timestamp = Time.now.to_i * 1000000 + Time.now.usec
          item1 = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :ttl => ttl, :optional_timestamp => timestamp }
          item2 = { :id => 'j', :a => 1, :b => 'c', :c => "'\"item(", :d => 2, :ttl => ttl, :optional_timestamp => timestamp }

          collected_inserts << @client.generate_batch_insertion_element(:test, item1)
          collected_inserts << @client.generate_batch_insertion_element(:test, item2)
        end

        should 'connect inserts for batching' do
          collected_inserts = []
          ttl = 12345
          timestamp = Time.now.to_i * 1000000 + Time.now.usec
          item1 = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :ttl => ttl, :optional_timestamp => timestamp }
          item2 = { :id => 'j', :a => 1, :b => 'c', :c => "'\"item(", :d => 2, :ttl => ttl, :optional_timestamp => timestamp }

          collected_inserts << @client.generate_batch_insertion_element(:test, item1)
          collected_inserts << @client.generate_batch_insertion_element(:test, item2)

          @client.batch_insert(collected_inserts)

          result = @client.select(:test).execute.rows
          assert_equal string_keys(item2), result.next
          assert_equal string_keys(item1), result.next
        end

        should 'only execute the inserts once' do

        end

      end
    end

    context 'select' do

      setup do
        @client.insert(:test, :id => 'i', :a => 1, :b => 'b', :c => '1')
        @client.insert(:test, :id => 'i', :a => 2, :b => 'a', :c => '1')
        @client.insert(:test, :id => 'i', :a => 3, :b => 'c', :c => '1')
        @client.insert(:test, :id => 'i2', :a => 4, :b => 'b', :c => '1')
      end

      should 'select all columns for all items' do
        items = @client.select(:test).execute
        assert_equal 4, items.count
        assert_equal [1,2,3,4].to_set, items.map { |x| x['a'] }.to_set
        assert_equal %w(id a b c d).to_set, items.first.keys.to_set
      end

      should 'select certain columns for all items' do
        items = @client.select(:test, %w(id a c)).execute
        assert_equal 4, items.count
        assert_equal [1,2,3,4].to_set, items.map { |x| x['a'] }.to_set
        assert_equal %w(id a c).to_set, items.first.keys.to_set
      end

      context 'where' do
        should 'allow where clause' do
          items = @client.select(:test).where(:id => 'i').execute
          assert_equal [1,2,3].to_set, items.map { |x| x['a'] }.to_set
        end

        should 'allow string-based where clauses' do
          items = @client.select(:test).where("id = 'i' and a > 1").execute
          assert_equal [2,3].to_set, items.map { |x| x['a'] }.to_set
        end

        should 'allow string-based where clauses with arguments' do
          items = @client.select(:test).where("id = ? and a > ?", 'i', 1).execute
          assert_equal [2,3].to_set, items.map { |x| x['a'] }.to_set
        end

        should 'allow multiple where clauses to be chained' do
          items = @client.select(:test).where(:id => 'i').where('a > 1').execute
          assert_equal [2,3].to_set, items.map { |x| x['a'] }.to_set
        end

        should 'create an IN clause when a list of values is passed' do
          items = @client.select(:test).where(:id => 'i', :a => 1, :b => %w(a b)).execute
          assert_equal [1], items.map { |x| x['a'] }
        end

        context 'hash arguments' do
          should 'allow single and double quotes in the value' do
            items = @client.select(:test).where(:id => "'\"abc").execute
            assert_equal [], items.to_a
          end
        end

        context 'string arguments' do
          should 'allow single and double quotes in the value' do
            items = @client.select(:test).where('id = ?', "'\"abc").execute
            assert_equal [], items.to_a
          end
        end
      end

      should 'order by ascending primary key by default' do
        items = @client.select(:test).where(:id => 'i').execute
        assert_equal [1,2,3], items.map { |x| x['a'] }
      end

      should 'allow order to specify ordering' do
        items = @client.select(:test).where(:id => 'i').order(:a, :desc).execute
        assert_equal [3, 2, 1], items.map { |x| x['a'] }
      end

      should 'allow limiting the result count' do
        items = @client.select(:test).where(:id => 'i').limit(2).execute
        assert_equal [1, 2], items.map { |x| x['a'] }
      end

      should 'not allow queries across multiple rows if allow_filtering is not set' do
        assert_raises(Cassandra::Errors::InvalidError) { @client.select(:test).where(:a => 1).execute }
      end

      should 'allow queries across multiple rows if allow_filtering is set' do
        items = @client.select(:test).where('a >= 3').allow_filtering.execute
        assert_equal [3, 4].to_set, items.map { |x| x['a'] }.to_set
      end

      should 'allow clauses to be chained in any order' do
        items = @client.select(:test).limit(2).allow_filtering.where('a >= 2').execute
        assert_equal [2, 3].to_set, items.map { |x| x['a'] }.to_set
      end

      should 'allow select statements to be modified without affecting the original statement' do
        partial_query = @client.select(:test).allow_filtering.where(:a => 1)

        items = partial_query.where(:b => 'missing').execute
        assert_equal 0, items.count

        original_items = partial_query.execute
        assert_equal 1, original_items.count
      end

      should 'support count queries' do
        count = @client.select(:test).where("id = ? and a > ?", 'i', 1).count.execute
        assert_equal 2, count.first["count"]
      end
    end

    context 'select_ttl' do
      should 'allow an existing ttl to be read' do
        ttl = 12345
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1, :ttl => ttl }
        @client.insert(:test, item)

        resulting_ttl = @client.select_ttl(:test, :d, {:id => 'i'})
        assert (1..ttl).include? resulting_ttl
      end

      should 'return nil if there is no ttl set on a cell' do
        item = { :id => 'i', :a => 1, :b => 'b', :c => "'\"item(", :d => 1 }
        @client.insert(:test, item)

        resulting_ttl = @client.select_ttl(:test, :d, {:id => 'i'})
        assert resulting_ttl.nil?
      end
    end

    context 'select_writetime' do
      should 'build the correct writetime select statement' do
        statement = @client.send(:select_writetime_statement, :test, :d, { :id => 'i' })
        assert_match /SELECT WRITETIME/, statement.cql
      end

      should 'correctly fetch the timestamp of a given column' do
        item = { :id => 'i', :a => 1, :b => 'b', :c => "item", :d => 1 }
        @client.insert(:test, item)

        timestamp = @client.select_writetime(:test, :d, { :id => 'i' })
        assert timestamp
      end
    end

    context 'delete' do
      setup do
        @client.insert(:test, :id => 'i', :a => 2, :b => 'a', :c => '1', :d => 1)
        @client.insert(:test, :id => 'i', :a => 3, :b => 'c', :c => '1', :d => 1)
      end

      should 'delete entire rows' do
        @client.delete(:test).where(:id => 'i', :a => 2).execute
        items = @client.select(:test).where(:id => 'i', :a => 2).execute
        assert_equal 0, items.count
      end

      should 'delete multiple elements in a partition' do
        @client.delete(:test).where(:id => 'i').execute
        items = @client.select(:test).where(:id => 'i').execute
        assert_equal 0, items.count
      end

      should 'delete individual columns' do
        @client.delete(:test, [:c, :d]).where(:id => 'i', :a => 2, :b => 'a').execute
        items = @client.select(:test).where(:id => 'i', :a => 2).execute
        assert_nil items.first['c']
        assert_nil items.first['d']
      end

      should 'delete with a timestamp' do 
        timestamp = Time.now.to_i * 1000000 + Time.now.usec
        assert @client.delete(:test, [:c, :d], timestamp).where(:id => 'i', :a => 2, :b => 'a').statement =~ /\sUSING\sTIMESTAMP\s#{timestamp}/
        @client.delete(:test, [:c, :d]).where(:id => 'i', :a => 2, :b => 'a').execute
        items = @client.select(:test).where(:id => 'i', :a => 2).execute
        assert_nil items.first['c']
        assert_nil items.first['d']
      end

      context 'hash arguments' do
        should 'allow single and double quotes in the value' do
          # no error raised
          @client.delete(:test).where(:id => "'\"abc").execute
        end
      end

      context 'string arguments' do
        should 'allow single and double quotes in the value' do
          # no error raised
          @client.delete(:test).where('id = ?', "'\"abc").execute
        end
      end
    end
  end
end
