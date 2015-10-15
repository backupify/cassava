# Cassava

[![Code Climate](https://codeclimate.com/github/backupify/cassava/badges/gpa.svg)](https://codeclimate.com/github/backupify/cassava)

An unopinionated Cassandra client built on top of the Datastax Cassandra Driver. Cassava provides a higher-level statement execution interface while still supporting asynchronous queries and the ability to connect to multiple clusters.

 _If prepared incorrectly, the cassava plant can produce cyanide, a deadly compound when consumed._

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'cassava', github: 'backupify/cassava'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install cassava

## Usage

Creating a client requires a `Cassandra::Session` object:

```ruby
require 'cassava'
cluster = Cassandra.cluster
session = cluster.connect('a_keyspace')
client = Cassava::Client.new(session)
```

### Insert

```ruby
client.insert(:table, :id => 'id', :a => 1, :b => 'b')

```

### Select

A select statement is built and then executed. To create a statement that will
select all columns:

```ruby
statement = client.select(:table)
```

This statement can then be further refined:

```ruby
new_statement = statement.where(:id => 'id').limit(2)
```

and then executed:

```ruby
result = new_statement.execute
```

or executed asynchronously:

```ruby
promise = new_statement.execute_async
```

To select only certain rows, provide those rows to the select method:

```ruby
client.select(:table, [:id, :a, :b]).execute
```

Ordering can be specified using the order method:

```ruby
client.select(:table).where('id = ? AND a > ?', 1, 'b').order(:a, :desc).execute
```

Filtering is permitting with the `allow_filtering` method.

Multiple records can be specified by passing an array of values, but this will generate an CQL IN query and should be used with caution:

```ruby
client.select(:table).where(:id => 1, :a => [1, 2])
```

### Delete

To delete an entire record:

```ruby
client.delete(table).where(:id => 1, :a => 1).execute
```

To delete only certain columns:

```ruby
client.delete(table, [:c, :d]).where(:id => 1, :a => 1).execute
```

Note here that :c and :d must not be part of the primary key.

## Contributing

1. Fork it ( https://github.com/backupify/cassava/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
