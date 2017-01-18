module Cassava
  class Client
    attr_reader :session, :executor

    # @param session [Cassandra::Session] The session object
    # @option opts [Object] :logger responding to :debug, :info, :warn, :error, :fatal
    def initialize(session, opts = {})
      @session = session
      logger = opts[:logger] || NullLogger.new
      @executor = Executor.new(session, logger)
    end

    # @see #insert
    def insert_async(table, data)
      ttl = data.delete(:ttl)
      executor.execute_async(insert_statement(table, data, ttl), :arguments => data.values)
    end

    # @param table [Symbol] the table name
    # @param data [Hash] A hash of column names to data, which will be inserted into the table
    def insert(table, data)
      ttl = data.delete(:ttl)
      statement = insert_statement(table, data, ttl)
      executor.execute(statement, :arguments => data.values)
    end

    # @param table [Symbol] the table name
    # @param columns [Array<Symbol>] An optional list of column names (as symbols), to only select those columns
    # @return [StatementBuilder] A statement builder representing the partially completed statement.
    def select(table, columns = nil)
      StatementBuilder.new(executor).select(table, columns)
    end

    # @param table [Symbol] the table name
    # @param target_attr [String] The attribute to select the TTL for
    # @param where_arguments [Hash] Pairs of keys and values for the where clause
    def select_ttl(table, target_attr, where_arguments)
      statement = select_ttl_statement(table, target_attr, where_arguments)
      executor.execute(statement).rows.first["ttl(#{target_attr})"]
    end

    # @param table [Symbol] the table name
    # @param columns [Array<String] A list of columns that will be deleted. If nil, all columns will be deleted.
    # @return [StatementBuilder] A statement builder representing the partially completed statement.
    def delete(table, columns = nil)
      StatementBuilder.new(executor).delete(table, columns)
    end

    # Pass a raw query to execute asynchronously to the underlying session object.
    # @param statement[String] The statment to execute
    # @param opts [Hash] options accepted by Cassandra::Session
    def execute_async(statement, opts = {})
      executor.execute_async(statement, opts)
    end

    # Pass a raw query to execute synchronously to the underlying session object.
    # @param statement [String] The statment to execute
    # @param opts [Hash] options accepted by Cassandra::Session
    def execute(statement, opts = {})
      executor.execute(statement, opts)
    end

    private

    def insert_statement(table, data, ttl = nil)
      column_names = data.keys
      statement_cql = "INSERT INTO #{table} (#{column_names.join(', ')}) VALUES (#{column_names.map { |x| '?' }.join(',')})"
      statement_cql += " USING TTL #{ttl}" if ttl
      executor.prepare(statement_cql)
    end

    # @param table [Symbol] the table name
    # @param target_attr [Symbol] The attribute to select the TTL for
    # @param where_arguments [Hash] Pairs of keys and values for the where clause
    def select_ttl_statement(table, target_attr, where_arguments)
      statement = "SELECT ttl(#{target_attr}) FROM #{table} WHERE "
      where_clause = where_arguments.map do |(key, val)|
        if val.is_a? Integer
          "#{key} = #{val}"
        else
          "#{key} = '#{val}'"
        end
      end

      statement + where_clause.join(" AND ")
    end
  end

  class StatementBuilder
    attr_reader :executor, :table, :clauses

    CLAUSE_ORDERING = {
                       :main => 0,
                       :from => 1,
                       :where => 2,
                       :order => 3,
                       :limit => 4,
                       :allow_filtering => 5
                      }

    def initialize(executor, clauses = {})
      @executor = executor
      @table = table
      @clauses = clauses
    end

    # Execute the statement synchronously
    # @param opts [Hash] options accepted by Cassandra::Session
    def execute(opts = {})
      options = opts.dup.merge(:arguments => prepared_arguments)
      executor.execute(prepared_statement, options)
    end

    # Execute the statement asynchronously
    # @param opts [Hash] options accepted by Cassandra::Session
    def execute_async(opts = {})
      options = opts.dup.merge(:arguments => prepared_arguments)
      executor.execute_async(prepared_statement, options)
    end

    # @param table [Symbol] table to select data from
    # @param columns [Array<Symbol>] Columns to select -- defaults to all.
    # @return [StatementBuilder]
    def select(table, columns = nil)
      add_clause(SelectClause.new(table, columns), :main)
    end

    # @param table [Symbol] table to delete data from
    # @param columns [Array<Symbol>] Columns to delete -- defaults to all.
    # @return [StatementBuilder]
    def delete(table, columns = nil)
      add_clause(DeleteClause.new(table, columns), :main)
    end

    # Condition the query based on a condition
    # Provide either a String and a list of arguments, or a hash.
    # @example
    #      statement.where('id = ? and field > ?', 1, 'a')
    # @example
    #      statement.where(:id => 1, :field => 'x')
    # @param args [Array] arguments representing the where condition
    # @return [StatementBuilder]
    def where(*args)
      clause = clauses[:where] || WhereClause.new([], [])
      add_clause(clause.where(*args), :where)
    end

    # Allow filtering for this query
    # @return [StatementBuilder]
    def allow_filtering
      add_clause('ALLOW FILTERING', :allow_filtering)
    end

    # @param clustering_column [Symbol] clustering_column to order by
    # @param direction [:asc|:desc] the direction to order by, defaults to :asc
    # @return [StatementBuilder]
    def order(clustering_column, direction = :asc)
      add_clause("ORDER BY #{clustering_column.to_s} #{direction.to_s}", :order)
    end

    # @param n [Integer] maximum number of results to return
    # @return [StatementBuilder]
    def limit(n)
      add_clause("LIMIT #{n.to_i}", :limit)
    end

    # Return the count of objects rather than the objects themselves
    # @return [StatementBuilder]
    def count
      add_clause(clauses[:main].count, :main)
    end

    # @return [String] the CQL statement that this StatementBuilder represents
    def statement
      clauses.sort_by { |s| CLAUSE_ORDERING[s[0]] }.map { |s| s[1] }.join(' ')
    end

    private

    def prepared_statement
      executor.prepare(statement)
    end

    def prepared_arguments
      clauses[:where] ? clauses[:where].arguments : []
    end

    # Adds a clause of a given type.
    # @return [StatementBuilder] A new StatementBuilder with the added clause
    def add_clause(clause, type)
      clauses_copy = clauses.dup
      clauses_copy[type] = clause
      self.class.new(executor, clauses_copy)
    end
  end

  SelectClause = Struct.new(:table, :columns, :count_boolean) do
    def count
      self.class.new(table, _columns = nil, _count_boolean = true)
    end

    def to_s
      if count_boolean
        "SELECT COUNT(*) FROM #{table}"
      else
        self.columns ||= ['*']
        "SELECT #{columns.join(', ')} from #{table}"
      end
    end
  end

  DeleteClause = Struct.new(:table, :columns) do
    def to_s
      if columns
        "DELETE #{columns.join(', ')} from #{table}"
      else
        "DELETE from #{table}"
      end
    end
  end

  WhereClause = Struct.new(:parts, :arguments) do
    COMPARATOR_SYMBOLS = [ '>', '<' ]
    def where(*args)
      new_parts = self.parts.dup || []
      new_arguments = self.arguments.dup || []

      case args[0]
      when String
        new_parts << args[0]
        new_arguments += args[1..-1]
      when Hash
        new_parts += args[0].map { |key, value| "#{key} #{where_string(value)}" }
        new_arguments += args[0].values.flatten
      when Array
        between_args = args[0][1..-1]
        new_parts += between_args.length.times.map{ |i| next unless between_args[i]; "#{args[0][0]} #{COMPARATOR_SYMBOLS[i]} ?" }.compact
        new_arguments += between_args.compact
      end
      self.class.new(new_parts, new_arguments)
    end

    def to_s
      "WHERE #{parts.join(' AND ')}"
    end

    private

    def where_string(value)
      case value
      when Array
        quoted_values = value.map { |v| type_quote(v) }
        "IN(#{quoted_values.map { |_| '?' }.join(', ')})"
      else "= ?"
      end
    end

    def type_quote(value)
      case value
      when Numeric then value.to_s
      when String then "'#{value}'"
      end
    end
  end

  Executor = Struct.new(:session, :logger) do
    def execute(statement, opts = {})
      log_statement(statement, opts)
      session.execute(statement, opts)
    rescue => e
      log_error(e, statement, opts)
      raise e
    end

    def execute_async(statement, opts = {})
      log_statement(statement, opts)
      session.execute_async(statement, opts)
    end

    def prepare(*args)
      session.prepare(*args)
    end

    def log_statement(statement, opts)
      logger.debug("Executing Cassandra request #{statement.to_s} with options #{opts}")
    end

    def log_error(e, statement, opts)
      logger.debug("Error #{e} executing Cassandra request #{statement.to_s} with options #{opts}")
    end
  end

  class NullLogger
    def method_missing(*); end
  end
end
