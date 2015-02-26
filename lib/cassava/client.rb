module Cassava
  class Client
    attr_reader :session

    # @param [Cassandra::Session] The session object
    def initialize(session)
      @session = session
    end

    # @see #insert
    def insert_async(table, data)
      session.execute_async(insert_statement(table, data), :arguments => data.values)
    end

    # @param [Symbol] table the table name
    # @param [Hash] A hash of column names to data, which will be inserted into the table
    def insert(table, data)
      session.execute(insert_statement(table, data), :arguments => data.values)
    end

    # @param [Symbol] table the table name
    # @param [Array<Symbol>] An optional list of column names (as symbols), to only select those columns
    # @return [StatementBuilder] A statement builder representing the partially completed statement.
    def select(table, columns = nil)
      StatementBuilder.new(session).select(table, columns)
    end

    # @param [Symbol] table the table name
    # @param [Array<String] A list of columns that will be deleted. If nil, all columns will be deleted.
    # @return [StatementBuilder] A statement builder representing the partially completed statement.
    def delete(table, columns = nil)
      StatementBuilder.new(session).delete(table, columns)
    end

    def execute_async(statement, opts = {})
      session.execute_async(statement, opts)
    end

    def execute(statement, opts = {})
      session.execute(statement, opts)
    end

    private

    def insert_statement(table, data)
      column_names = data.keys
      statement_cql = "INSERT INTO #{table} (#{column_names.join(', ')}) VALUES (#{column_names.map { |x| '?' }.join(',')})"
      session.prepare(statement_cql)
    end
  end

  class StatementBuilder
    attr_reader :session, :table, :clauses

    CLAUSE_ORDERING = {
                       :main => 0,
                       :from => 1,
                       :where => 2,
                       :order => 3,
                       :limit => 4,
                       :allow_filtering => 5
                      }

    def initialize(session, clauses = {})
      @session = session
      @table = table
      @clauses = clauses
    end

    def execute_async(opts = {})
      session.execute_async(statement, opts)
    end

    def execute(opts = {})
      session.execute(statement, opts)
    end

    def statement
      clauses.sort_by { |s| CLAUSE_ORDERING[s[0]] }.map { |s| s[1] }.join(' ')
    end

    def select(table, columns = nil)
      add_clause(SelectClause.new(table, columns), :main)
    end

    def delete(table, columns = nil)
      add_clause(DeleteClause.new(table, columns), :main)
    end

    def where(arg)
      clause = clauses[:where] || WhereClause.new([])
      add_clause(clause.where(arg), :where)
    end

    def allow_filtering
      add_clause('ALLOW FILTERING', :allow_filtering)
    end

    def order(clustering_column, direction = :asc)
      add_clause("ORDER BY #{clustering_column.to_s} #{direction.to_s}", :order)
    end

    def limit(n)
      add_clause("LIMIT #{n}", :limit)
    end

    def count
      add_clause(clauses[:main].count, :main)
    end

    private

    # Adds a clause of a given type.
    # @return [StatementBuilder] A new StatementBuilder with the added clause
    def add_clause(clause, type)
      clauses_copy = clauses.dup
      clauses_copy[type] = clause
      self.class.new(session, clauses_copy)
    end
  end

  SelectClause = Struct.new(:table, :columns, :count_boolean) do
    def count
      self.class.new(table, columns = nil, count_boolean = true)
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

  WhereClause = Struct.new(:parts) do
    def where(clause)
      self.class.new(parts.dup << clause)
    end

    def to_s
      predicates = parts.map do |part|
        case part
        when String then part
        when Hash then part.map { |key, value| "#{key} #{where_string(value)}" }.join(' AND ')
        end
      end
      "WHERE #{predicates.join(' AND ')}"
    end

    private

    def where_string(value)
      case value
      when Array
        quoted_values = value.map { |v| type_quote(v) }
        "IN(#{quoted_values.join(', ')})"
      else "= #{type_quote(value)}"
      end
    end

    def type_quote(value)
      case value
      when Numeric then value.to_s
      when String then "'#{value}'"
      end
    end
  end
end
