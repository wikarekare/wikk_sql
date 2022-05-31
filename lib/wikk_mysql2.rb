# Provides common front end, even if we change the connector library
# Connector is mysql2. Requires libmysql-dev or libmariadb-dev to install the gem.
module WIKK
  require 'mysql2'

  # WIKK_SQL wrapper for ruby mysql gem.
  # @attr_reader [Numeric] affected_rows the number of rows changed, deleted, or added.
  # @attr_reader [Mysql::Result] result the last query's result
  # @attr_reader [Mysql] my the DB connection descriptor
  class SQL
    attr_reader :affected_rows, :result, :my

    # Create WIKK::SQL instance and set up the mySQL connection.
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @yieldparam sql [WIKK_SQL] if a block is given.
    # @return [NilClass] if block is given, and closes the mySQL connection.
    # @return [WIKK_SQL] if no block is given, and caller must call sql.close
    def self.connect(db_config)
      sql = self.new
      sql.connect(db_config)
      if block_given?
        yield sql
        return sql.close
      else
        return sql
      end
    end

    # Set up the mySQL connection.
    #
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @yieldparam [] if a block is given.
    # @return [NilClass] if block is given, and closes the mySQL connection.
    # @return [WIKK_SQL] if no block is given, and caller must call sql.close
    def connect(db_config)
      if db_config.instance_of?(Hash)
        sym = db_config.transform_keys(& :to_sym )
        db_config = Struct.new(*(k = sym.keys)).new(*sym.values_at(*k))
      end

      begin
        @my = Mysql2::Client.new( host: db_config.host,
                                  database: db_config.db,
                                  username: db_config.dbuser,
                                  password: db_config.key,
                                  socket: db_config.respond_to?(:socket) ? db_config.socket : nil,
                                  port: db_config.respond_to?(:port) ? db_config.port : nil,
                                  flags: 'SESSION_TRACK',
                                  init_command: 'SET @@SESSION.session_track_schema=ON'
                                )
      rescue StandardError => e
        @my = nil
        raise e
      end
      raise Mysql2::Error, 2002 if @my.nil?

      # @@my.reconnect = true
      if block_given?
        yield
        return close
      end
      return @my
    end

    alias open connect

    # close the mySQL connection. Call only if connect was not given a block.
    #
    # @return [NilClass]
    def close
      @my.close if @my != nil
      return (@my = nil)
    end

    # Run a query on the DB server.
    #
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Mysql::Result] @result and @affected_rows are also set.
    # @return [Mysql::Result] @result and @affected_rows are also set.
    def query(the_query, options = nil )
      raise Mysql2::Error, 2002 if @my.nil?

      # Default to returning values as an array, with no caching of previous results
      # This is consistent with ruby-mysql
      options ||= { as: :array, cache_rows: false }
      begin
        if @result != nil
          @result.free # Free any result we had left over from previous use.
          @result = nil
        end
        @affected_rows = 0 # incase this query crashes and burns, this will have a value.
        @result = @my.query(the_query, options)
        @affected_rows = @my.affected_rows # This is non-zero for select/insert/delete/update of rows
        if block_given?
          yield @result
        else
          return @result
        end
      rescue Mysql2::Error => e
        if @result != nil
          @result.free # Free any result we had left over from previous use.
          @result = nil
        end
        raise e
      end
    end

    # Perform a transaction in the passed block.
    # RollBACK on error, otherwise COMMIT
    #
    # @yieldparam [] yields to block, where the queries are performed.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    def transaction
      raise Mysql2::Error, 2002 if @my.nil?

      if block_given?
        begin
          @my.query('START TRANSACTION WITH CONSISTENT SNAPSHOT')
          yield # Start executing the query black.
          @my.query('COMMIT')
        rescue Mysql2::Error => e
          @my.query('ROLLBACK')
          raise e
        end
      end
    end

    # Yields query query results row by row, as Array
    #
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Array] each result row
    # @return [Array] of rows
    # @note @result and @affected_rows are also set via call to query().
    def each_row(the_query, &block)
      begin
        query(the_query, { as: :array, cache_rows: false })
        unless @result.nil?
          if block_given?
            @affected_rows = @result.num_rows # This is non-zero is we do a select, and get results.
            @result.each(&block)
          else
            result = []
            @result.each { |row| result << row }
            return result
          end
        end
      rescue Mysql2::Error => e
        # puts "#{e.errno}: #{e.error}"
        raise e
      ensure
        if block_given? && @result != nil
          @result.free
        end
      end
    end

    # Yields query result row by row, as Hash, using String keys
    #
    # @param the_query [String]  Sql query to send to DB server.
    # @param with_table_names [Boolean] if TrueClass, then table names are included in the hash keys.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @return [Array] If no block is given:  of rows with hash keys
    # @note @result and @affected_rows are also set via call to query().
    def each_hash(the_query, with_table_names = false, &block)
      begin
        if with_table_names
          # We have to build the hash ourselves, if we want table names included
          query(the_query, { as: :array, cache_rows: false })
          if @result != nil
            fields = @result.fields
            tables = @result.respond_to?(:tables) ? @result.tables : [] # My addition to mysql2 results.c

            result = []
            @result.each do |row|
              hrow = {}
              (0...row.length).each do |i|
                field_name = tables[i].nil? ? fields[i] : "#{tables[i]}.#{fields[i]}"
                hrow[field_name] = row[i]
              end
              yield hrow
              result << hashed_row
            end
            return result
          end
        else
          query(the_query, { as: :hash, cache_rows: false })
          if @result != nil
            if block_given?
              @result.each(&block)
            else
              result = []
              @result.each { |row| result << row }
              return result
            end
          end
        end
      rescue Mysql2::Error => e
        # puts "#{e.errno}: #{e.error}"
        raise e
      ensure
        if block_given? && @result != nil
          @result.free
        end
      end
    end

    # Yields query result row by row, as Hash using Symbol keys, so can't have table names included.
    # This can be used with keyword arguments. eg. each_sym { |key1:, key2:, ..., **rest_of_args| do something }
    #
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @return [Array] If no block is given: of hashed rows with symbol keys.
    # @note @result and @affected_rows are also set via call to query().
    def each_sym(the_query)
      query(the_query, { symbolize_keys: true, as: :hash, cache_rows: false })
      if @result != nil
        if block_given?
          @result.each(&block)
        else
          result = []
          @result.each { |row| result << row }
          return result
        end
      end
    end

    # Get the database field attributes from a query result.
    #
    # @return [Array][Mysql::Field] Array of field records
    # @note mySQL2 doesn't expose this structure. Only parts of it.
    def fetch_fields
      fields = @result.fields
      field_types = @result.field_types
      tables = @result.respond_to?(:tables) ? @result.tables : [] # My addition to mysql2 results.c
      fields_arr = []
      (0...@result.fields.length).each do |i|
        fields_arr[i] = MySQL_FIELD.new(
          name: fields[i],
          table: tables[i],
          type: field_types[i]
        )
      end
      return fields_arr
    end

    # Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    #
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Mysql::Result] @result and @affected_rows are also set.
    # @return [Mysql::Result] @result and @affected_rows are also set.
    def self.query(db_config, the_query)
      self.connect db_config do |sql|
        result = sql.query(the_query)
        if block_given?
          yield result
          return sql.affected_rows
        else
          return result
        end
      end
    end

    # Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    # Yields query query results row by row, as Array
    #
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Array] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_row(db_config, query, &block)
      self.connect db_config do |sql|
        if block_given?
          sql.each_row(query, &block)
          return sql.affected_rows
        else
          return sql.each_row(query)
        end
      end
    end

    # Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    # Yields query result row by row, as Hash, using String keys
    #
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @param with_table_names [Boolean] if TrueClass, then table names are included in the hash keys.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_hash(db_config, query, with_table_names = false, &block)
      self.connect( db_config ) do |sql|
        if block_given?
          sql.each_hash(query, with_table_names, &block)
          return sql.affected_rows
        else
          return sql.each_hash(query, with_table_names)
        end
      end
    end

    # Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    # Yields query result row by row, as Hash using Symbol keys, so can't have table names included.
    #
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # This can be used with keyword arguments. eg. each_sym { |key1:, key2:, ..., **rest_of_args| do something }
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_sym(db_config, query, &block)
      self.connect( db_config ) do |sql|
        if block_given?
          sql.each_sym(query, &block)
          return sql  # May be useful to access the affected rows
        else
          return sql.each_sym(query)
        end
      end
    end
  end
end
