module WIKK
  require 'mysql'
  require 'pp'
  require 'json'

  #WIKK_SQL wrapper for ruby mysql gem.
  # @attr_reader [Numeric] affected_rows the number of rows changed, deleted, or added.
  # @attr_reader [Mysql::Result] result the last query's result
  # @attr_reader [Mysql] my the DB connection descriptor
  class SQL
    VERSION = '0.1.2'

    attr_reader :affected_rows, :result, :my
    
    #Create WIKK::SQL instance and set up the mySQL connection.
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
  
    #Set up the mySQL connection.
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @yieldparam [] if a block is given.
    # @return [NilClass] if block is given, and closes the mySQL connection. 
    # @return [WIKK_SQL] if no block is given, and caller must call sql.close 
    def connect(db_config)
      if db_config.class == Hash
        sym = db_config.each_with_object({}) { |(k,v),h| h[k.to_sym] = v }
        db_config = Struct.new(*(k = sym.keys)).new(*sym.values_at(*k))
      end
    
      @my = Mysql::new(db_config.host, db_config.dbuser, db_config.key, db_config.db ) 
      #@@my.reconnect = true
      if block_given?
        yield
        return close
      end
      return @my
    end
  
    alias open connect
  
    #close the mySQL connection. Call only if connect was not given a block.
    # @return [NilClass]
    def close
      @my.close if @my != nil
      return (@my = nil)
    end
  
    #Run a query on the DB server.
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Mysql::Result] @result and @affected_rows are also set.
    # @return [Mysql::Result] @result and @affected_rows are also set.
    def query(the_query)
      begin
        if result != nil 
          @result.free #Free any result we had left over from previous use.
          @result = nil
        end
        @affected_rows = 0 #incase this query crashes and burns, this will have a value.
        @result = @my.query(the_query)
        @affected_rows = @my.affected_rows #This is non-zero for insert/delete/update of rows
        if block_given?
          yield @result
        else
          return @result
        end
      rescue Mysql::Error => e
        if result != nil 
          @result.free #Free any result we had left over from previous use.
          @result = nil
        end
        raise e
      end
    end

    #Perform a transaction in the passed block.
    #RollBACK on error, otherwise COMMIT
    # @yieldparam [] yields to block, where the queries are performed.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    def transaction
      puts "transaction"
      if block_given?
        begin
          @my.query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
          yield #Start executing the query black.
          @my.query("COMMIT")
        rescue Mysql::Error => e
          @my.query("ROLLBACK")
          raise e
        end
      end
    end
    
    #Yields query query results row by row, as Array
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Array] each result row
    # @note @result and @affected_rows are also set via call to query().
    def each_row(the_query)
      begin
        query(the_query)
        if @result != nil && block_given?
          @affected_rows = @result.num_rows() #This is non-zero is we do a select, and get results.
          @result.each do |row|
            yield row #return one row at a time to the block
          end
        end
      rescue Mysql::Error => e
        #puts "#{e.errno}: #{e.error}"
        raise e
      ensure
        if block_given? && @result != nil
          @result.free
        end
      end
    end 
  
    #Yields query result row by row, as Hash, using String keys
    # @param the_query [String]  Sql query to send to DB server.
    # @param with_table_names [Boolean] if TrueClass, then table names are included in the hash keys.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def each_hash(the_query, with_table_names=false)
      begin
        query(the_query)
        if @result != nil && block_given?
          @affected_rows = @result.num_rows() #This is non-zero is we do a select, and get results.
          @result.each_hash(with_table_names) do |row|
            yield row
          end
        end
      rescue Mysql::Error => e
        #puts "#{e.errno}: #{e.error}"
        raise e
      ensure
        if block_given? && @result != nil
          @result.free
        end
      end
    end 
  
    #Yields query result row by row, as Hash using Symbol keys, so can't have table names included.
    #This can be used with keyword arguments. eg. each_sym { |key1:, key2:, ..., **rest_of_args| do something }
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def each_sym(the_query)
      each_hash(the_query) do |row_hash|
        yield row_hash.each_with_object({}) { |(k,v),h| h[k.to_sym] = v }
      end
    end

    #Get the database field attributes from a query result.
    # @yieldparam [Array][Mysql::Field] Array of field records
    # @note fields are name (of field), table (name), def, type, length, max_length, flags,decimals
    def fetch_fields
      @result.fetch_fields
    end

    #Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Mysql::Result] @result and @affected_rows are also set.
    # @return [Mysql::Result] @result and @affected_rows are also set.
    def self.query(db_config, the_query)
      sql = self.new
      sql.open db_config
      begin
        result = sql.query(the_query)
        if block_given?
          yield result
        end
      ensure
        sql.close
      end
      return result
    end

    #Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    #Yields query query results row by row, as Array
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Array] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_row(db_config, query)
      sql = self.new
      sql.open db_config
      begin
        if block_given?
          sql.each_row(query) { |y| yield y }
        end
      ensure
        sql.close
      end
      return sql
    end
  
    #Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    #Yields query result row by row, as Hash, using String keys
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    # @param the_query [String]  Sql query to send to DB server.
    # @param with_table_names [Boolean] if TrueClass, then table names are included in the hash keys.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_hash(db_config, query, with_table_names=false)
      sql = self.new
      sql.open db_config
      begin
        if block_given?
          sql.each_hash(query, with_table_names) do |res|
            yield res
          end
        end
      ensure
        sql.close
      end
      return sql
    end

    #Create WIKK::SQL instance and set up the mySQL connection, and Run a query on the DB server.
    #Yields query result row by row, as Hash using Symbol keys, so can't have table names included.
    # @param db_config [Configuration]  Configuration class, Hash, or any class with appropriate attr_readers.
    #This can be used with keyword arguments. eg. each_sym { |key1:, key2:, ..., **rest_of_args| do something }
    # @param the_query [String]  Sql query to send to DB server.
    # @raise [Mysql] passes on Mysql errors, freeing the result.
    # @yieldparam [Hash] each result row
    # @note @result and @affected_rows are also set via call to query().
    def self.each_sym(db_config, query)
      sql = self.new
      sql.open db_config
      begin
        if block_given?
          sql.each_sym(query) do |**res|
            yield **res
          end
        end
      ensure
        sql.close
      end
      return sql
    end
  end
end


