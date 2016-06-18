require 'mysql'
require 'pp'
require 'json'

class WIKK_SQL
  VERSION = '1.0.0'

  attr_reader :affected_rows, :result, :my
    
  #Class level. Set up the connection, using the db_config class.
  def self.connect(db_config)
    sql = self.new
    sql.connect(db_config)
    if block_given?
      yield sql
      sql.close
    else
      return sql
    end
  end
  
  #Instance level. Set up the connection
  def connect(db_conf)
    @my = Mysql::new(db_conf.host, db_conf.dbuser, db_conf.key, db_conf.db ) 
    #@@my.reconnect = true
    if block_given?
      yield
      close
    end
    return @my
  end
  
  alias open connect
  
  def close
    @my.close if @my != nil
  end
  
  def query(the_query)
    begin
      if result != nil 
        @result.free #Free any result we had left over from previous use.
        @result = nil
      end
      @affected_rows = 0 #incase this query crashes and burns, this will have a value.
      @affected_rows = @my.affected_rows #This is non-zero for insert/delete/update of rows
      @result = @my.query(the_query)
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
    
  #Return result by row
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
  
  #Return result by row
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
  
  def each_sym(the_query)
    each_hash(the_query) do |row_hash|
      yield row_hash.each_with_object({}) { |(k,v),h| h[k.to_sym] = v }
    end
  end

  def fetch_fields
    @result.fetch_fields
  end

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




