# Provides common front end, even if we change the connector library
# Choice of ruby-mysql or mysql2 connectors
module WIKK
  require 'pp'
  require 'json'

  # From mysql.h, but mostly blank for both our connectors
  MySQL_FIELD = Struct.new(
    :name,        # Field name. Could be an 'as' alias
    :org_name,    # Field name, not aliased (for introspection)
    :table,       # Table name. Could be an 'as' alias
    :org_table,   # Table name, not aliased (for introspection)
    :db,          # database for table (useful for multitable queries)
    :catalog,     # catalog for table
    :def,         # Default value (if code wants to revert to the default)
    :col_width,   # Width of column (length in mysql.h, but length is taken in Ruby)
    :max_length,  # Max width for selected set
    :flags,       # Div flags
    :decimals,    # Number of decimals in field
    :charsetnr,   # Character set
    :type,        # Field type
    :extension
  )

  class SQL
    VERSION = '0.2.1'
  end

  # Prefer mysql2 over ruby-mysql, as it is faster
  # Downside is, mysql2 doesn't expose field table names.
  begin
    Gem::Specification.find_by_name('mysql2')
  rescue Gem::MissingSpecError
    begin
      Gem::Specification.find_by_name('ruby-mysql')
    rescue Gem::MissingSpecError
      raise Gem::MissingSpecError 'Need either mysql2 or ruby-mysql gems'
    else
      require_relative 'wikk_ruby_mysql.rb'
    end
  else
    require_relative 'wikk_mysql2.rb'
  end
end
