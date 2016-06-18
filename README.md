# wikk_sql

Wrappers around mysql gem

## DESCRIPTION:

Instance level
  connect(config)  { |sql| rest of block } #Aliased to open
  close #Call if connect didn't get passed a block
  query(the_query)
  each_row(the_query) { |row| rest of block }  
  each_hash(the_query, with_table_names) { |hash| rest of block }  
  each_sym(the_query) { |sym:, sym:, ..., **hash| rest of block }
  transaction { calls to query, each_row, each_hash or each_sym }
  fetch_fields #Returns table field info
  affected_rows #Returns the number of rows changed, inserted or deleted.
  
Class level calls mirror the instance level calls taking blocks
   WIKK_SQL::connect(config,...) { |sql| rest of block }  
   WIKK_SQL::each_row(config,...) { |row| rest of block }  
   WIKK_SQL::each_hash(config,...) { |hash| rest of block }  
   WIKK_SQL::each_sym(config,...) { |sym:, sym:, ..., **hash| rest of block }
   
config is hash of the form
    config = {
        "host" => "hostname or IP",
        "db" => "database name",
        "dbuser" => "user to login as",
        "key" => "password"
    }
  
## FEATURES/PROBLEMS:


## SYNOPSIS:

###Instance example
  WIKK_SQL::connect(@config) do |sql|
    sql.each_hash("select * from customer limit 2", with_table_names = true) do |row|
      row.each do |k,v|
        printf "  %s => %s\n", k, v
      end
    end
    puts "Number of rows returned: #{sql.affected_rows}"
  end
  
###Class level example
  WIKK_SQL::each_sym(@config, "select * from customer limit 2") do |customer_id:, name:, site_name:, **row|
    printf "customer_id %s  site_name %s name %s\n", customer_id, site_name, name
  end
  

## REQUIREMENTS:

* require 'wikk_sql'

Can use wikk_configuration gem to load config from a json file.

## INSTALL:

* sudo gem install wikk_sql

## LICENSE:

(The MIT License)

Copyright (c) 2016 FIX

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
