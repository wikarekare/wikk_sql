#!/usr/local/bin/ruby
#Warning Site specific testing going on here!!!!!!!!!!

require_relative '../lib/wikk_sql.rb' #Local test
#require "wikk_sql" #test installed gem
require 'wikk_configuration'

@config = Configuration.new('../../../etc/ntm_conf.json')

def test_class_lvl_each_row
  puts "test_class_lvl_each_row: select name, site_name from customer limit 2"
  WIKK_SQL::each_row(@config, "select name, site_name  from customer limit 2") do |row|
    printf "%s, %s\n", row[0], row[1]
  end
end

def test_class_lvl_each_hash
  puts "test_class_lvl_each_hash: select * from customer limit 2"
  WIKK_SQL::each_hash(@config, "select * from customer limit 2") do |row|
    row.each do |k,v|
      printf "  %s => %s\n", k, v
    end
  end
end

def test_class_lvl_each_sym
  puts "test_class_lvl_each_sym select * from customer limit 2"
  WIKK_SQL::each_sym(@config, "select * from customer limit 2") do |customer_id:, name:, site_name:, **row|
    printf "customer_id %s  site_name %s name %s\n", customer_id, site_name, name
  end
end

def test_class_lvl_each_hash_with_table_names
  puts "test_class_lvl_each_hash_with_table_names: select * from customer limit 2"
  WIKK_SQL::each_hash(@config, "select * from customer limit 2", with_table_names = true) do |row|
    puts "Row"
    row.each do |k,v|
      printf "  %s => %s\n", k, v
    end
  end
end


  
def test_instance_lvl_get_fields
  puts "test_instance_lvl_get_fields: select * from customer limit 1"
  WIKK_SQL::connect(@config) do |sql|
    sql.query("select * from customer limit 1") do |result|
      result.fetch_fields.each_with_index do |info, i|
             printf "--- Column %d (%s) ---\n", i, info.name
             printf "table:            %s\n", info.table
             printf "def:              %s\n", info.def
             printf "type:             %s\n", info.type
             printf "length:           %s\n", info.length
             printf "max_length:       %s\n", info.max_length
             printf "flags:            %s\n", info.flags
             printf "decimals:         %s\n", info.decimals
      end
    end
  end
end

def test_instance_lvl_each_row
  puts "test_instance_lvl_each_row: select name, site_name from customer limit 2"
  WIKK_SQL::connect(@config) do |sql|
    sql.each_row("select name, site_name from customer limit 2") do |row|
      printf "%s, %s\n", row[0], row[1]
    end
    puts "Number of rows returned: #{sql.affected_rows}"
  end
end

def test_instance_lvl_each_hash
  puts "test_instance_lvl_each_hash: select * from customer limit 2"
  WIKK_SQL::connect(@config) do |sql|
    sql.each_hash("select * from customer limit 2") do |row|
      puts "Row"
      row.each do |k,v|
        printf "  %s => %s\n", k, v
      end
    end
    puts "Number of rows returned: #{sql.affected_rows}"
  end
end


#
def test_transaction
  puts "test_transaction: select site_name, state, INET_NTOA(network + ..."
  WIKK_SQL::connect(@config) do |sql|
    sql.transaction do
      sql.each_row("select customer_id, site_name from customer limit 1") do |row|
        printf "%s, %s\n", row[0], row[1]
      end
      sql.each_row("select distribution_id, site_name from distribution limit 1") do |row|
        printf "%s, %s\n", row[0], row[1]
      end
    end
  end
end


puts "*****Test Class level each row********"
test_class_lvl_each_row
puts
puts "*****Test Class level each hash********"
test_class_lvl_each_hash
puts
puts "*****Test Class level each sym********"
test_class_lvl_each_sym
puts
puts "*****Test each row********"
test_class_lvl_each_hash_with_table_names
puts
puts "*****Test get fields********"
test_instance_lvl_get_fields
puts
puts "*****Test transaction 1 customer, 1 distribution ********"
test_transaction
puts

