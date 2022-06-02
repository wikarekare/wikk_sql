#!/usr/local/bin/ruby
# Warning Site specific testing going on here!!!!!!!!!!

require_relative '../lib/wikk_sql.rb' # Local test
# require "wikk_sql" #test installed gem
require 'wikk_configuration'

@config = WIKK::Configuration.new(ARGV[0])

def test_class_lvl_each_row
  puts 'test_class_lvl_each_row: SELECT id, s FROM T1'
  WIKK::SQL.each_row(@config, 'SELECT id, s FROM T1') do |row|
    puts "id: #{row[0]}, s: #{row[1]}"
  end
end

def test_class_lvl_each_hash
  puts 'test_class_lvl_each_hash: SELECT * FROM T1'
  WIKK::SQL.each_hash(@config, 'SELECT * FROM T1') do |row|
    row.each do |k, v|
      print "#{k}: #{v}, "
    end
    puts
  end
end

def test_class_lvl_each_sym
  puts 'test_class_lvl_each_sym SELECT * FROM T1'
  WIKK::SQL.each_sym(@config, 'SELECT * FROM T1') do |row|
    row.each do |k, v|
      print "#{k}: #{v}, "
    end
    puts
  end
end

def test_class_lvl_each_param
  puts 'test_class_lvl_each_param SELECT * FROM T1'
  WIKK::SQL.each_param(@config, 'SELECT * FROM T1') do |id:, s:, **_row|
    puts "id #{id} s #{s}"
  end
end

def test_class_lvl_each_hash_with_table_names
  query = <<~SQL
    SELECT T1.id, T1.s, T2.id, T2.s
    FROM T1, T2, T3
    WHERE T1.id = T3.id1
    AND T3.id2 = T2.id
  SQL
  puts "test_class_lvl_each_hash_with_table_names: #{query}"
  # with_table_names
  WIKK::SQL.each_hash(@config, query, true) do |row|
    puts 'Row', row
    puts
    row.each do |k, v|
      print "#{k}: #{v}, "
    end
    puts
  end
end

def test_instance_lvl_get_fields
  puts 'test_instance_lvl_get_fields: SELECT T1.id, T1.s FROM T1'
  WIKK::SQL.connect(@config) do |sql|
    sql.query('SELECT T1.id, T1.s FROM T1') do |_result|
      # puts result.fetch_fields[0].class
    end
    sql.fetch_fields.each_with_index do |info, i|
      puts "Column #{i} (#{info.name}) ---"
      puts "table:            #{info.table}"
      puts "def:              #{info.def}"
      puts "type:             #{info.type}\n"
      printf "length:           #{info.length}"
      puts "max_length:       #{info.max_length}"
      puts "flags:            #{info.flags}\n"
      puts "decimals:         #{info.decimals}"
    end
  end
end

def test_instance_lvl_each_row
  puts 'test_instance_lvl_each_row: SELECT id, s FROM T1'
  WIKK::SQL.connect(@config) do |sql|
    sql.each_row('SELECT id, s FROM T1') do |row|
      puts "id: #{row[0]}, s: #{row[1]}"
    end
    puts "Number of rows returned: #{sql.affected_rows}"
  end
end

def test_instance_lvl_each_hash
  puts 'test_instance_lvl_each_hash: SELECT * FROM T1'
  WIKK::SQL.connect(@config) do |sql|
    sql.each_hash('SELECT * FROM T1') do |row|
      puts 'Row'
      row.each do |k, v|
        print "#{k}: #{v}, "
      end
      puts
    end
    puts "Number of rows returned: #{sql.affected_rows}"
  end
end

def test_transaction
  puts 'test_transaction: select site_name, state, INET_NTOA(network + ...'
  WIKK::SQL.connect(@config) do |sql|
    sql.transaction do
      sql.each_row('SELECT id, s, n FROM T1') do |row|
        puts "#{row[0]}, #{row[1]} #{row[2]}"
      end
      sql.each_row('SELECT id, s FROM T2') do |row|
        puts "#{row[0]}, #{row[1]}"
      end
    end
  end
end

puts '*****Test Class level each row********'
test_class_lvl_each_row
puts
puts '*****Test Class level each hash********'
test_class_lvl_each_hash
puts
puts '*****Test Class level each sym********'
test_class_lvl_each_sym
puts
puts '*****Test Class level each param********'
test_class_lvl_each_param
puts
puts '*****Test each row********'
test_class_lvl_each_hash_with_table_names
puts
# puts '*****Test get fields********'
# test_instance_lvl_get_fields
puts
puts '*****Test transaction 1 customer, 1 distribution ********'
test_transaction
puts
