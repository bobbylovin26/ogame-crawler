#!/usr/bin/ruby
require 'httpclient'
require 'nokogiri'
require 'sqlite3'
require 'date'

Header = {
  'accept'=> 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'user-agent'=> 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.63 Safari/537.31',
  'accept-encoding'=> 'gzip,deflate,sdch',
  'accept-language'=> 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4',
  'accept-charset'=> 'GBK,utf-8;q=0.7,*;q=0.3'
}

class OgameCrawler
  def initialize
    @uni      = 'uni110.ogame.tw'
    @username = 'nswish'
    @password = 'zwyxyz'
    @clnt     = HTTPClient.new
  end

  def ranks(pages_count = 11)
    self.login

    record = Record.new

    if pages_count != nil then
      pages_count = "#{pages_count}".to_i
    else
      pages_count = 11
    end

    # load history ranks data
    history_ranks = {}
    record.read('RANKS').each do |row|
      history_ranks[row['ORIGIN']] = row
    end

    # scan ogame ranks data
    ogame_ranks = {}
    pages_count.times do |idx|
      res = @clnt.post_content("http://uni110.ogame.tw/game/index.php?page=highscoreContent&category=1&type=0&site=#{idx+1}")

      Nokogiri::HTML(res).css('table#ranks tr')[1..-1].each do |item|
        info = user_rank(item)
        ogame_ranks[info['ORIGIN']] = info
      end

      print '+'
    end

    # merge two ranks data
    merged_ranks = {}
    ogame_ranks.each do |key, value|
      if history_ranks[key] then
        history_rank = history_ranks.delete key

        if history_rank['LAST_SCORE'] == value['LAST_SCORE'] then
          merged_ranks[key] = history_rank

        elsif history_rank['LAST_SCORE'] > value['LAST_SCORE'] then
          merged_ranks[key] = value
          merged_ranks[key]['SINCE_DATE'] = history_rank['SINCE_DATE']

        elsif history_rank['LAST_SCORE'] < value['LAST_SCORE'] then
          merged_ranks[key] = value
        end

      else
        merged_ranks[key] = value
      end
    end

    history_ranks.each do |key, value|
      merged_ranks[key] = value
    end

    record.database.transaction do
      record.database.execute('DELETE FROM RANKS')
      merged_ranks.each do |key, value|
        print '.'
        record.write('RANKS', value)
      end
    end

    print "ok!\n"
  end

  def galaxy(start)
    login

    record = Record.new

    url = "http://uni110.ogame.tw/game/index.php?page=galaxyContent&ajax=1"
    galaxy, start_system = start.delete('[]').split(':')

    (start_system.to_i..499).each do |system|
      puts "[#{galaxy}:#{system}]"
      result = []
      res = @clnt.post_content(url, {:galaxy=>galaxy, :system=>system})

      galaxytable = Nokogiri::HTML(res).css('#galaxytable')

      galaxytable.css('tbody tr').map do |row|
        data = planet_info row
        if data.size > 0 then
          result.push data
        end
      end

      record.database.transaction do
        record.database.execute('DELETE FROM GALAXIES WHERE POSITION LIKE ?', ["[#{galaxy}:#{system}:%"])
        result.each do |value|
          record.write('GALAXIES', value)
        end
      end
    end

    puts "ok!"
  end

  def absence(day=2)
    record = Record.new

    day = 2 if !day

    select_sql = <<-SQL
      SELECT ORIGIN, NAME, LAST_SCORE, ROUND((JULIANDAY(DATETIME('now')) - JULIANDAY(SINCE_DATE))*24,0) AS HOURS
        FROM RANKS
       WHERE (JULIANDAY(DATETIME('now')) - JULIANDAY(SINCE_DATE))*24 > #{day}*24
         AND LAST_SCORE > 300
    ORDER BY LAST_SCORE DESC
    SQL

    raw_distance_array = record.query(select_sql).map.with_index do |row, index|
      coord = row['ORIGIN'].delete('[]').split(':').map { |token| token.to_i}
      distance = (coord[1] - 140).abs
      if distance <= 40 then
        [row['NAME'], distance]
      end
    end

    raw_distance_array.delete(nil)

    raw_distance_array.sort! do |a,b|
      (a[1] <=> b[1]) * -1
    end

    distance_array = raw_distance_array.map do |row|
      user row[0]
    end
  end

  def origin(coord)
    record = Record.new

    select_sql = <<-SQL
      SELECT ORIGIN,NAME,LAST_SCORE,ROUND((JULIANDAY(DATETIME('now')) - JULIANDAY(SINCE_DATE))*24,0) AS HOURS
      FROM RANKS
      WHERE ORIGIN IN ('#{coord}')
    SQL

    record.query(select_sql).each_with_index do |row, index|
      puts "#{row['ORIGIN']}|#{row['NAME']}|#{row['LAST_SCORE']}|#{row['HOURS']}"
    end    
  end

  def user(name)
    record = Record.new

    select_sql = <<-SQL
      SELECT ORIGIN,NAME,LAST_SCORE,ROUND((JULIANDAY(DATETIME('now')) - JULIANDAY(SINCE_DATE))*24,0) AS HOURS
      FROM RANKS
      WHERE NAME IN ('#{name}')
    SQL

    record.query(select_sql).each_with_index do |row, index|
      puts "#{row['ORIGIN']}|#{row['NAME']}|#{row['LAST_SCORE']}|#{row['HOURS']}"
    end    

    select_sql = <<-SQL
      SELECT *
      FROM GALAXIES
      WHERE uSER_NAME IN ('#{name}')
    SQL

    record.query(select_sql).each_with_index do |row, index|
      puts "\t#{row['POSITION']}|#{row['NAME']}|#{row['USER_STATUS']}|#{row['SINCE_DATE']}"
    end    
  end

  def login
    print 'Start Login...'
    res = @clnt.get('http://ogame.tw', nil, Header)   # access home page

    body = {
      'uni'   => @uni,
      'login' => @username,
      'pass'  => @password,
      'kid'   => ''
    }

    res = @clnt.post_content('http://ogame.tw/main/login', body)
    puts "ok"
  end

  # private method here
  private

  def user_rank(dom)
    result = {}

    result['ORIGIN']     = "[" + dom.css('a.dark_highlight_tablet')[0].attr('href').gsub(/^.*page=galaxy&galaxy=|&system|&position/, '').gsub(/\=/, ':') + "]"
    result['NAME']       = dom.css('.playername')[0].content.strip
    result['LAST_SCORE'] = dom.css('.score')[0].content.strip.sub(/,/, '').to_i
    result['SINCE_DATE'] = DateTime.now.to_s

    return result
  end

  def planet_info(row)
    result = {}

    if row.css("span#pos-planet").size > 0 then
      result['POSITION']    = row.css("span#pos-planet")[0].content.strip
      result['NAME']        = row.css("td.planetname")[0].content.strip
      result['USER_NAME']   = row.css("td.playername span")[0].content.strip
      result['USER_STATUS'] = row.css("td.playername span.status")[0].content.strip
      result['SINCE_DATE']  = DateTime.now.to_s
    end

    return result
  end
end

class Record
  attr_reader :database

  def initialize
    @database = SQLite3::Database.new "ogame.db"
    init_db
  end

  def write(table, data)
    keys = []
    values = []
    data.each do |key, value|
      if key != 'ID' then
        keys.push key
        values.push value
      end
    end

    insert_sql = "INSERT INTO #{table}(#{keys.join ','}) VALUES(#{(['?']*values.length).join ','})"

    @database.execute(insert_sql, values);
  end

  def read(table, where={})
    values = []
    key_values = where.map do |key, value|
      values.push value
      key + ' = ?'
    end

    select_sql = "SELECT * FROM #{table}" 
    select_sql = select_sql + " #{'WHERE ' + key_values.join(' and ')}" if values.length > 0

    return query(select_sql, values)
  end

  def query(raw_sql, args=[])
    result_set = @database.prepare(raw_sql).execute(args)

    result = []
    columns = result_set.columns
    result_set.each { |row|
      data_row = {}
      row.each_with_index do |item, index|
        data_row[columns[index]] = item
      end
      result.push data_row
    }

    return result
  end

  private
  def init_db
    begin
      rows = @database.execute <<-SQL
        select 1 from RANKS;
      SQL
    rescue Exception=>ex
      if ex.to_s.start_with? 'no such table'
        @database.execute <<-SQL
          CREATE TABLE RANKS(
            ID INTEGER PRIMARY KEY,
            ORIGIN VARCHAR(20),
            NAME VARCHAR(100),
            ALLY VARCHAR(100),
            LAST_SCORE INTEGER,
            SINCE_DATE TIMESTAMP
          );
        SQL
      end
    end

    begin
      rows = @database.execute <<-SQL
        select 1 from GALAXIES;
      SQL
    rescue Exception=>ex
      if ex.to_s.start_with? 'no such table'
        @database.execute <<-SQL
          CREATE TABLE GALAXIES(
            ID INTEGER PRIMARY KEY,
            POSITION VARCHAR(20),
            NAME VARCHAR(100),
            USER_NAME VARCHAR(100),
            USER_STATUS VARCHAR(50),
            SINCE_DATE TIMESTAMP
          );
        SQL
      end
    end    
  end
end

# crawler.rb <arg> <c>
command = nil
args = nil

if ARGV.length == 0 then
  puts "crawler.rb [-args] <command>"
elsif ARGV.length > 0 then
  ARGV.each do |token|
    if token.start_with? "-" then

    else
      cmd_args = token.split "="
      command = cmd_args[0].to_sym
      args = cmd_args[1]
    end
  end

  if !command then
    puts "crawler.rb [-args] <command>"
    exit(-1)
  end

  crawler = OgameCrawler.new
  crawler.send(command, args)

end