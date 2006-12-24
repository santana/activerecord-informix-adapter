# $Id: informix_adapter.rb,v 1.10 2006/12/24 01:21:24 santana Exp $

# Copyright (c) 2006, Gerardo Santana Gomez Garrido <gerardo.santana@gmail.com>
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
# STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

require 'active_record/connection_adapters/abstract_adapter'

module ActiveRecord
  class Base
    def self.informix_connection(config) #:nodoc:
      require 'informix' unless self.class.const_defined?(:Informix)
      require 'stringio'
      
      config = config.symbolize_keys

      database    = config[:database].to_s
      username    = config[:username]
      password    = config[:password]
      db          = Informix.connect(database, username, password)
      ConnectionAdapters::InformixAdapter.new(db, logger)
    end

    after_save :write_lobs
    private
    def write_lobs
      if connection.is_a?(ConnectionAdapters::InformixAdapter)
        self.class.columns.select { |c| [:text, :binary].include?(c.type) }.each { |c|
          value = self[c.name]
          next if value.nil?  || value == ''
          qry = "update #{self.class.table_name} set #{c.name} = ? where #{self.class.primary_key} = #{quote(id)}"
          stmt = connection.prepare(qry)
          stmt.execute(StringIO.new(value))
          stmt.drop
        }
      end
    end
  end # class Base

  module ConnectionAdapters
    # This adapter requires the Informix driver for Ruby
    # http://ruby-informix.sourceforge.net
    #
    # Options:
    #
    # * <tt>:database</tt>  -- Defaults to nothing.
    # * <tt>:username</tt>  -- Defaults to nothing.
    # * <tt>:password</tt>  -- Defaults to nothing.

    class InformixAdapter < AbstractAdapter
      def initialize(db, logger)
        super
        stmt = db.prepare("select dbinfo('version', 'major') version from systables where tabid = 1")
        @ifx_version = stmt.execute['version'].to_i
        stmt.drop
      end

      def native_database_types
        {
          :primary_key => "serial primary key",
          :string      => { :name => "varchar", :limit => 255  },
          :text        => { :name => "text" },
          :integer     => { :name => "integer" },
          :float       => { :name => "float" },
          :datetime    => { :name => "datetime year to second" },
          :timestamp   => { :name => "datetime year to second" },
          :time        => { :name => "datetime hour to second" },
          :date        => { :name => "date" },
          :binary      => { :name => "byte"},
          :boolean     => { :name => "boolean"}
        }
      end

      def adapter_name
        'Informix'
      end

      def prefetch_primary_key?(table_name = nil)
        true
      end
 
      def supports_migrations? #:nodoc:
        false # XXX yet
      end

      def default_sequence_name(table, column) #:nodoc:
        "#{table}_seq"
      end

      # DATABASE STATEMENTS =====================================
      def select_all(sql, name = nil)
        select(sql, name)
      end

      def select_one(sql, name = nil)
        add_limit!(sql, :limit => 1)
        result = select(sql, name)
        result.first if result
      end

      def execute(sql, name = nil)
        log(sql, name) { @connection.do(sql) }
      end

      def prepare(sql, name = nil)
        log(sql, name) { @connection.prepare(sql) }
      end

      def insert(sql, name= nil, pk= nil, id_value= nil, sequence_name = nil)
        log(sql, name) { @connection.do(sql) }
        id_value
      end

      alias_method :update, :execute
      alias_method :delete, :execute

      def begin_db_transaction
        @connection.do("begin work")
      end

      def commit_db_transaction
        @connection.commit
      end

      def rollback_db_transaction
        @connection.rollback
      end

      def add_limit_offset!(sql, options)
        if options[:limit]
          limit = "FIRST #{options[:limit]}"
          # SKIP available only in IDS >= 10
          offset = @ifx_version >= 10 && options[:offset]? "SKIP #{options[:offset]}": ""
          sql.sub!(/^select /i,"SELECT #{offset} #{limit} ")
        end
        sql
      end

      def next_sequence_value(sequence_name)
        select_one("select #{sequence_name}.nextval id from systables where tabid=1")['id']
      end

      # QUOTING ===========================================
      def quote_string(string)
        string.gsub(/\'/, "''")
      end

      def quote(value, column = nil)
        if column && [:binary, :text].include?(column.type)
          return "NULL"
        end
        if column && column.type == :date
          return "'#{value.mon}/#{value.day}/#{value.year}'"
        end
        super
      end

      # SCHEMA STATEMENTS =====================================
      def tables(name = nil)
        c = @connection.cursor("SELECT tabname from systables WHERE tabid > 99")
        tables = c.open.fetch_all
        c.drop
        tables.flatten
      end

      def columns(table_name, name = nil)
        result = @connection.columns(table_name)
        columns = []
        result.each { |column|
          columns << Column.new(column[:name], column[:default],
            make_type(column[:stype], column[:precision]), column[:nullable])
        }
        columns
      end

      def make_type(type, prec)
        types = %w(CHAR CHARACTER CHARACTER\ VARYING DECIMAL FLOAT LIST
          LVARCHAR MONEY MULTISET NCHAR NUMERIC NVARCHAR SERIAL SERIAL8
          VARCHAR)
        type.sub!(/money/i, 'decimal')
        if types.include? type.upcase
          "#{type}(#{prec})" 
        elsif type =~ /datetime/i
          type = "time" if prec == 6
          type
        elsif type =~ /byte/i
          "binary"
        else
          type
        end
      end

      # MIGRATION =========================================
      def recreate_database(name)
        drop_database(name)
        create_database(name)
      end

      def drop_database(name)
        @connection.do("drop database #{name}")
      end

      def create_database(name)
        @connection.do("create database #{name}")
      end

      # XXX
      def indexes(table_name, name = nil)
        indexes = []
        indexes
      end
            
      def create_table(name, options = {})
        super(name, options)
        execute("CREATE SEQUENCE #{name}_seq")
      end

      def rename_table(name, new_name)
        execute("RENAME TABLE #{name} TO #{new_name}")
        execute("RENAME SEQUENCE #{name}_seq TO #{new_name}_seq")
      end

      def drop_table(name)
        super(name)
        execute("DROP SEQUENCE #{name}_seq")
      end
      
      def rename_column(table, column, new_column_name)
        execute("RENAME COLUMN #{table}.#{column} TO #{new_column_name}")
      end
      
      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        sql = "ALTER TABLE #{table_name} MODIFY #{column_name} #{type_to_sql(type, options[:limit])}"
        add_column_options!(sql, options)
        execute(sql)
      end

      def remove_index(table_name, options = {})
        execute("DROP INDEX #{index_name(table_name, options)}")
      end

      # XXX
      def structure_dump
        super
      end

      def structure_drop
        super
      end

      private
        def select(sql, name = nil)
          sql.gsub!(/=\s*null/i, 'IS NULL')
          c = log(sql, name) { @connection.cursor(sql) }
          rows = c.open.fetch_hash_all
          c.drop
          rows
        end
    end #class InformixAdapter < AbstractAdapter
  end #module ConnectionAdapters
end #module ActiveRecord
