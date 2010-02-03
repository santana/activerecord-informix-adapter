# Copyright (c) 2006-2010, Gerardo Santana Gomez Garrido <gerardo.santana@gmail.com>
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
        return if !connection.is_a?(ConnectionAdapters::InformixAdapter)
        self.class.columns.each do |c|
          value = self[c.name]
          next if ![:text, :binary].include? c.type || value.nil? || value == ''
          connection.raw_connection.execute(<<-end_sql, StringIO.new(value))
              UPDATE #{self.class.table_name} SET #{c.name} = ?
              WHERE #{self.class.primary_key} = #{quote_value(id)}
          end_sql
        end
      end
  end # class Base

  module ConnectionAdapters
    class InformixColumn < Column
      def initialize(column)
        sql_type = make_type(column[:stype], column[:length],
                             column[:precision], column[:scale])
        super(column[:name], column[:default], sql_type, column[:nullable])
      end

      private
        IFX_TYPES_SUBSET = %w(CHAR CHARACTER CHARACTER\ VARYING DECIMAL FLOAT
                              LIST LVARCHAR MONEY MULTISET NCHAR NUMERIC
                              NVARCHAR SERIAL SERIAL8 VARCHAR).freeze

        def make_type(type, limit, prec, scale)
          type.sub!(/money/i, 'DECIMAL')
          if IFX_TYPES_SUBSET.include? type.upcase
            if prec == 0
              "#{type}(#{limit})" 
            else
              "#{type}(#{prec},#{scale})"
            end
          elsif type =~ /datetime/i
            type = "time" if prec == 6
            type
          elsif type =~ /byte/i
            "binary"
          else
            type
          end
        end

        def simplified_type(sql_type)
          if sql_type =~ /serial/i
            :primary_key
          else
            super
          end
        end
    end

    # This adapter requires Ruby/Informix
    # http://ruby-informix.rubyforge.org
    #
    # Options:
    #
    # * <tt>:database</tt>  -- Defaults to nothing.
    # * <tt>:username</tt>  -- Defaults to nothing.
    # * <tt>:password</tt>  -- Defaults to nothing.

    class InformixAdapter < AbstractAdapter
      def initialize(db, logger)
        super
        @ifx_version = db.version.major.to_i
      end

      def native_database_types
        {
          :primary_key => "serial primary key",
          :string      => { :name => "varchar", :limit => 255  },
          :text        => { :name => "text" },
          :integer     => { :name => "integer" },
          :float       => { :name => "float" },
          :decimal     => { :name => "decimal" },
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
        true
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
        log(sql, name) { @connection.immediate(sql) }
      end

      def prepare(sql, name = nil)
        log(sql, name) { @connection.prepare(sql) }
      end

      def insert(sql, name= nil, pk= nil, id_value= nil, sequence_name = nil)
        execute(sql)
        id_value
      end

      alias_method :update, :execute
      alias_method :delete, :execute

      def begin_db_transaction
        execute("begin work")
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
        @connection.cursor(<<-end_sql) do |cur|
            SELECT tabname FROM systables WHERE tabid > 99 AND tabtype != 'Q'
          end_sql
          cur.open.fetch_all.flatten
        end
      end

      def columns(table_name, name = nil)
        @connection.columns(table_name).map {|col| InformixColumn.new(col) }
      end

      # MIGRATION =========================================
      def recreate_database(name)
        drop_database(name)
        create_database(name)
      end

      def drop_database(name)
        execute("drop database #{name}")
      end

      def create_database(name)
        execute("create database #{name}")
      end

      # XXX
      def indexes(table_name, name = nil)
        indexes = []
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
          c.free
          rows
        end
    end #class InformixAdapter < AbstractAdapter
  end #module ConnectionAdapters
end #module ActiveRecord
