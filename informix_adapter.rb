# $Id: informix_adapter.rb,v 1.4 2006/04/11 16:34:13 santana Exp $

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
      conn        = Informix.connect(database, username, password)
      ConnectionAdapters::InformixAdapter.new(conn, logger)
    end

    def comma_pair_list(hash)
      hash.keys.sort*" = ?, " + " = ?"
    end

    def quoted_column_names(attributes = attributes_with_quotes)
      attributes.keys.sort.collect do |column_name|
        self.class.connection.quote_column_name(column_name)
      end
    end

    def quoted_values(include_primary_key = false)
      params = []
      attributes.keys.sort.each { |name|
        column = column_for_attribute(name)
        next if !column
        if [:binary, :text].include?(column.type) && attributes[name].is_a?(String)
          v = StringIO.new(attributes[name])
        else
          v = attributes[name]
        end
        if !column.primary
          params << v
        elsif include_primary_key
          params << 0
        end
      }
      params
    end

    def create
      query =<<-EOS
        INSERT INTO #{self.class.table_name} (#{quoted_column_names.join(', ')})
        VALUES(?#{", ?"*(attributes.size - 1)})
      EOS

      params = quoted_values(true)
      self.id = connection.insert(query, params, "#{self.class.name} Create")
      @new_record = false
    end

    def update
      query =<<-EOS
        UPDATE #{self.class.table_name}
        SET  #{quoted_comma_pair_list(connection,attributes_with_quotes(false))}
        WHERE #{self.class.primary_key} = #{quote(id)}
      EOS

      params = quoted_values
      connection.update(query, params, "#{self.class.sequence_name} Update")
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
      
      def supports_migrations? #:nodoc:
        false # XXX yet
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

      def insert(sql, params, name= nil, pk= nil, id_value= nil, sq_name = nil)
        log(sql, name) {
          stmt = @connection.prepare(sql)
          stmt[*params]
          stmt.drop
        }
        id_value
      end

      alias_method :update, :insert
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
        if limit = options[:limit]
          sql.sub!(/^select /i,"SELECT FIRST #{limit} ") 
        end
        sql
      end

      # QUOTING ===========================================
      def quote_string(string)
        string.gsub(/\'/, "''")
      end

      # SCHEMA STATEMENTS =====================================
      def tables(name = nil)
        c = @connection.cursor("SELECT tabname from systables WHERE tabid > 99")
        tables = c.open.fetch_all
        c.drop
        return tables.nil?? []: tables.flatten
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
            
      def rename_table(name, new_name)
        execute("RENAME TABLE #{name} TO #{new_name}")
      end
      
      def rename_column(table, column, new_column_name)
        execute("RENAME COLUMN #{table}.#{column} TO #{new_column_name}")
      end
      
      # XXX
      def change_column(table_name, column_name, type, options = {}) #:nodoc:
        super
      end

      def change_column_default(table_name, column_name, default)
        super
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
          rows = []
          sql.gsub!(/=\s*null/i, 'IS NULL')
          c = log(sql, name) { @connection.cursor(sql) }
          c.open.each_hash {|row|
            rows << row
          }.drop
          rows
        end
    end #class InformixAdapter < AbstractAdapter
  end #module ConnectionAdapters
end #module ActiveRecord
